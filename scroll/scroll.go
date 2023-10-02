package scroll

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/ttab/elephant-api/repository"
)

// Roller defaults and limits.
const (
	DefaultWait      = 10 * time.Second
	DefaultBatchWait = 200 * time.Millisecond
	DefaultSpanSize  = 10_000
	MaxSpanSize      = 10_000
	DefaultBatchSize = 500
)

// EventHandler is responsible for processing events.
//
// As the Roller will persist the log position state it's recommended to block
// until the entire batch has been processed when lastInBatch is true.
type EventHandler func(
	ctx context.Context,
	event *repository.EventlogItem,
	compactLog bool,
	lastInBatch bool,
) error

// Roller helps your application to work through the repository event so that
// you can process all documents.
type Roller struct {
	client     repository.Documents
	stateStore StateStore

	opts RollerOptions
	stop atomic.Bool
}

type RollerOptions struct {
	// SpanSize is the size of the range that we use to page through the
	// compacted event log. This is range over which document uniqueness is
	// guaranteed.
	SpanSize int64
	// BatchSize is the size of the batches that we use when processing the
	// compacted and streaming eventlog.
	BatchSize int32
	// Wait is the maximum time a long poll request will wait before
	// returning an empty result when calling the streaming event log.
	Wait time.Duration
	// BatchWait is the maximum time a long poll request will wait for the
	// batch to be filled after receiving the initial event of a batch whem
	// calling the streaming event log.
	BatchWait time.Duration
}

func (r *Roller) NewRoller(
	client repository.Documents,
	store StateStore,
	opts RollerOptions,
) (*Roller, error) {
	if opts.SpanSize > MaxSpanSize {
		return nil, fmt.Errorf("span size cannot be greater than %d",
			MaxSpanSize)
	}

	if opts.SpanSize == 0 {
		opts.SpanSize = DefaultSpanSize
	}

	if opts.BatchSize == 0 {
		opts.BatchSize = DefaultBatchSize
	}

	if opts.Wait == 0 {
		opts.Wait = DefaultWait
	}

	if opts.BatchWait == 0 {
		opts.BatchWait = DefaultBatchWait
	}

	return &Roller{
		client:     client,
		stateStore: store,
		opts:       opts,
	}, nil
}

// Run the eventlog processor, runs until Stop() is called or an error is
// encountered. If the roller is stopped it returns ErrRollerStopped.
func (r *Roller) Run(ctx context.Context, handler EventHandler) error {
	r.stop.Store(false)

	state, err := r.stateStore.GetState(ctx)
	if err != nil {
		return fmt.Errorf("read log state: %w", err)
	}

	for !state.Streaming {
		mark := time.Now()

		err := r.runCompactLoop(ctx, state, handler)
		if err != nil {
			return fmt.Errorf(
				"process compacted event log: %w", err)
		}

		// If we've been processing the compact loop for more than 24
		// hours we take another go at the compact log.
		if time.Since(mark) > 24*time.Hour {
			continue
		}

		state.Streaming = true

		err = r.stateStore.SetState(ctx, state)
		if err != nil {
			return fmt.Errorf(
				"persist state after processing compacted event log: %w", err)
		}

		break
	}

	err = r.runEventLoop(ctx, state, handler)
	if err != nil {
		return fmt.Errorf(
			"process event log: %w", err)
	}

	err = r.stateStore.SetState(ctx, state)
	if err != nil {
		return fmt.Errorf(
			"persist state after event log processing stopped: %w", err)
	}

	return nil
}

func (r *Roller) runCompactLoop(
	ctx context.Context, state *State, handler EventHandler,
) error {
	// Locate the last event so that we know what the end-point for our
	// compacted log reading is.
	lastEvent, err := r.client.Eventlog(ctx, &repository.GetEventlogRequest{
		After: -1,
	})
	if err != nil {
		return fmt.Errorf("get latest event: %w", err)
	}

	// The log is empty, so we're caught up.
	if len(lastEvent.Items) == 0 {
		return nil
	}

	after := state.Position
	fin := lastEvent.Items[0].Id

	for {
		if after == fin {
			break
		}

		var offset int32

		bound := min(fin, after+r.opts.SpanSize)

		for {
			if r.stop.Load() {
				return ErrRollerStopped
			}

			batch, err := r.client.CompactedEventlog(ctx,
				&repository.GetCompactedEventlogRequest{
					After:  after,
					Until:  bound,
					Offset: offset,
					Limit:  r.opts.BatchSize,
				})
			if err != nil {
				return fmt.Errorf("fetch events: %w", err)
			}

			size := len(batch.Items)
			if size == 0 {
				break
			}

			for i, evt := range batch.Items {
				err := handler(ctx, evt, true, i == size-1)
				if err != nil {
					return fmt.Errorf(
						"process event %d: %w",
						evt.Id, err)
				}

				state.Position = evt.Id
			}

			err = r.stateStore.SetState(ctx, state)
			if err != nil {
				return fmt.Errorf(
					"persist state after processing batch: %w", err)
			}

			offset += r.opts.BatchSize
		}

		after = min(fin, after+r.opts.SpanSize)
	}

	return nil
}

func (r *Roller) runEventLoop(
	ctx context.Context, state *State, handler EventHandler,
) error {
	waitMS := int32(r.opts.Wait / time.Millisecond)
	batchWaitMS := int32(r.opts.BatchWait / time.Millisecond)

	for {
		if r.stop.Load() {
			return ErrRollerStopped
		}

		batch, err := r.client.Eventlog(ctx, &repository.GetEventlogRequest{
			After:       state.Position,
			WaitMs:      waitMS,
			BatchWaitMs: batchWaitMS,
			BatchSize:   r.opts.BatchSize,
		})
		if err != nil {
			return fmt.Errorf("get eventlog entries: %w", err)
		}

		size := len(batch.Items)

		for i, evt := range batch.Items {
			err := handler(ctx, evt, false, i == size-1)
			if err != nil {
				return fmt.Errorf(
					"process event %d: %w",
					evt.Id, err)
			}

			state.Position = evt.Id
		}

		err = r.stateStore.SetState(ctx, state)
		if err != nil {
			return fmt.Errorf(
				"persist state after processing batch: %w", err)
		}
	}
}
