package index

import (
	"context"
	"errors"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/ttab/elephantine/rpc"
)

// TestBatchWaitOutcomes pins the three ways a long poll can stop waiting.
// Telling them apart is what lets PollSubscription answer a Connect caller
// that set Connect-Timeout-Ms with deadline_exceeded rather than with an
// empty successful response: connect-go checks the request context before it
// calls a handler and not after, so a success returned past the deadline is
// not corrected on the way out.
func TestBatchWaitOutcomes(t *testing.T) {
	t.Parallel()

	t.Run("max_wait", func(t *testing.T) {
		t.Parallel()

		events := make(chan EventPercolated)
		deadline := time.Now().Add(20 * time.Millisecond)

		got := batchWait(t.Context(), events, deadline,
			10, time.Second)
		if got != waitMaxWait {
			t.Fatalf("got %v, want waitMaxWait", got)
		}
	})

	t.Run("context_cancelled", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(t.Context())

		events := make(chan EventPercolated)
		deadline := time.Now().Add(10 * time.Second)

		go func() {
			time.Sleep(20 * time.Millisecond)
			cancel()
		}()

		got := batchWait(ctx, events, deadline, 10, time.Second)
		if got != waitContext {
			t.Fatalf("got %v, want waitContext", got)
		}
	})

	t.Run("full_batch", func(t *testing.T) {
		t.Parallel()

		events := make(chan EventPercolated)
		deadline := time.Now().Add(10 * time.Second)

		go func() {
			for range 2 {
				events <- EventPercolated{}
			}
		}()

		got := batchWait(t.Context(), events, deadline,
			2, time.Second)
		if got != waitBatch {
			t.Fatalf("got %v, want waitBatch", got)
		}
	})

	t.Run("batch_window_closes", func(t *testing.T) {
		t.Parallel()

		events := make(chan EventPercolated)
		deadline := time.Now().Add(10 * time.Second)

		go func() {
			events <- EventPercolated{}
		}()

		// One event, a batch size that will not be reached, and a
		// short batch window: the window closing is what ends the
		// wait, and there is something to report.
		got := batchWait(t.Context(), events, deadline,
			10, 20*time.Millisecond)
		if got != waitBatch {
			t.Fatalf("got %v, want waitBatch", got)
		}
	})
}

// TestPollWaitError pins which code each reason for a context ending the wait
// is answered with. A caller that set a timeout has to read deadline_exceeded
// where the call actually timed out; canceled is left to mean a caller that
// went away, so the metrics and the logs do not report a timeout as a
// disconnect.
func TestPollWaitError(t *testing.T) {
	t.Parallel()

	t.Run("deadline", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithTimeout(t.Context(), time.Nanosecond)
		defer cancel()

		<-ctx.Done()

		err := pollWaitError(ctx)
		if !rpc.IsCode(err, connect.CodeDeadlineExceeded) {
			t.Fatalf("got %v, want deadline_exceeded", err)
		}
	})

	t.Run("cancelled", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		<-ctx.Done()

		err := pollWaitError(ctx)
		if !rpc.IsCode(err, connect.CodeCanceled) {
			t.Fatalf("got %v, want canceled", err)
		}

		if errors.Is(err, context.DeadlineExceeded) {
			t.Fatal("a cancelled context must not read as a deadline")
		}
	})
}
