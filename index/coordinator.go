package index

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ttab/elephant-index/postgres"
	"github.com/ttab/elephantine"
	"github.com/ttab/elephantine/pg"
	"golang.org/x/sync/errgroup"
)

type CoordinatorOptions struct {
	Logger         *slog.Logger
	DefaultCluster string
}

type Coordinator struct {
	logger     *slog.Logger
	opt        CoordinatorOptions
	db         *pgxpool.Pool
	q          *postgres.Queries
	changes    chan Notification
	startCount int32

	stopOnce sync.Once
	stop     chan struct{}
	stopped  chan struct{}
}

func NewCoordinator(
	ctx context.Context,
	db *pgxpool.Pool, opt CoordinatorOptions,
) (*Coordinator, error) {
	if opt.DefaultCluster == "" {
		return nil, errors.New("missing default cluster")
	}

	logger := opt.Logger
	if logger == nil {
		logger = slog.Default()
	}

	c := Coordinator{
		logger:  logger,
		db:      db,
		q:       postgres.New(db),
		opt:     opt,
		stop:    make(chan struct{}),
		stopped: make(chan struct{}),
	}

	go c.cleanupLoop(context.Background())

	return &c, nil
}

// Run the coordinator. A coordinator can only run once.
func (c *Coordinator) Run(ctx context.Context) error {
	if c.askedToStop() {
		return errors.New("coordinator has been stopped")
	}

	count := atomic.AddInt32(&c.startCount, 1)
	if count > 1 {
		return errors.New("already started")
	}

	defer close(c.stopped)

	err := c.ensureDefaultIndexSet(ctx)
	if err != nil {
		return fmt.Errorf(
			"ensure default cluster and index set: %w", err)
	}

	for {
		if c.askedToStop() {
			break
		}
	}

	finCtx, cancel := context.WithTimeout(
		context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()

	err = c.finalise(finCtx)
	if err != nil {
		return fmt.Errorf("post-stop cleanup: %w", err)
	}

	return nil
}

func (c *Coordinator) finalise(ctx context.Context) error {
	return nil
}

type NotifyChannel string

const (
	NotifyIndexSetActivated NotifyChannel = "index_activated"
	NotifyIndexSetEnabled   NotifyChannel = "index_enabled"
	NotifyIndexSetDisabled  NotifyChannel = "index_disabled"
)

type Notification struct {
	Type NotifyChannel
	Name string
}

func (c *Coordinator) runListener(ctx context.Context) error {
	// We need an actual connection here, as we're giong to hijack it and
	// punt it into listen mode.
	conn, err := c.db.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire connection from pool: %w", err)
	}

	pConn := conn.Hijack()

	defer func() {
		err := pConn.Close(ctx)
		if err != nil {
			c.logger.ErrorContext(ctx,
				"failed to close PG listen connection",
				elephantine.LogKeyError, err)
		}
	}()

	notifications := []NotifyChannel{
		NotifyIndexSetActivated,
		NotifyIndexSetEnabled,
		NotifyIndexSetDisabled,
	}

	for _, channel := range notifications {
		ident := pgx.Identifier{string(channel)}

		_, err := conn.Exec(ctx, "LISTEN "+ident.Sanitize())
		if err != nil {
			return fmt.Errorf("failed to start listening to %q: %w",
				channel, err)
		}
	}

	received := make(chan *pgconn.Notification)
	grp, gCtx := errgroup.WithContext(ctx)

	grp.Go(func() error {
		for {
			notification, err := conn.Conn().WaitForNotification(gCtx)
			if err != nil {
				return fmt.Errorf(
					"error while waiting for notification: %w", err)
			}

			received <- notification
		}
	})

	grp.Go(func() error {
		for {
			var notification *pgconn.Notification

			select {
			case <-ctx.Done():
				return ctx.Err() //nolint:wrapcheck
			case notification = <-received:
			}

			c.changes <- Notification{
				Type: NotifyChannel(notification.Channel),
				Name: notification.Payload,
			}

		}
	})

	err = grp.Wait()
	if err != nil {
		return err //nolint:wrapcheck
	}

	return nil
}

// Convenience function for cases where it's easier than doing a channel select
// on c.stop.
func (c *Coordinator) askedToStop() bool {
	select {
	case <-c.stop:
		return true
	default:
		return false
	}
}

// Stop the coordinator. Blocks until it has stopped or the timeout has been
// reached.
func (c *Coordinator) Stop(timeout time.Duration) {
	c.stopOnce.Do(func() {
		close(c.stop)
	})

	select {
	case <-time.After(timeout):
	case <-c.stopped:
	}
}

// Run cleanup on a 12-24-hour interval.
func (c *Coordinator) cleanupLoop(ctx context.Context) {
	for {
		err := c.cleanup(ctx)
		if err != nil {
			c.logger.Error("failed to run cleanup",
				elephantine.LogKeyError, err)
		}

		// Wait between 12 and 24 hours.
		randomMinutes := time.Duration(rand.Intn(12*60)) * time.Minute
		delay := 12*time.Hour + randomMinutes

		select {
		case <-time.After(delay):
		case <-c.stop:
		}
	}
}

// Delete old index sets that are disabled and inactive, and clusters that don't
// have an index set (and were created more than a week ago). By performing
// deferred deletes after a time of inactivity we don't have to deal with index
// sets being deleted before the coordinator has picked up on them being
// disabled.
func (c *Coordinator) cleanup(ctx context.Context) error {
	return pg.WithTX(ctx, c.logger, c.db, "cleanup", func(tx pgx.Tx) error {
		q := postgres.New(tx)

		oneWeekAgo := time.Now().AddDate(0, 0, -7)

		err := q.DeleteOldIndexSets(ctx, pg.Time(oneWeekAgo))
		if err != nil {
			return fmt.Errorf("delete old index sets: %w", err)
		}

		err = q.DeleteUnusedClusters(ctx, pg.Time(oneWeekAgo))
		if err != nil {
			return fmt.Errorf("delete unused clusters: %w", err)
		}

		return nil
	})
}

// Ensure that we have a default cluster and index set. Starts with an ACCESS
// EXCLUSIVE lock on the cluster table, so only one instance will be running
// this check at any given time.
func (c *Coordinator) ensureDefaultIndexSet(ctx context.Context) error {
	return pg.WithTX(ctx, c.logger, c.db, "ensure-default-indexer", func(tx pgx.Tx) error {
		// Completely lock the cluster table while we initialise.
		_, err := tx.Exec(ctx, "LOCK TABLE cluster IN ACCESS EXCLUSIVE MODE")
		if err != nil {
			return fmt.Errorf("lock cluster table: %w", err)
		}

		q := postgres.New(tx)

		clusters, err := q.GetClusters(ctx)
		if err != nil {
			return fmt.Errorf("list clusters: %w", err)
		}

		if len(clusters) == 0 {
			err := q.AddCluster(ctx, postgres.AddClusterParams{
				Name: "default",
				Url:  c.opt.DefaultCluster,
			})
			if err != nil {
				return fmt.Errorf("create default cluster: %w", err)
			}

			// Schema 002 and earlier can have null clusters. Phase
			// out before going 1.0.
			err = q.SetClusterWhereMissing(ctx, "default")
			if err != nil {
				return fmt.Errorf("set default cluster for index sets: %w", err)
			}
		}

		indexSets, err := q.GetIndexSets(ctx)
		if err != nil {
			return fmt.Errorf("get index sets: %w", err)
		}

		if len(indexSets) == 0 {
			err := q.CreateIndexSet(ctx, postgres.CreateIndexSetParams{
				Name:    "v1",
				Cluster: "default",
			})
			if err != nil {
				return fmt.Errorf("create default index set: %w", err)
			}
		}

		return nil
	})
}
