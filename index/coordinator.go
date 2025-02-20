package index

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lucasepe/codename"
	"github.com/opensearch-project/opensearch-go/v2"
	"github.com/ttab/elephant-api/newsdoc"
	"github.com/ttab/elephant-api/repository"
	"github.com/ttab/elephant-index/postgres"
	"github.com/ttab/elephantine"
	"github.com/ttab/elephantine/pg"
	"golang.org/x/sync/errgroup"
)

const IndexerStopTimeout = 10 * time.Second

const (
	NotifyIndexStatusChange string = "index_status_change"
	NotifyPercolated        string = "percolation_event"
)

type IndexStatusChange struct {
	Name string
}

type PercolationEvent struct {
	Subscription int64
	Percolator   int64
}

type OpenSearchClientFunc func(
	ctx context.Context, cluster string,
) (*opensearch.Client, error)

type CoordinatorOptions struct {
	Logger       *slog.Logger
	Metrics      *Metrics
	Documents    repository.Documents
	ClientGetter OpenSearchClientFunc
	Validator    ValidatorSource
	Languages    LanguageOptions
	Sharding     ShardingPolicy
	NoIndexing   bool
}

type LanguageOptions struct {
	Substitutions   map[string]string
	DefaultLanguage string
	DefaultRegions  map[string]string
}

type Coordinator struct {
	logger     *slog.Logger
	opt        CoordinatorOptions
	nameRng    *rand.Rand
	db         *pgxpool.Pool
	q          *postgres.Queries
	startCount int32

	activeMut    sync.RWMutex
	activeClient *opensearch.Client
	activeSet    string

	indexers     map[string]*Indexer
	indexerCtx   context.Context
	indexerGroup *errgroup.Group

	percolator *Percolator

	indexStatuses *pg.FanOut[IndexStatusChange]
	changes       chan IndexStatusChange

	percolationEvents *pg.FanOut[PercolationEvent]

	stopOnce sync.Once
	stop     chan struct{}
	stopped  chan struct{}
}

func NewCoordinator(
	db *pgxpool.Pool, opt CoordinatorOptions,
) (*Coordinator, error) {
	rng, err := codename.DefaultRNG()
	if err != nil {
		return nil, fmt.Errorf("initialise name RNG: %w", err)
	}

	logger := opt.Logger
	if logger == nil {
		logger = slog.Default()
	}

	indexGrp, gCtx := errgroup.WithContext(context.Background())

	c := Coordinator{
		logger:            logger,
		db:                db,
		q:                 postgres.New(db),
		opt:               opt,
		nameRng:           rng,
		indexStatuses:     pg.NewFanOut[IndexStatusChange](NotifyIndexStatusChange),
		percolationEvents: pg.NewFanOut[PercolationEvent](NotifyPercolated),
		changes:           make(chan IndexStatusChange),
		indexers:          make(map[string]*Indexer),
		indexerCtx:        gCtx,
		indexerGroup:      indexGrp,
		stop:              make(chan struct{}),
		stopped:           make(chan struct{}),
	}

	return &c, nil
}

// GetActiveIndex the name of the currently active index set, and an OpenSearch
// client that can be used to access it.
func (c *Coordinator) GetActiveIndex() (*opensearch.Client, string) {
	c.activeMut.RLock()
	defer c.activeMut.RUnlock()

	return c.activeClient, c.activeSet
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

	go c.cleanupLoop(ctx)

	go pg.Subscribe(
		ctx, c.logger, c.db,
		c.percolationEvents, c.indexStatuses)

	go func() {
		c.indexStatuses.ListenAll(ctx, c.changes)
		close(c.changes)
	}()

	defer close(c.stopped)

	var errs []error

	err := c.runEventloop(ctx)
	if err != nil {
		c.stopOnce.Do(func() {
			close(c.stop)
		})

		c.logger.ErrorContext(ctx, "failed to run coordinator",
			elephantine.LogKeyError, err)

		errs = append(errs, err)
	}

	err = c.finalise()
	if err != nil {
		errs = append(errs,
			fmt.Errorf("post-stop cleanup: %w", err))
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}

	return nil
}

func (c *Coordinator) finalise() error {
	// Give an extra 30% on top of the index stop timeout.
	indexerDeadline := IndexerStopTimeout / 100 * 130
	indexersStopped := make(chan struct{})

	go func() {
		_ = c.indexerGroup.Wait()

		close(indexersStopped)
	}()

	select {
	case <-time.After(indexerDeadline):
		return fmt.Errorf("indexers failed to stop in time")
	case <-indexersStopped:
		return nil
	}
}

func (c *Coordinator) runEventloop(
	ctx context.Context,
) error {
	q := postgres.New(c.db)

	sets, err := q.GetIndexSets(ctx)
	if err != nil {
		return fmt.Errorf("failed to get the current index sets: %w", err)
	}

	for _, set := range sets {
		if c.opt.NoIndexing {
			if set.Active {
				err := c.ensureActiveClient(set)
				if err != nil {
					return fmt.Errorf(
						"failed to ensure active client %q: %w",
						set.Name, err)
				}
			}

			continue
		}

		err = c.setUpdate(ctx, set)
		if err != nil {
			return fmt.Errorf(
				"set up index set %q: %w",
				set.Name, err,
			)
		}
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err() //nolint:wrapcheck
		case change := <-c.changes:
			err := c.handleChange(ctx, change)
			if err != nil {
				return err
			}
		}
	}
}

func (c *Coordinator) handleChange(
	ctx context.Context, change IndexStatusChange,
) error {
	set, err := c.q.GetIndexSet(ctx, change.Name)
	if err != nil {
		return fmt.Errorf(
			"read status of changed index set %q: %w",
			change.Name, err,
		)
	}

	if c.opt.NoIndexing {
		if set.Active {
			err := c.ensureActiveClient(set)
			if err != nil {
				return fmt.Errorf(
					"failed to ensure active client %q: %w",
					set.Name, err)
			}
		}

		return nil
	}

	err = c.setUpdate(ctx, set)
	if err != nil {
		return fmt.Errorf(
			"update changed index set %q: %w",
			change.Name, err,
		)
	}

	return nil
}

func (c *Coordinator) setUpdate(ctx context.Context, set postgres.IndexSet) error {
	idxr, ok := c.indexers[set.Name]

	// The only state changes that are relevant for the indexer
	// right now is a change in the enabled state. That might change
	// if/when we introduce partial re-index.
	switch {
	case (set.Enabled && !set.Deleted) && !ok:
		i, err := c.startIndexer(ctx, set)
		if err != nil {
			return fmt.Errorf(
				"start indexer %q: %w",
				set.Name, err)
		}

		c.indexers[set.Name] = i
		idxr = i
	case ok && (!set.Enabled || set.Deleted):
		delete(c.indexers, set.Name)

		err := idxr.Stop(IndexerStopTimeout)
		if err != nil {
			return fmt.Errorf(
				"stop disabled indexer: %w", err)
		}
	}

	if set.Active {
		err := c.ensureActiveClient(set)
		if err != nil {
			return fmt.Errorf(
				"failed to ensure active client %q: %w",
				set.Name, err)
		}
	}

	if set.Deleted {
		// Spinning off the post-delete cleanup, no reason to block
		// waiting for it.
		go func() {
			ctx := context.WithoutCancel(ctx)

			err := pg.WithTX(ctx, c.db, func(tx pgx.Tx) error {
				return c.finaliseSetDelete(
					ctx, tx, set.Name)
			})
			if err != nil {
				c.logger.Error(
					"cleanup of indices failed",
					"set_name", set.Name,
					elephantine.LogKeyError, err,
				)
			}
		}()
	}

	return nil
}

func (c *Coordinator) finaliseSetDelete(
	ctx context.Context,
	tx pgx.Tx,
	name string,
) error {
	q := postgres.New(tx)

	// GetIndexSetForDelete gets the index_set_row with a FOR UPDATE NOWAIT
	// so that only one of the index workers will act on the delete.
	idx, err := q.GetIndexSetForDelete(ctx, name)
	if errors.Is(err, pgx.ErrNoRows) || !idx.Deleted {
		return nil
	}

	client, err := c.opt.ClientGetter(ctx, idx.Cluster.String)
	if err != nil {
		return fmt.Errorf(
			"get client for cluster %q: %w",
			idx.Cluster.String, err)
	}

	cat := client.Cat.Indices

	listRes, err := cat(
		cat.WithContext(ctx),
		cat.WithFormat("json"),
		cat.WithIndex(
			"documents-"+idx.Name+"-*",
			"percolate-"+idx.Name+"-*",
		),
	)
	if err != nil {
		return fmt.Errorf("list indices: %w", err)
	}

	defer elephantine.SafeClose(
		c.logger, "indices list", listRes.Body)

	var indices []struct {
		Index string `json:"index"`
	}

	dec := json.NewDecoder(listRes.Body)

	err = dec.Decode(&indices)
	if err != nil {
		return fmt.Errorf("decode indices list: %w", err)
	}

	if len(indices) > 0 {
		names := make([]string, len(indices))

		for i := range indices {
			names[i] = indices[i].Index
		}

		del := client.Indices.Delete

		delRes, err := del(names,
			del.WithContext(ctx))
		if err != nil {
			return fmt.Errorf("delete indices: %w", err)
		}

		defer elephantine.SafeClose(
			c.logger, "indices delete response", delRes.Body)
	}

	err = q.DeleteIndexSet(ctx, idx.Name)
	if err != nil {
		return fmt.Errorf("delete index set from database: %w", err)
	}

	return nil
}

func (c *Coordinator) ensureActiveClient(set postgres.IndexSet) error {
	c.activeMut.Lock()
	defer c.activeMut.Unlock()

	current := c.activeSet
	if current == set.Name {
		return nil
	}

	client, err := c.opt.ClientGetter(context.Background(), set.Cluster.String)
	if err != nil {
		return fmt.Errorf(
			"get client for cluster %q: %w",
			set.Cluster.String, err)
	}

	c.activeClient = client
	c.activeSet = set.Name

	return nil
}

// PercolateDocument acts as a filter that only runs percolation for the
// currently active indexer.
func (c *Coordinator) PercolateDocument(
	setName string, index string, document map[string][]string, doc *newsdoc.Document,
) {
	c.activeMut.RLock()
	defer c.activeMut.RUnlock()

	if setName != c.activeSet {
		return
	}

	c.percolator.PercolateDocument(index, document, doc)
}

func (c *Coordinator) startIndexer(
	ctx context.Context, set postgres.IndexSet,
) (*Indexer, error) {
	client, err := c.opt.ClientGetter(ctx, set.Cluster.String)
	if err != nil {
		return nil, fmt.Errorf(
			"get client for cluster %q: %w",
			set.Cluster.String, err)
	}

	i, err := NewIndexer(ctx, IndexerOptions{
		Logger: c.logger.With(
			"cluster_name", set.Cluster.String,
			"indexer_name", set.Name,
		),
		SetName:           set.Name,
		Database:          c.db,
		Client:            client,
		Documents:         c.opt.Documents,
		Validator:         c.opt.Validator,
		Metrics:           c.opt.Metrics,
		LanguageOptions:   c.opt.Languages,
		Sharding:          c.opt.Sharding,
		EnablePercolation: true,
		Percolator:        c,
	})
	if err != nil {
		return nil, fmt.Errorf("create indexer: %w", err)
	}

	c.indexerGroup.Go(func() error {
		err := i.Run(c.indexerCtx)
		if errors.Is(err, context.Canceled) {
			// Don't treat cancel as an error.
			return nil
		} else if err != nil {
			return fmt.Errorf("run indexer for set %q: %w",
				set.Name, err)
		}

		return nil
	})

	go func() {
		select {
		case <-c.stop:
			_ = i.Stop(IndexerStopTimeout)
		case <-i.Stopping():
			return
		}
	}()

	return i, nil
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

			select {
			case <-time.After(10 * time.Minute):
			case <-c.stop:
				return
			}
		}

		// Wait between 12 and 24 hours.
		//nolint: gosec
		randomMinutes := time.Duration(rand.Intn(12*60)) * time.Minute
		delay := 12*time.Hour + randomMinutes

		select {
		case <-time.After(delay):
		case <-c.stop:
			return
		}
	}
}

// Delete old index sets that have been marked as deleted.
func (c *Coordinator) cleanup(ctx context.Context) error {
	//nolint:wrapcheck
	return pg.WithTX(ctx, c.db, func(tx pgx.Tx) error {
		q := postgres.New(tx)

		// Get any remaining deleted sets and delete their indices. This
		// should have been handled in the setUpdate() handler, but this
		// acts as a retry-mechanism.
		deleted, err := q.ListDeletedIndexSets(ctx)
		if err != nil {
			return fmt.Errorf("list deleted index sets: %w", err)
		}

		for _, name := range deleted {
			err := c.finaliseSetDelete(ctx, tx, name)
			if err != nil {
				return fmt.Errorf(
					"delete indices of %q: %w",
					name, err,
				)
			}
		}

		return nil
	})
}

// Ensure that we have a default cluster and index set. Starts with an ACCESS
// EXCLUSIVE lock on the cluster table, so only one instance will be running
// this check at any given time.
func (c *Coordinator) EnsureDefaultIndexSet(
	ctx context.Context,
	defaultClusterURL string,
	clusterAuth ClusterAuth,
) error {
	//nolint:wrapcheck
	return pg.WithTX(ctx, c.db, func(tx pgx.Tx) error {
		q := postgres.New(tx)

		// Completely lock the cluster table while we initialise.
		err := q.LockClusters(ctx)
		if err != nil {
			return fmt.Errorf("lock cluster table: %w", err)
		}

		clusters, err := q.GetClusters(ctx)
		if err != nil {
			return fmt.Errorf("list clusters: %w", err)
		}

		var clusterName string

		if len(clusters) > 0 {
			// This is not a fresh setup, leave things as is.
			return nil
		}

		clusterName = codename.Generate(c.nameRng, 0)

		authData, err := json.Marshal(clusterAuth)
		if err != nil {
			return fmt.Errorf("marshal cluster auth: %w", err)
		}

		err = q.AddCluster(ctx, postgres.AddClusterParams{
			Name: clusterName,
			Url:  defaultClusterURL,
			Auth: authData,
		})
		if err != nil {
			return fmt.Errorf("create default cluster: %w", err)
		}

		// Schema 002 and earlier can have null clusters. Phase
		// out before going 1.0.
		err = q.SetClusterWhereMissing(ctx, clusterName)
		if err != nil {
			return fmt.Errorf("set default cluster for index sets: %w", err)
		}

		indexName := codename.Generate(c.nameRng, 0)

		// Create a fresh index set.
		err = q.CreateIndexSet(ctx, postgres.CreateIndexSetParams{
			Name:    indexName,
			Cluster: clusterName,
			Active:  true,
			Enabled: true,
		})
		if err != nil {
			return fmt.Errorf("create default index set: %w", err)
		}

		err = c.indexStatuses.Publish(ctx, tx, IndexStatusChange{
			Name: indexName,
		})
		if err != nil {
			return fmt.Errorf(
				"notify of index set status change: %w",
				err)
		}

		return nil
	})
}
