package index

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lucasepe/codename"
	"github.com/opensearch-project/opensearch-go/v2"
	"github.com/ttab/elephant-api/repository"
	"github.com/ttab/elephant-index/postgres"
	"github.com/ttab/elephantine"
	"github.com/ttab/elephantine/pg"
	"golang.org/x/sync/errgroup"
)

const IndexerStopTimeout = 10 * time.Second

// DefaultIndexSetReconcileInterval is how often the coordinator re-reads the
// index sets from the database. The index_status_change notification is a
// latency optimisation on top of this, not the only way an activation reaches
// a replica.
const DefaultIndexSetReconcileInterval = 30 * time.Second

// The triggers that can bring a replica in line with the index sets in the
// database, used as the label on elephant_indexer_index_set_sync_total.
const (
	syncTriggerStartup      = "startup"
	syncTriggerNotification = "notification"
	syncTriggerReconnect    = "reconnect"
	syncTriggerTick         = "tick"
)

// indexSetQueryTimeout bounds the read at the start of a reconciliation. A
// silently dead Postgres connection would otherwise block the event loop for
// the TCP retransmit period with nothing logged, which is precisely the
// undetected-stall state the reconciliation exists to rule out.
const indexSetQueryTimeout = 10 * time.Second

const (
	ChanIndexStatusChange string = "index_status_change"
	ChanPercolatorUpdate  string = "percolator_update"
	ChanPercolateEvent    string = "percolate_event"
	ChanPercolated        string = "percolation_event"
)

type IndexStatusChange struct { //nolint: revive
	Name string
}

type PercolateEvent struct {
	ID int64
}

type EventPercolated struct {
	ID          int64
	Percolators []int64
}

type OpenSearchClientFunc func(
	ctx context.Context, cluster string,
) (*opensearch.Client, error)

type CoordinatorOptions struct {
	Logger          *slog.Logger
	Metrics         *Metrics
	Documents       repository.Documents
	ClientGetter    OpenSearchClientFunc
	Validator       ValidatorSource
	Languages       LanguageOptions
	Sharding        ShardingPolicy
	PercolatorCache *PercolatorDocCache
	NoIndexing      bool

	// ListenDatabase is the pool the notification subscriber takes its
	// LISTEN connection from. It has to be a direct connection to
	// PostgreSQL, as LISTEN does not work through a transaction pooler.
	// Nil means the pool the coordinator was created with.
	ListenDatabase *pgxpool.Pool

	// ReconcileInterval overrides how often the index sets are re-read from
	// the database. Zero means DefaultIndexSetReconcileInterval.
	ReconcileInterval time.Duration
}

type LanguageOptions struct {
	Substitutions   map[string]string
	DefaultLanguage string
	DefaultRegions  map[string]string
}

func StandardLanguageOptions(defaultLanguage string) LanguageOptions {
	return LanguageOptions{
		DefaultLanguage: defaultLanguage,
		Substitutions: map[string]string{
			"se": "sv",
		},
		DefaultRegions: map[string]string{
			"sv": "SE",
			"en": "GB",
			"es": "ES",
			"fr": "FR",
			"it": "IT",
			"de": "DE",
			"fi": "FI",
			"da": "DK",
			"nn": "NO",
			"no": "NO",
		},
	}
}

type Coordinator struct {
	logger     *slog.Logger
	opt        CoordinatorOptions
	nameRng    *rand.Rand
	db         *pgxpool.Pool
	q          *postgres.Queries
	startCount atomic.Int32
	lang       *LanguageResolver

	activeMut     sync.RWMutex
	activeClient  *opensearch.Client
	activeSet     string
	activeCluster string

	indexers     map[string]*Indexer
	indexerCtx   context.Context
	indexerGroup *errgroup.Group

	// activeMissing records that the last reconciliation found no active
	// index set, so the warning is logged on the transition rather than on
	// every pass. Owned by the coordinator event loop.
	activeMissing bool

	percolator *Percolator
	percDocs   *PercolatorDocCache

	indexStatuses *pg.FanOut[IndexStatusChange]
	changes       chan IndexStatusChange

	// reconcile carries a request to re-read every index set from the
	// database. Buffered to depth one so that requests coalesce and the
	// sender never blocks on the event loop.
	reconcile chan struct{}

	percolatorUpdate *pg.FanOut[PercolatorUpdate]
	percolateEvent   *pg.FanOut[PercolateEvent]
	eventPercolated  *pg.FanOut[EventPercolated]

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

	lang := NewLanguageResolver(opt.Languages)

	indexGrp, gCtx := errgroup.WithContext(context.Background())

	c := Coordinator{
		logger:           logger,
		db:               db,
		q:                postgres.New(db),
		lang:             lang,
		opt:              opt,
		nameRng:          rng,
		indexStatuses:    pg.NewFanOut[IndexStatusChange](ChanIndexStatusChange),
		percolatorUpdate: pg.NewFanOut[PercolatorUpdate](ChanPercolatorUpdate),
		percolateEvent:   pg.NewFanOut[PercolateEvent](ChanPercolateEvent),
		eventPercolated:  pg.NewFanOut[EventPercolated](ChanPercolated),
		changes:          make(chan IndexStatusChange),
		reconcile:        make(chan struct{}, 1),
		indexers:         make(map[string]*Indexer),
		indexerCtx:       gCtx,
		indexerGroup:     indexGrp,
		percDocs:         opt.PercolatorCache,
		stop:             make(chan struct{}),
		stopped:          make(chan struct{}),
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

	stopCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() {
		<-c.stop
		cancel()
	}()

	count := c.startCount.Add(1)
	if count > 1 {
		return errors.New("already started")
	}

	go c.cleanupLoop(ctx)

	fanOuts := []pg.ChannelSubscription{
		c.indexStatuses,
		c.percolatorUpdate,
		c.percolateEvent,
		c.eventPercolated,
	}

	// A notification that is sent while the LISTEN connection is silently
	// dead is lost for good, so every connect and reconnect asks the event
	// loop for a full reconciliation rather than trusting that nothing
	// happened while we were away.
	listenDB := c.opt.ListenDatabase
	if listenDB == nil {
		listenDB = c.db
	}

	sub := pg.NewSubscriber(c.logger, listenDB, fanOuts,
		pg.WithOnReconnect(func(_ context.Context) error {
			c.requestReconcile()

			return nil
		}),
	)

	go func() {
		err := sub.Run(stopCtx)
		if err != nil && !errors.Is(err, context.Canceled) {
			// Stop if the subscriber fails.
			c.stopOnce.Do(func() {
				close(c.stop)
			})

			c.logger.ErrorContext(stopCtx, "notification subscriber stopped",
				elephantine.LogKeyError, err)
		}
	}()

	go func() {
		c.indexStatuses.ListenAll(stopCtx, c.changes)
		close(c.changes)
	}()

	defer close(c.stopped)

	lang := NewLanguageResolver(c.opt.Languages)

	perc, err := NewPercolator(
		stopCtx, c.logger, c.opt.Metrics, c.db, c, lang, NewPercolatorDocCache(c.db),
		c.percolatorUpdate, c.percolateEvent, c.eventPercolated)
	if err != nil {
		c.stopOnce.Do(func() {
			close(c.stop)
		})

		return fmt.Errorf("create percolator: %w", err)
	}

	c.percolator = perc

	var errs []error

	err = c.runEventloop(ctx)
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

// requestReconcile asks the event loop to re-read every index set. It never
// blocks: a request already queued covers the caller as well.
func (c *Coordinator) requestReconcile() {
	select {
	case c.reconcile <- struct{}{}:
	default:
	}
}

func (c *Coordinator) runEventloop(
	ctx context.Context,
) error {
	err := c.reconcileSets(ctx)

	c.recordSetSync(syncTriggerStartup, err)

	if err != nil {
		return fmt.Errorf("initial index set reconciliation: %w", err)
	}

	ticker := time.NewTicker(c.reconcileInterval())
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case change := <-c.changes:
			err := c.handleChange(ctx, change)

			c.recordSetSync(syncTriggerNotification, err)

			if err != nil {
				c.logger.ErrorContext(ctx,
					"failed to apply an index set change, reconciling instead",
					"set_name", change.Name,
					elephantine.LogKeyError, err)

				// The sweep reads every set rather than the
				// one the notification named, so it is a
				// strictly better attempt at the same work.
				// Queued rather than run here so that it goes
				// through the same path as every other
				// reconciliation, and so that a notification
				// waiting behind this one is handled first.
				c.requestReconcile()
			}
		case <-c.reconcile:
			c.reconcileOrLog(ctx, syncTriggerReconnect)
		case <-ticker.C:
			c.reconcileOrLog(ctx, syncTriggerTick)
		}
	}
}

// reconcileOrLog runs a reconciliation and logs a failure instead of stopping
// the event loop. Unlike a notification, a reconciliation always has another
// attempt coming one interval later, so turning a transient database error
// into a process exit would only turn a Postgres failover into a crash loop on
// every replica.
func (c *Coordinator) reconcileOrLog(ctx context.Context, trigger string) {
	err := c.reconcileSets(ctx)

	c.recordSetSync(trigger, err)

	if err != nil {
		c.logger.ErrorContext(ctx, "failed to reconcile index sets",
			"trigger", trigger,
			"retry_in", c.reconcileInterval().String(),
			elephantine.LogKeyError, err)
	}
}

// recordSetSync counts an attempt to bring this replica in line with the
// index sets in the database.
//
// The successes are as much the point as the failures: they are the only
// evidence that a replica's event loop is still running at all, which nothing
// else reports. A `tick` success rate that has gone to zero on one replica is
// a stalled event loop, and that replica's routing is frozen wherever it
// happened to be.
func (c *Coordinator) recordSetSync(trigger string, err error) {
	result := "ok"
	if err != nil {
		result = "failed"
	}

	c.opt.Metrics.indexSetSync.WithLabelValues(trigger, result).Inc()
}

func (c *Coordinator) reconcileInterval() time.Duration {
	if c.opt.ReconcileInterval > 0 {
		return c.opt.ReconcileInterval
	}

	return DefaultIndexSetReconcileInterval
}

// reconcileSets brings this replica in line with the index sets in the
// database: it starts and stops indexers and resolves the active set and its
// client. Everything else is a notification that saves us waiting for the next
// reconciliation; this is the path that is authoritative.
func (c *Coordinator) reconcileSets(ctx context.Context) error {
	readCtx, cancel := context.WithTimeout(ctx, indexSetQueryTimeout)
	defer cancel()

	sets, err := c.q.GetIndexSets(readCtx)
	if err != nil {
		return fmt.Errorf("failed to get the current index sets: %w", err)
	}

	return c.applyIndexSets(ctx, sets)
}

// applyIndexSets is the half of a reconciliation that acts on what the
// database said. Split out from the read so that it can be exercised without
// a database.
//
// It must only be called from the coordinator event loop, as setUpdate mutates
// the unsynchronised indexers map.
//
// Sets are independent, so one that cannot be applied must not stop the
// others: a set whose cluster is unreachable would otherwise block the
// active-set switch for every set after it in the list, which is the drift
// this whole mechanism exists to prevent. The failures are collected and
// returned together, and the caller logs and comes back next interval.
func (c *Coordinator) applyIndexSets(
	ctx context.Context, sets []postgres.IndexSet,
) error {
	var (
		errs        []error
		activeFound bool
	)

	known := make(map[string]bool, len(sets))

	for _, set := range sets {
		known[set.Name] = true

		if set.Active {
			activeFound = true
		}

		if c.opt.NoIndexing {
			if set.Active {
				err := c.ensureActiveClient(set)
				if err != nil {
					errs = append(errs, fmt.Errorf(
						"failed to ensure active client %q: %w",
						set.Name, err))
				}
			}

			continue
		}

		err := c.setUpdate(ctx, set)
		if err != nil {
			errs = append(errs, fmt.Errorf(
				"set up index set %q: %w",
				set.Name, err,
			))
		}
	}

	// GetIndexSets excludes deleted sets, so a set that was deleted while we
	// missed its notification is simply absent here. Its indexer would
	// otherwise keep following the log into indices that are being removed.
	for name, idxr := range c.indexers {
		if known[name] {
			continue
		}

		delete(c.indexers, name)

		// Stop only fails by timing out, and it has already signalled
		// the indexer to stop by then, so there is nothing a retry
		// would do and no reason to abandon the rest of the sweep.
		err := idxr.Stop(IndexerStopTimeout)
		if err != nil {
			c.logger.ErrorContext(ctx,
				"indexer for a removed index set did not stop in time",
				"set_name", name,
				elephantine.LogKeyError, err)
		}
	}

	c.noteActiveSetPresence(activeFound)

	return errors.Join(errs...)
}

// noteActiveSetPresence warns when the database holds no active index set.
//
// The read path is deliberately left pointing at whatever it last had. An
// active set can only be replaced by another one through the API, so this
// state is only reachable by hand, and in that case a replica that keeps
// serving slightly stale results is the better of the two failures: dropping
// the client would make GetActiveIndex return nil, and the percolator's
// cleanup and subscription goroutines use that client without a nil check and
// without a recover, so every replica would exit instead.
//
// The warning is logged on the transition rather than on every reconciliation.
func (c *Coordinator) noteActiveSetPresence(activeFound bool) {
	if activeFound {
		c.activeMissing = false

		return
	}

	if c.activeMissing {
		return
	}

	c.activeMissing = true

	_, current := c.GetActiveIndex()

	c.logger.Warn("no index set is marked active in the database",
		"serving_set_name", current,
	)
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
) (outErr error) {
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

	defer elephantine.Close("indices list", listRes.Body, &outErr)

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

		defer elephantine.Close(
			"indices delete response", delRes.Body, &outErr)
	}

	err = q.DeleteIndexSet(ctx, idx.Name)
	if err != nil {
		return fmt.Errorf("delete index set from database: %w", err)
	}

	return nil
}

// ensureActiveClient points the read path at the given index set. The cluster
// is part of the comparison, not just the name: a set that is moved to another
// cluster keeps its name, and reacting only to the name would leave every
// search going to the cluster the set no longer lives in.
func (c *Coordinator) ensureActiveClient(set postgres.IndexSet) error {
	c.activeMut.Lock()
	defer c.activeMut.Unlock()

	if c.activeSet == set.Name && c.activeCluster == set.Cluster.String {
		return nil
	}

	client, err := c.opt.ClientGetter(context.Background(), set.Cluster.String)
	if err != nil {
		return fmt.Errorf(
			"get client for cluster %q: %w",
			set.Cluster.String, err)
	}

	previousSet := c.activeSet
	previousCluster := c.activeCluster

	c.activeClient = client
	c.activeSet = set.Name
	c.activeCluster = set.Cluster.String

	// Logged at info because this is the only after-the-fact record of which
	// replicas picked an activation up, and the metric below only says where
	// a replica is now.
	c.logger.Info("switched active index set",
		"previous_set_name", previousSet,
		"previous_cluster", previousCluster,
		"set_name", set.Name,
		"cluster", set.Cluster.String,
	)

	c.opt.Metrics.activeIndexSet.Reset()
	c.opt.Metrics.activeIndexSet.WithLabelValues(
		set.Name, set.Cluster.String,
	).Set(1)

	return nil
}

// RequestDocumentPercolation acts as a filter that only runs percolation for
// the currently active indexer.
func (c *Coordinator) RequestDocumentPercolation(
	ctx context.Context,
	setName string,
	documents []postgres.PercolatorDocument,
) {
	if len(documents) == 0 {
		return
	}

	c.activeMut.RLock()
	defer c.activeMut.RUnlock()

	if setName != c.activeSet {
		c.opt.Metrics.percolationEvent.WithLabelValues(
			"inactive_set", setName,
		).Inc()

		return
	}

	for _, doc := range documents {
		c.percDocs.CacheDocument(doc)
	}

	err := pg.WithTX(ctx, c.db, func(tx pgx.Tx) error {
		q := postgres.New(tx)

		for _, doc := range documents {
			err := q.InsertPercolatorEventPayload(ctx, postgres.InsertPercolatorEventPayloadParams{
				ID:      doc.EventID,
				Created: pg.Time(time.Now()),
				Data:    doc,
			})
			if err != nil {
				return fmt.Errorf("store percolator document: %w", err)
			}
		}

		lastID := documents[len(documents)-1].EventID

		err := c.percolateEvent.Publish(ctx, tx, PercolateEvent{
			ID: lastID,
		})
		if err != nil {
			return fmt.Errorf("send percolate event: %w", err)
		}

		c.opt.Metrics.percolationEvent.WithLabelValues(
			"queued", setName,
		).Inc()

		return nil
	})
	if err != nil {
		c.opt.Metrics.percolationEvent.WithLabelValues(
			"queue_failed", setName,
		).Inc()

		c.logger.ErrorContext(ctx, "failed to queue documents for percolation",
			elephantine.LogKeyEventID, documents[0].EventID,
			elephantine.LogKeyError, err,
		)
	}
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
		Language:          c.lang,
		Sharding:          c.opt.Sharding,
		EnablePercolation: true,
		Percolator:        c,
	})
	if err != nil {
		return nil, fmt.Errorf("create indexer: %w", err)
	}

	c.indexerGroup.Go(func() error {
		err := elephantine.CallWithRecover(c.indexerCtx, i.Run)
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
		err := elephantine.CallWithRecover(ctx, c.cleanup)
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
	defaultClusterURL *url.URL,
	clusterAuth ClusterAuth,
) error {
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

		//nolint:gosec // ClusterAuth.Password holds an encrypted value, not plaintext
		authData, err := json.Marshal(clusterAuth)
		if err != nil {
			return fmt.Errorf("marshal cluster auth: %w", err)
		}

		err = q.AddCluster(ctx, postgres.AddClusterParams{
			Name: clusterName,
			Url:  defaultClusterURL.String(),
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
