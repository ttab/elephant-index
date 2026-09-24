package index

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/opensearch-project/opensearch-go/v2"
	"github.com/ttab/elephant-index/internal"
	"github.com/ttab/elephant-index/postgres"
	"github.com/ttab/elephantine"
	"github.com/ttab/elephantine/pg"
	"github.com/ttab/elephantine/pg/joblock"
	"github.com/ttab/flerr"
)

type PercolatorSpec struct {
	Targets []PercolatorTarget
	Query   map[string]any
}

type PercolatorTarget struct {
	Type     string
	Language string
}

type PercolatorSubscription struct {
	Fields       []string
	Source       bool
	LoadDocument bool
}

type PercolatorUpdate struct {
	ID      int64
	DocType string
	Deleted bool
}

type PercolatorReference struct {
	ID          int64
	DocType     string
	Language    string
	Query       map[string]any
	HasDocument map[string]bool
}

type PercolatorDocumentGetter interface {
	GetDocument(
		ctx context.Context,
		id int64,
	) (postgres.PercolatorDocument, error)
}

func NewPercolator(
	ctx context.Context,
	logger *slog.Logger,
	metrics *Metrics,
	db *pgxpool.Pool,
	index ActiveIndexGetter,
	lang *LanguageResolver,
	docs PercolatorDocumentGetter,
	percChanges *pg.FanOut[PercolatorUpdate],
	percolate *pg.FanOut[PercolateEvent],
	eventPercolated *pg.FanOut[EventPercolated],
) (*Percolator, error) {
	chUpdate := make(chan PercolatorUpdate, 16)

	go func() {
		percChanges.ListenAll(ctx, chUpdate)
		close(chUpdate)
	}()

	chPercolate := make(chan PercolateEvent, 16)

	go func() {
		percolate.ListenAll(ctx, chPercolate)
		close(chPercolate)
	}()

	p := Percolator{
		log:             logger,
		metrics:         metrics,
		db:              db,
		index:           index,
		lang:            lang,
		docs:            docs,
		pUpdate:         chUpdate,
		pEvent:          chPercolate,
		percolators:     make(map[string]map[int64]*PercolatorReference),
		eventPercolated: eventPercolated,
		percChanges:     percChanges,
	}

	q := postgres.New(db)

	state, err := q.GetAppState(ctx, "percolator")
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("read percolator state: %w", err)
	}

	if state.Percolator != nil {
		p.lastEvent = state.Percolator.CurrentPosition
	}

	defs, err := q.GetPercolators(ctx)
	if err != nil {
		return nil, fmt.Errorf("get percolators: %w", err)
	}

	for _, def := range defs {
		p.registerPercolator(newPercolatorReference(def))
	}

	// Get notified when percolators are created or deleted.
	go p.handlePercolatorUpdates(ctx)

	go p.percolationLoop(ctx)

	// Remove old percolation data.
	go p.cleanup(ctx)

	return &p, nil
}

// Percolator percolates documents for a single index set. This will become a
// bottleneck for indexing unless we add some concurrency.
type Percolator struct {
	log     *slog.Logger
	metrics *Metrics
	db      *pgxpool.Pool
	index   ActiveIndexGetter
	lang    *LanguageResolver
	docs    PercolatorDocumentGetter

	pUpdate chan PercolatorUpdate
	pEvent  chan PercolateEvent

	lastEvent int64

	pMutex    sync.RWMutex
	activeSet string
	// Percolators keyed by document type.
	percolators map[string]map[int64]*PercolatorReference

	eventPercolated *pg.FanOut[EventPercolated]
	percChanges     *pg.FanOut[PercolatorUpdate]
}

func (p *Percolator) cleanup(ctx context.Context) {
	q := postgres.New(p.db)

	for {
		// Subscriptions, events, and event payloads are expired by
		// age. Percolators are removed when they no longer have any
		// associated subscriptions.
		subscriptionCutoff := time.Now().Add(-30 * time.Minute)
		eventCutoff := time.Now().Add(-60 * time.Minute)
		eventPayloadCutoff := time.Now().Add(-90 * time.Minute)

		err := q.DeletePercolatorEvents(ctx, pg.Time(eventCutoff))
		if err != nil {
			p.log.ErrorContext(ctx, "clean up old events",
				elephantine.LogKeyError, err)
		}

		err = q.DeletePercolatorEventPayloads(ctx, pg.Time(eventPayloadCutoff))
		if err != nil {
			p.log.ErrorContext(ctx, "clean up old event payloads",
				elephantine.LogKeyError, err)
		}

		err = pg.WithTX(ctx, p.db, func(tx pgx.Tx) error {
			q := postgres.New(tx)

			deleteSubs, err := q.SubscriptionsToDelete(ctx, pg.Time(subscriptionCutoff))
			if err != nil {
				return fmt.Errorf("find subscriptions to delete: %w", err)
			}

			if len(deleteSubs) == 0 {
				return nil
			}

			err = q.DeleteSubscriptions(ctx, deleteSubs)
			if err != nil {
				return fmt.Errorf("delete subscriptions: %w", err)
			}

			// We probably don't need to send a notifications for
			// this. Unused subscriptions in the service layer cache
			// will expire on their own, and the side effect they
			// could cause is users being able to poll for events
			// after a subscription has been deleted. Non-harmful.

			return nil
		})
		if err != nil {
			p.log.ErrorContext(ctx, "clean up subscriptions",
				elephantine.LogKeyError, err)
		}

		err = pg.WithTX(ctx, p.db, func(tx pgx.Tx) error {
			q := postgres.New(tx)

			deletePercs, err := q.PercolatorsToDelete(ctx)
			if err != nil {
				return fmt.Errorf("find percolators to delete: %w", err)
			}

			if len(deletePercs) == 0 {
				return nil
			}

			err = q.MarkPercolatorsForDeletion(ctx, deletePercs)
			if err != nil {
				return fmt.Errorf("mark percolators as deleted: %w", err)
			}

			for _, id := range deletePercs {
				err := p.percChanges.Publish(ctx, tx, PercolatorUpdate{
					ID:      id,
					Deleted: true,
				})
				if err != nil {
					return fmt.Errorf("publish delete notification: %w", err)
				}
			}

			return nil
		})
		if err != nil {
			p.log.ErrorContext(ctx, "mark unused percolators for deletion: %w",
				elephantine.LogKeyError, err)
		}

		// This construct is a bit odd, but i's here to get a context
		// that we can return out of.
		err = func() error {
			deletePercs, err := q.GetPercolatorsMarkedForDeletion(ctx)
			if err != nil {
				return fmt.Errorf("get percolators marked for deletion: %w", err)
			}

			client, _ := p.index.GetActiveIndex()

			for _, perc := range deletePercs {
				err := p.purgePercolator(ctx, client, perc.ID, perc.DocType)
				if err != nil {
					p.log.ErrorContext(ctx, "purge percolator",
						"percolator_id", perc.ID,
						elephantine.LogKeyError, err)
				}
			}

			return nil
		}()
		if err != nil {
			p.log.ErrorContext(ctx, "clean up unused percolators",
				elephantine.LogKeyError, err)
		}

		select {
		case <-time.After(1 * time.Minute):
		case <-ctx.Done():
			return
		}
	}
}

func (p *Percolator) purgePercolator(
	ctx context.Context, client *opensearch.Client, id int64, docType string,
) error {
	return pg.WithTX(ctx, p.db, func(tx pgx.Tx) (outErr error) {
		q := postgres.New(tx)

		var clean flerr.Cleaner

		defer clean.FlushTo(&outErr)

		indices, err := q.GetPercolatorDocumentIndices(ctx, id)
		if err != nil {
			return fmt.Errorf("get percolator indices with created documents: %w", err)
		}

		for _, index := range indices {
			res, err := client.Delete(index, strconv.FormatInt(id, 10),
				client.Delete.WithContext(ctx),
			)
			if err != nil {
				return fmt.Errorf("make delete percolator document request: %w", err)
			}

			clean.Addf(res.Body.Close, "close delete response")

			if res.IsError() && res.StatusCode != http.StatusNotFound {
				// OpenSearch treats DELETE of non-existing doc
				// as a 200 OK, but we're guarding against the
				// possibility index itself not existing here.
				return fmt.Errorf("delete percolator document: %w", ElasticErrorFromResponse(res))
			}

			err = clean.Flush()
			if err != nil {
				return err //nolint: wrapcheck
			}
		}

		err = q.DeletePercolator(ctx, id)
		if err != nil {
			return fmt.Errorf("delete percolator from DB: %w", err)
		}

		err = p.percChanges.Publish(ctx, tx, PercolatorUpdate{
			ID:      id,
			DocType: docType,
			Deleted: true,
		})
		if err != nil {
			return fmt.Errorf("publish percolator change: %w", err)
		}

		return nil
	})
}

func (p *Percolator) percolationLoop(ctx context.Context) {
	// percolateEvents returns nil when the lock is lost, so this has to
	// keep contending for it rather than treat a nil return as done. See
	// Indexer.Run.
	err := joblock.Run(ctx, p.db, p.log,
		"percolator", "percolator",
		joblock.Options{
			MetricsRegisterer: p.metrics.Registerer,
		}, p.percolateEvents)
	if err != nil && !errors.Is(err, context.Canceled) {
		p.log.ErrorContext(ctx, "percolator stopped",
			elephantine.LogKeyError, err)
	}
}

const minPercolateInterval = 5 * time.Second

func (p *Percolator) percolateEvents(ctx context.Context) error {
	p.metrics.percolatorLife.WithLabelValues("acquire-lock").Inc()
	p.metrics.percolatorLife.WithLabelValues("start").Inc()

	defer p.metrics.percolatorLife.WithLabelValues("stop").Inc()

	q := postgres.New(p.db)

	// Another replica may have held the lock since we last did, so resume
	// from the persisted position rather than from our own.
	state, err := q.GetAppState(ctx, "percolator")
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("read percolator state: %w", err)
	}

	if state.Percolator != nil {
		p.lastEvent = state.Percolator.CurrentPosition
	}

	timer := time.NewTicker(minPercolateInterval)

	// We get events from the fanout when something is queued for
	// percolation.
	for {
		var evt PercolateEvent

		timer.Reset(minPercolateInterval)

		// Wait for notification, the minimum poll interval or context
		// cancel.
		select {
		case evt = <-p.pEvent:
			p.metrics.percolatorLife.WithLabelValues("triggered").Inc()
		case <-timer.C:
			p.metrics.percolatorLife.WithLabelValues("poll").Inc()
		case <-ctx.Done():
			return nil
		}

		// We don't have delivery guarantees for the events, so start at
		// the last event if we have a position.
		start := evt.ID
		if p.lastEvent != 0 {
			start = p.lastEvent + 1
		}

		// Same deal here, process to the last known event.
		end, err := q.GetLastPercolatorEventID(ctx)
		if err != nil {
			return fmt.Errorf("get last event ID: %w", err)
		}

		if end <= p.lastEvent {
			p.metrics.percolatorLife.WithLabelValues("no-work").Inc()

			continue
		}

		for id := start; id <= end; id++ {
			err := p.handleEventPercolation(ctx, id)
			if err != nil {
				return fmt.Errorf(
					"percolate event %d: %w", id, err)
			}

			p.metrics.percolationEvent.WithLabelValues(
				"percolate-event", "percolator",
			).Inc()

			p.lastEvent = id
		}

		p.metrics.percolatorPos.Set(float64(p.lastEvent))

		err = q.SetAppState(ctx, postgres.SetAppStateParams{
			Name: "percolator",
			Data: postgres.AppStateData{
				Percolator: &postgres.PercolatorState{
					CurrentPosition: p.lastEvent,
				},
			},
		})
		if err != nil {
			return fmt.Errorf("persist percolator state: %w", err)
		}

		p.metrics.percolatorLife.WithLabelValues("end-iteration").Inc()
	}
}

func (p *Percolator) handleEventPercolation(ctx context.Context, id int64) error {
	doc, err := p.docs.GetDocument(ctx, id)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	} else if err != nil {
		return fmt.Errorf("load document: %w", err)
	}

	client, set := p.index.GetActiveIndex()

	language, err := p.lang.GetLanguageInfo(doc.Document.Language)
	if err != nil {
		return fmt.Errorf("invalid document language: %w", err)
	}

	index := NewIndexName(IndexTypePercolate, set, doc.Document.Type, language)

	err = p.ensurePercolatorQueries(ctx, client, set, doc.Document.Type, index.Full)
	if err != nil {
		return fmt.Errorf("ensure queries: %w", err)
	}

	err = p.percolateDocument(ctx, client, index.Full, doc)
	if err != nil {
		return fmt.Errorf("percolate document: %w", err)
	}

	return nil
}

func (p *Percolator) preseedQuery(
	ctx context.Context,
	percolator postgres.Percolator,
	ref *PercolatorReference,
) error {
	// Only preseed for defined languages.
	if percolator.Language == "" {
		return nil
	}

	language, err := p.lang.GetLanguageInfo(percolator.Language)
	if err != nil {
		return fmt.Errorf("invalid document language: %w", err)
	}

	client, set := p.index.GetActiveIndex()

	index := NewIndexName(IndexTypePercolate, set, percolator.DocType, language)

	// Both failures here leave a registered subscription whose query is not
	// percolated against, which is what query-doc-error is the signal for.
	// The lazy path counts it, so this one has to as well, or the runbook
	// is only true for half the ways it happens.
	err = p.createPercolatorDocument(ctx, client, index.Full, ref)
	if err != nil {
		p.metrics.percolatorLife.WithLabelValues("query-doc-error").Inc()

		return err
	}

	err = p.refreshIndex(ctx, client, index.Full)
	if err != nil {
		p.metrics.percolatorLife.WithLabelValues("query-doc-error").Inc()

		return fmt.Errorf("refresh index: %w", err)
	}

	ref.HasDocument[index.Full] = true

	p.metrics.percolatorLife.WithLabelValues("query-doc-preseed").Inc()

	return nil
}

func (p *Percolator) ensurePercolatorQueries(
	ctx context.Context,
	client *opensearch.Client,
	activeSet string,
	docType string, index string,
) (outErr error) {
	unseeded := p.getUnseededPercolators(docType, activeSet, index)
	if len(unseeded) == 0 {
		return nil
	}

	var written int64

	for _, perc := range unseeded {
		err := p.createPercolatorDocument(ctx, client, index, perc)
		if err != nil {
			p.log.ErrorContext(ctx, "failed to create percolator document",
				"index", index,
				"percolator_id", perc.ID,
				elephantine.LogKeyError, err,
			)

			p.metrics.percolatorLife.WithLabelValues("query-doc-error").Inc()

			continue
		}

		p.pMutex.Lock()

		perc.HasDocument[index] = true

		p.pMutex.Unlock()

		p.metrics.percolatorLife.WithLabelValues("query-doc").Inc()

		written++
	}

	if written > 0 {
		err := p.refreshIndex(ctx, client, index)
		if err != nil {
			return fmt.Errorf("refresh index: %w", err)
		}
	}

	return nil
}

// refreshIndex makes the percolator queries written to the index evaluable.
//
// A refresh, not a flush: a flush is a Lucene commit, so it makes the write
// durable without reopening the searcher, and a query that has only been
// flushed stays invisible to percolation until the next periodic refresh.
// Percolating against an index whose queries are not all visible reports a
// document that should match as a non-match, so the error is returned rather
// than logged — the percolator retries the event from its last position, and
// a wrong answer is worse than a late one.
func (p *Percolator) refreshIndex(
	ctx context.Context, client *opensearch.Client, index string,
) (outErr error) {
	res, err := client.Indices.Refresh(
		client.Indices.Refresh.WithContext(ctx),
		client.Indices.Refresh.WithIndex(index),
	)
	if err != nil {
		return fmt.Errorf("make refresh request: %w", err)
	}

	defer elephantine.Close("refresh response", res.Body, &outErr)

	err = ElasticErrorFromResponse(res)
	if err != nil {
		return fmt.Errorf("refresh percolator index: %w", err)
	}

	return nil
}

func (p *Percolator) createPercolatorDocument(
	ctx context.Context,
	client *opensearch.Client,
	index string,
	perc *PercolatorReference,
) error {
	return pg.WithTX(ctx, p.db, func(tx pgx.Tx) (outErr error) {
		q := postgres.New(tx)

		err := q.RegisterPercolatorDocumentIndex(ctx,
			postgres.RegisterPercolatorDocumentIndexParams{
				Percolator: perc.ID,
				Index:      index,
			})
		if err != nil {
			return fmt.Errorf(
				"register percolation document in DB: %w", err)
		}

		body, err := json.Marshal(map[string]any{
			"query": perc.Query,
		})
		if err != nil {
			return fmt.Errorf("marshal percolator document: %w", err)
		}

		res, err := client.Create(
			index,
			strconv.FormatInt(perc.ID, 10),
			bytes.NewReader(body),
			client.Create.WithContext(ctx),
		)
		if err != nil {
			return fmt.Errorf("make create request: %w", err)
		}

		defer elephantine.Close("close response body", res.Body, &outErr)

		// Ignore the error response if it's caused by the percolator
		// document already existing, subsciptions are immutable.
		if res.IsError() && res.StatusCode != http.StatusConflict {
			return fmt.Errorf(
				"create query %d in percolator index: %w",
				perc.ID,
				ElasticErrorFromResponse(res))
		}

		return nil
	})
}

func (p *Percolator) getUnseededPercolators(
	docType string, activeSet string, index string,
) []*PercolatorReference {
	p.pMutex.Lock()
	defer p.pMutex.Unlock()

	setChange := p.activeSet != activeSet
	if setChange {
		// Clear the HasDocument maps.
		for _, t := range p.percolators {
			for _, p := range t {
				clear(p.HasDocument)
			}
		}

		p.activeSet = activeSet
	}

	byID, hasType := p.percolators[docType]
	if !hasType {
		return nil
	}

	var unseeded []*PercolatorReference

	for _, ref := range byID {
		if ref.HasDocument[index] {
			continue
		}

		unseeded = append(unseeded, ref)
	}

	return unseeded
}

func (p *Percolator) handlePercolatorUpdates(ctx context.Context) {
	for def := range p.pUpdate {
		err := p.handleUpdate(ctx, def)
		if err != nil {
			p.log.Error("handle percolator update",
				"percolator_id", def.ID,
				elephantine.LogKeyError, err)
		}
	}
}

func (p *Percolator) handleUpdate(
	ctx context.Context,
	change PercolatorUpdate,
) error {
	if change.Deleted {
		p.unregisterPercolator(change.DocType, change.ID)

		return nil
	}

	// Safe to check and then register without holding the lock across
	// both: handlePercolatorUpdates is the only writer of the percolator
	// set and is a single goroutine. Deletions arrive here too, as a
	// Deleted update published by purgePercolator, rather than reaching
	// into the map from the cleanup goroutine.
	if p.hasPercolator(change.DocType, change.ID) {
		return nil
	}

	q := postgres.New(p.db)

	def, err := q.GetPercolator(ctx, change.ID)
	if err != nil {
		return fmt.Errorf("load percolator definition: %w", err)
	}

	ref := newPercolatorReference(def)

	// Seed and refresh before registering, and hold no lock while doing
	// it. percolateDocument needs a read lock to see the percolator set,
	// so writing the query document under the write lock stalled
	// percolation for the length of a Postgres transaction and two
	// OpenSearch calls. Registering last gives the same guarantee for
	// free: percolateDocument either does not see this percolator yet,
	// which is a missed notification the delivery contract allows, or sees
	// one whose query is already evaluable.
	seedErr := p.preseedQuery(ctx, def, ref)

	// Registered even when seeding failed. The reference then carries no
	// index in HasDocument, so ensurePercolatorQueries writes and
	// refreshes the query before the next document is percolated against
	// it. Dropping it here would leave the subscription unregistered until
	// the service restarts.
	p.registerPercolator(ref)

	if seedErr != nil {
		return fmt.Errorf("preseed percolator document: %w", seedErr)
	}

	return nil
}

func newPercolatorReference(def postgres.Percolator) *PercolatorReference {
	return &PercolatorReference{
		ID:          def.ID,
		DocType:     def.DocType,
		Language:    def.Language,
		Query:       def.Query,
		HasDocument: make(map[string]bool),
	}
}

// registerPercolator makes the percolator visible to percolateDocument.
func (p *Percolator) registerPercolator(ref *PercolatorReference) {
	p.pMutex.Lock()
	defer p.pMutex.Unlock()

	m, ok := p.percolators[ref.DocType]
	if !ok {
		m = make(map[int64]*PercolatorReference)
		p.percolators[ref.DocType] = m
	}

	m[ref.ID] = ref
}

func (p *Percolator) unregisterPercolator(docType string, id int64) {
	p.pMutex.Lock()
	defer p.pMutex.Unlock()

	delete(p.percolators[docType], id)
}

func (p *Percolator) hasPercolator(docType string, id int64) bool {
	p.pMutex.RLock()
	defer p.pMutex.RUnlock()

	_, exists := p.percolators[docType][id]

	return exists
}

func (p *Percolator) percolateDocument(
	ctx context.Context,
	client *opensearch.Client,
	index string,
	doc postgres.PercolatorDocument,
) (outErr error) {
	payload, err := json.Marshal(internal.SearchRequestV1{
		Query: internal.QWrap("percolate", percolateQuery{
			Field:    "query",
			Document: doc.Fields,
		}),
	})
	if err != nil {
		return fmt.Errorf("marshal percolate document: %w", err)
	}

	// We want to collect all IDs of the percolators so that we know which
	// didn't match the query.
	//
	// The snapshot has to be taken before the search runs, not after. A
	// percolator that's registered while the search is in flight has a
	// query that was evaluable when it was registered, but that wasn't
	// necessarily in the index the search read. Including it in the
	// snapshot would report the document as a non-match against a query
	// that was never run. Taking the snapshot first means such a
	// percolator either doesn't appear at all, which is a missed
	// notification, or comes back as a hit and is recorded as a match.
	p.pMutex.RLock()

	allPercs := make(map[int64]bool, len(p.percolators[doc.Document.Type]))
	for k := range p.percolators[doc.Document.Type] {
		allPercs[k] = false
	}

	p.pMutex.RUnlock()

	res, err := client.Search(
		client.Search.WithContext(ctx),
		client.Search.WithIndex(index),
		client.Search.WithBody(bytes.NewReader(payload)),
	)
	if err != nil {
		return fmt.Errorf("run percolator query: %w", err)
	}

	defer elephantine.Close("response body: %w", res.Body, &outErr)

	dec := json.NewDecoder(res.Body)

	if res.IsError() {
		var elasticErr ElasticErrorResponse

		err := dec.Decode(&elasticErr)
		if err != nil {
			return errors.Join(
				fmt.Errorf("opensearch responded with: %s", res.Status()),
				fmt.Errorf("decode error response: %w", err),
			)
		}

		return fmt.Errorf("error response from opensearch: %s", res.Status())
	}

	var response searchResponse

	err = dec.Decode(&response)
	if err != nil {
		return fmt.Errorf("unmarshal opensearch response: %w", err)
	}

	// Bulk insert arrays.
	percolators := make([]int64, 0, len(allPercs))
	matches := make([]bool, 0, len(allPercs))

	for _, item := range response.Hits.Hits {
		id, err := strconv.ParseInt(item.ID, 10, 64)
		if err != nil {
			continue
		}

		// A hit that isn't in the snapshot is a percolator that was
		// registered while the search was in flight. It matched, so
		// record it as a match.
		percolators = append(percolators, id)
		matches = append(matches, true)

		allPercs[id] = true
	}

	for id, match := range allPercs {
		// Matches have already been added to the insert arrays.
		if match {
			continue
		}

		// TODO: This is where we could use a counting bloom filter to
		// only (probably) emit non-matches when we've had a previous
		// match.
		percolators = append(percolators, id)
		matches = append(matches, false)
	}

	if len(percolators) == 0 {
		return nil
	}

	err = pg.WithTX(ctx, p.db, func(tx pgx.Tx) error {
		q := postgres.New(tx)

		docUUID, _ := uuid.Parse(doc.Document.UUID)

		err = q.InsertPercolatorEvents(ctx, postgres.InsertPercolatorEventsParams{
			Percolators: percolators,
			Matched:     matches,
			ID:          doc.EventID,
			Document:    docUUID,
			Created:     pg.Time(time.Now()),
		})
		if err != nil {
			return fmt.Errorf("insert percolation result: %w", err)
		}

		err = p.eventPercolated.Publish(ctx, tx, EventPercolated{
			ID:          doc.EventID,
			Percolators: percolators,
		})
		if err != nil {
			return fmt.Errorf("notify about percolation result: %w", err)
		}

		return nil
	})
	if err != nil {
		return err //nolint: wrapcheck
	}

	return nil
}

type percolateQuery struct {
	Field    string              `json:"field"`
	Document map[string][]string `json:"document"`
}
