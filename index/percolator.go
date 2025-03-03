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
	"github.com/ttab/elephant-index/postgres"
	"github.com/ttab/elephantine"
	"github.com/ttab/elephantine/pg"
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
		p.registerPercolator(def)
	}

	go p.handlePercolatorUpdates(ctx)
	go p.percolationLoop(ctx)
	go p.cleanup(ctx)

	return &p, nil
}

// Percolator percolates documents for a single index set. This will become a
// bottleneck for indexing unless we add some concurrency.
type Percolator struct {
	log   *slog.Logger
	db    *pgxpool.Pool
	index ActiveIndexGetter
	lang  *LanguageResolver
	docs  PercolatorDocumentGetter

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
			p.log.ErrorContext(ctx, "clean up subscriptions",
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
	return pg.WithTX(ctx, p.db, func(tx pgx.Tx) (outErr error) { //nolint: wrapcheck
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
				// OS treats DELETE of non-existing doc as a 200
				// OK, but guarding against the index itself not
				// existing here.
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
	for {
		lock, err := pg.NewJobLock(
			p.db, p.log, "percolator",
			pg.JobLockOptions{})
		if err != nil {
			p.log.ErrorContext(ctx, "failed to create percolator job lock",
				elephantine.LogKeyError, err)

			time.Sleep(1 * time.Second)

			continue
		}

		err = lock.RunWithContext(ctx, p.percolateEvents)
		if err != nil {
			p.log.ErrorContext(ctx, "failed to percolate events",
				elephantine.LogKeyError, err)

			time.Sleep(1 * time.Second)

			continue
		}

		break
	}
}

func (p *Percolator) percolateEvents(ctx context.Context) error {
	q := postgres.New(p.db)

	for evt := range p.pEvent {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		if p.lastEvent >= evt.ID {
			continue
		}

		start := evt.ID
		if p.lastEvent != 0 {
			start = p.lastEvent
		}

		for id := start; id <= evt.ID; id++ {
			err := p.handleEventPercolation(ctx, id)
			if err != nil {
				return fmt.Errorf(
					"percolate event %d: %w", id, err)
			}

			p.lastEvent = id
		}

		err := q.SetAppState(ctx, postgres.SetAppStateParams{
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
	}

	return nil
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

			continue
		}

		p.pMutex.Lock()
		perc.HasDocument[index] = true
		p.pMutex.Unlock()

		written++
	}

	if written > 0 {
		// Flush the index so that we're guaranteed that the written
		// queries are evaluated.
		res, err := client.Indices.Flush(
			client.Indices.Flush.WithContext(ctx),
			client.Indices.Flush.WithIndex(index),
			client.Indices.Flush.WithWaitIfOngoing(true),
		)

		defer elephantine.Close("flush response", res.Body, &outErr)

		err = errors.Join(ElasticErrorFromResponse(res), err)
		if err != nil {
			p.log.Error(
				"failed to flush indices after percolator update",
				elephantine.LogKeyError, err)
		}
	}

	return nil
}

func (p *Percolator) createPercolatorDocument(
	ctx context.Context,
	client *opensearch.Client,
	index string,
	perc *PercolatorReference,
) error {
	return pg.WithTX(ctx, p.db, func(tx pgx.Tx) (outErr error) { //nolint: wrapcheck
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
	p.pMutex.Lock()
	defer p.pMutex.Unlock()

	m, knownType := p.percolators[change.DocType]

	if change.Deleted {
		if knownType {
			delete(m, change.ID)
		}

		return nil
	}

	_, exists := m[change.ID]
	if exists {
		return nil
	}

	q := postgres.New(p.db)

	def, err := q.GetPercolator(ctx, change.ID)
	if err != nil {
		return fmt.Errorf("load percolator definition: %w", err)
	}

	p.registerPercolator(def)

	return nil
}

func (p *Percolator) registerPercolator(def postgres.Percolator) {
	m, ok := p.percolators[def.DocType]
	if !ok {
		m = make(map[int64]*PercolatorReference)
		p.percolators[def.DocType] = m
	}

	ref := PercolatorReference{
		ID:          def.ID,
		Query:       def.Query,
		HasDocument: make(map[string]bool),
	}

	m[def.ID] = &ref
}

func (p *Percolator) percolateDocument(
	ctx context.Context,
	client *opensearch.Client,
	index string,
	doc postgres.PercolatorDocument,
) (outErr error) {
	payload, err := json.Marshal(searchRequestV1{
		Query: qWrap("percolate", percolateQuery{
			Field:    "query",
			Document: doc.Fields,
		}),
	})
	if err != nil {
		return fmt.Errorf("marshal percolate document: %w", err)
	}

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

	p.pMutex.RLock()
	// We want to collect all IDs of the
	allPercs := make(map[int64]bool, len(p.percolators[doc.Document.Type]))
	for k := range p.percolators[doc.Document.Type] {
		allPercs[k] = false
	}
	p.pMutex.RUnlock()

	// Bulk insert arrays.
	percolators := make([]int64, 0, len(allPercs))
	matches := make([]bool, 0, len(allPercs))

	for _, item := range response.Hits.Hits {
		id, err := strconv.ParseInt(item.ID, 10, 64)
		if err != nil {
			continue
		}

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
			ID:          doc.ID,
			Document:    docUUID,
			Created:     pg.Time(time.Now()),
		})
		if err != nil {
			return fmt.Errorf("insert percolation result: %w", err)
		}

		err = p.eventPercolated.Publish(ctx, tx, EventPercolated{
			ID:          doc.ID,
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
