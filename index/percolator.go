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
		err := p.registerPercolator(def)
		if err != nil {
			return nil, fmt.Errorf(
				"register percolator %d: %w",
				def.ID, err)
		}
	}

	go p.handlePercolatorUpdates(ctx)
	go p.percolateEvents(ctx)
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
		subscriptionCutoff := time.Now().Add(-1 * time.Hour)
		eventCutoff := time.Now().Add(-90 * time.Minute)
		eventPayloadCutoff := time.Now().Add(-120 * time.Minute)

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
			p.log.ErrorContext(ctx, "clean up subscriptions",
				elephantine.LogKeyError, err)
		}

		err = pg.WithTX(ctx, p.db, func(tx pgx.Tx) error {
			q := postgres.New(tx)

			deletePercs, err := q.GetPercolatorsMarkedForDeletion(ctx)

			return nil
		})
	}
}

func (p *Percolator) percolateEvents(ctx context.Context) {
	q := postgres.New(p.db)

	for evt := range p.pEvent {
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
				p.log.ErrorContext(ctx, "percolation failed",
					elephantine.LogKeyEventID, id,
					elephantine.LogKeyError, err)
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
			p.log.ErrorContext(ctx, "failed to persist percolator state",
				elephantine.LogKeyEventID, p.lastEvent,
				elephantine.LogKeyError, err)
		}
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

	var (
		clean   flerr.Cleaner
		written int64
	)

	defer clean.FlushTo(&outErr)

	for _, p := range unseeded {
		body, err := json.Marshal(map[string]any{
			"query": p.Query,
		})
		if err != nil {
			return fmt.Errorf("marshal percolator document: %w", err)
		}

		res, err := client.Create(
			index,
			strconv.FormatInt(p.ID, 10),
			bytes.NewReader(body),
			client.Create.WithContext(ctx),
		)
		if err != nil {
			return fmt.Errorf("make create request: %w", err)
		}

		clean.Addf(res.Body.Close, "close response body")

		// Ignore the error reponse if it's caused by the percolator
		// document already existing, subsciptions are immutable.
		if res.IsError() && res.StatusCode != http.StatusConflict {
			return fmt.Errorf(
				"create query %d in percolator index: %w",
				p.ID,
				ElasticErrorFromResponse(res))
		}

		err = clean.Flush()
		if err != nil {
			return err
		}

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

		clean.Addf(res.Body.Close, "close flush response")

		err = errors.Join(ElasticErrorFromResponse(res), err)
		if err != nil {
			p.log.Error(
				"failed to flush indices after percolator update",
				elephantine.LogKeyError, err)
		}
	}

	return nil
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

	err = p.registerPercolator(def)
	if err != nil {
		return err
	}

	return nil
}

func (p *Percolator) registerPercolator(def postgres.Percolator) error {
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

	return nil
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
		return
	}

	res, err := client.Search(
		client.Search.WithContext(ctx),
		client.Search.WithIndex(index),
		client.Search.WithBody(bytes.NewReader(payload)),
	)

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

	if len(response.Hits.Hits) == 0 {
		return nil
	}

	percolators := make([]int64, 0, len(response.Hits.Hits))

	for _, item := range response.Hits.Hits {
		id, err := strconv.ParseInt(item.ID, 10, 64)
		if err != nil {
			continue
		}

		percolators = append(percolators, id)
	}

	err = pg.WithTX(ctx, p.db, func(tx pgx.Tx) error {
		q := postgres.New(tx)

		docUUID, _ := uuid.Parse(doc.Document.UUID)

		err = q.InsertPercolatorEvents(ctx, postgres.InsertPercolatorEventsParams{
			Percolators: percolators,
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
		return err
	}

	return nil
}

type percolateQuery struct {
	Field    string              `json:"field"`
	Document map[string][]string `json:"document"`
}
