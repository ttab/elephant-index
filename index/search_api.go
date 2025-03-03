package index

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/gobwas/glob"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ttab/elephant-api/index"
	"github.com/ttab/elephant-api/newsdoc"
	"github.com/ttab/elephant-api/repository"
	"github.com/ttab/elephant-index/postgres"
	"github.com/ttab/elephantine"
	"github.com/ttab/elephantine/pg"
	"github.com/twitchtv/twirp"
	"github.com/viccon/sturdyc"
)

const (
	DefaultSearchSize = 50
)

var _ index.SearchV1 = &SearchServiceV1{}

type MappingSource interface {
	GetMappings(
		ctx context.Context,
		indexSet string,
		docType string,
	) (map[string]Mapping, error)
}

func NewSearchServiceV1(
	logger *slog.Logger,
	db *pgxpool.Pool,
	mappings MappingSource,
	active ActiveIndexGetter,
	documents repository.Documents,
	percChanges *pg.FanOut[PercolatorUpdate],
	eventPercolated *pg.FanOut[EventPercolated],
	percDocs PercolatorDocumentGetter,
) *SearchServiceV1 {
	return &SearchServiceV1{
		log:             logger,
		db:              db,
		mappings:        mappings,
		active:          active,
		documents:       documents,
		percDocs:        percDocs,
		percChanges:     percChanges,
		eventPercolated: eventPercolated,
		subscriptions: sturdyc.New[userSub](
			5000, 5, 30*time.Minute, 10,
		),
	}
}

type SearchServiceV1 struct {
	log             *slog.Logger
	db              *pgxpool.Pool
	mappings        MappingSource
	active          ActiveIndexGetter
	documents       repository.Documents
	percDocs        PercolatorDocumentGetter
	percChanges     *pg.FanOut[PercolatorUpdate]
	eventPercolated *pg.FanOut[EventPercolated]
	subscriptions   *sturdyc.Client[userSub]
}

// EndSubscription implements index.SearchV1.
func (s *SearchServiceV1) EndSubscription(
	_ context.Context, _ *index.EndSubscriptionRequest,
) (*index.EndSubscriptionResponse, error) {
	panic("unimplemented")
}

// PollSubscription implements index.SearchV1.
func (s *SearchServiceV1) PollSubscription(
	ctx context.Context, req *index.PollSubscriptionRequest,
) (*index.PollSubscriptionResponse, error) {
	auth, err := RequireAnyScope(ctx, ScopeSearch, ScopeIndexAdmin)
	if err != nil {
		return nil, err
	}

	if len(req.Subscriptions) == 0 {
		return nil, twirp.RequiredArgumentError("subscriptions")
	}

	batchDelay := time.Duration(req.BatchDelayMs) * time.Millisecond
	if req.BatchDelayMs == 0 {
		batchDelay = 200 * time.Millisecond
	}

	maxWait := time.Duration(req.MaxWaitMs) * time.Millisecond
	if req.MaxWaitMs == 0 {
		maxWait = 10 * time.Second
	}

	// Requested subscriptions
	var (
		subIDs    []int64
		subCursor []int64
	)

	// Validate and decompose.
	for _, s := range req.Subscriptions {
		if s.Id == 0 {
			return nil, twirp.RequiredArgumentError("subscriptions.id")
		}

		if slices.Contains(subIDs, s.Id) {
			return nil, twirp.InvalidArgumentError("subscription.id",
				fmt.Sprintf("the subscription %d was specified more than once", s.Id))
		}

		subIDs = append(subIDs, s.Id)
		subCursor = append(subCursor, s.Cursor)
	}

	q := postgres.New(s.db)

	subscriptions, err := s.getSubscriptionsForUser(ctx, q, auth.Claims.Subject, subIDs)
	if err != nil {
		return nil, twirp.InternalErrorf("get subscription details: %w", err)
	}

	// Underlying percolators for the subscription.
	var percIDs []int64

	// Store the subscription definition so that we can access it easily,
	// this is needed to apply things like fields and load document.
	subDefs := make(map[int64]userSub)

	for _, sub := range subscriptions {
		for _, rSub := range req.Subscriptions {
			if rSub.Id != sub.ID {
				continue
			}

			sub.Cursor = rSub.Cursor
		}

		subDefs[sub.ID] = sub

		if !slices.Contains(percIDs, sub.Percolator) {
			percIDs = append(percIDs, sub.Percolator)
		}
	}

	// Collect the lowest cursor for each percolator.
	cursorCollect := make(map[int64]int64)
	// Map the percolators to their subscriptions, a client might have two
	// subscriptions for the same percolator with different subsciption
	// specs (fields et.c.).
	percSubs := make(map[int64][]int64)

	// Unknown subs will be reported back to the client.
	var (
		unknownSubs []int64
		knownSubs   []int64
	)

	for i, subID := range subIDs {
		def, ok := subDefs[subID]
		if !ok {
			unknownSubs = append(unknownSubs, subID)

			continue
		}

		knownSubs = append(knownSubs, subID)

		c := cursorCollect[def.Percolator]
		if c == 0 {
			c = subCursor[i]
		} else {
			c = min(c, subCursor[i])
		}

		cursorCollect[def.Percolator] = c

		percSubs[def.Percolator] = append(percSubs[def.Percolator], subID)
	}

	err = q.TouchSubscriptions(ctx, postgres.TouchSubscriptionsParams{
		Touched: pg.Time(time.Now()),
		Ids:     knownSubs,
	})
	if err != nil {
		return nil, twirp.InternalErrorf("failed to set subscriptions as touched: %v", err)
	}

	// All subscriptions were unknown.
	if len(cursorCollect) == 0 {
		return &index.PollSubscriptionResponse{
			UnknownSubscriptions: unknownSubs,
		}, nil
	}

	deadline := time.Now().Add(maxWait)
	events := make(chan EventPercolated)

	// Subscribe to percolation events, but only accept percolation events
	// that match one of our percolator IDs and are after our cursor.
	go s.eventPercolated.Listen(ctx, events, func(v EventPercolated) bool {
		for _, s := range v.Percolators {
			cursor, ok := cursorCollect[s]
			if ok && v.ID > cursor {
				return true
			}
		}

		return false
	})

	res := index.PollSubscriptionResponse{
		UnknownSubscriptions: unknownSubs,
	}

	// Collect the percolator cursors in an array in the same order as the
	// percIDs slice so that they can be unnested together in the
	// FetchPercolatorEvents query.
	percCursors := make([]int64, len(percIDs))

	for idx, id := range percIDs {
		percCursors[idx] = cursorCollect[id]
	}

	// Collect the poll result by subscription id.
	p := make(map[int64]*index.SubscriptionPollResult)

	for i := range 2 {
		items, err := q.FetchPercolatorEvents(ctx,
			postgres.FetchPercolatorEventsParams{
				Ids:         percCursors,
				Percolators: percIDs,
				Limit:       30,
			})
		if err != nil {
			return nil, fmt.Errorf("fetch events: %w", err)
		}

		for _, item := range items {
			for _, subID := range percSubs[item.Percolator] {
				sub := subDefs[subID]

				if item.ID <= sub.Cursor {
					continue
				}

				doc, err := s.percDocs.GetDocument(ctx, item.ID)
				if err != nil {
					s.log.WarnContext(ctx,
						"failed to load document for poll result",
						elephantine.LogKeyEventID, item.ID,
						elephantine.LogKeyError, err)
				}

				r, ok := p[subID]
				if !ok {
					r = &index.SubscriptionPollResult{
						Subscription: &index.SubscriptionReference{
							Id: subID,
						},
					}

					p[subID] = r
				}

				// Items are sorted in ascending order by ID so
				// just set the cursor.
				r.Subscription.Cursor = item.ID
				r.Items = append(r.Items, documentToItem(item, sub, doc))
			}
		}

		if len(items) > 0 || i > 0 {
			break
		}

		if !batchWait(ctx, events, deadline, 10, batchDelay) {
			return &res, nil
		}
	}

	for _, sub := range p {
		res.Result = append(res.Result, sub)
	}

	return &res, nil
}

// Wait until we're likely to fill a batch or batchDuration has passed since we
// got the first item. Returns false if we hit the deadline or the context is
// cancelled before we get any item.
func batchWait(ctx context.Context,
	events chan EventPercolated, deadline time.Time,
	batchSize int, batchDuration time.Duration,
) bool {
	var (
		got       int
		batchDone <-chan time.Time
	)

	doneWaiting := time.After(time.Until(deadline))

	for {
		select {
		case <-batchDone:
			return true
		case <-events:
			if got == 0 {
				// Force finish batch after batch duration or
				// just before we hit the request deadline.
				batchDone = time.After(min(
					batchDuration,
					time.Until(deadline)-1*time.Millisecond,
				))
			}

			got++

			if got == batchSize {
				return true
			}
		case <-doneWaiting:
			return false
		case <-ctx.Done():
			return false
		}
	}
}

func documentToItem(
	item postgres.FetchPercolatorEventsRow,
	sub userSub,
	doc postgres.PercolatorDocument,
) *index.SubscriptionItem {
	hit := index.SubscriptionItem{
		Id:    doc.Document.UUID,
		Match: item.Matched,
	}

	if !item.Matched {
		return &hit
	}

	if sub.Spec.LoadDocuments {
		hit.Document = newsdoc.DocumentToRPC(*doc.Document)
	}

	if sub.Spec.Source {
		hit.Source = make(map[string]*index.FieldValuesV1)

		for key, values := range doc.Fields {
			hit.Source[key] = &index.FieldValuesV1{
				Values: values,
			}
		}
	}

	if sub.Filter != nil {
		hit.Fields = make(map[string]*index.FieldValuesV1)

		for field, values := range doc.Fields {
			if !sub.Filter.Includes(field) {
				continue
			}

			hit.Fields[field] = &index.FieldValuesV1{
				Values: values,
			}
		}
	}

	return &hit
}

type FieldFilter struct {
	globs []glob.Glob
	exact map[string]struct{}
}

func NewFieldFilter(fields []string) (*FieldFilter, error) {
	ff := FieldFilter{
		exact: make(map[string]struct{}),
	}

	for _, field := range fields {
		if !strings.Contains(field, "*") {
			ff.exact[field] = struct{}{}

			continue
		}

		g, err := glob.Compile(field, '.')
		if err != nil {
			return nil, fmt.Errorf(
				"invalid field expression %q: %w", field, err)
		}

		ff.globs = append(ff.globs, g)
	}

	return &ff, nil
}

func (ff *FieldFilter) Includes(field string) bool {
	if _, ok := ff.exact[field]; ok {
		return true
	}

	for i := range ff.globs {
		if ff.globs[i].Match(field) {
			return true
		}
	}

	return false
}

type userSub struct {
	postgres.GetSubscriptionsRow

	Cursor int64
	Filter *FieldFilter
}

func (s *SearchServiceV1) getSubscriptionsForUser(
	ctx context.Context, q *postgres.Queries, client string, subIDs []int64,
) ([]userSub, error) {
	subCacheKeys := make([]string, len(subIDs))

	for i, id := range subIDs {
		subCacheKeys[i] = strconv.FormatInt(id, 10)
	}

	res, err := s.subscriptions.GetOrFetchBatch(ctx, subCacheKeys, func(id string) string {
		// Make sure that the cache key is scoped per user.
		return id + " " + client
	}, func(ctx context.Context, ids []string) (map[string]userSub, error) {
		missIDs := make([]int64, len(ids))

		for idx, s := range ids {
			// Ignoring error here, but we know that these were
			// created using FormatInt().
			missIDs[idx], _ = strconv.ParseInt(s, 10, 64)
		}

		subscriptions, err := q.GetSubscriptions(ctx, postgres.GetSubscriptionsParams{
			Subscriptions: missIDs,
			Client:        client,
		})
		if err != nil {
			return nil, fmt.Errorf("load subscriptions from DB: %w", err)
		}

		result := make(map[string]userSub)

		for _, sub := range subscriptions {
			var filter *FieldFilter
			if len(sub.Spec.Fields) > 0 {
				filter, _ = NewFieldFilter(sub.Spec.Fields)
			}

			result[strconv.FormatInt(sub.ID, 10)] = userSub{
				GetSubscriptionsRow: sub,
				Filter:              filter,
			}
		}

		return result, nil
	})
	if err != nil {
		return nil, err //nolint: wrapcheck
	}

	subs := make([]userSub, 0, len(res))

	for _, sub := range res {
		subs = append(subs, sub)
	}

	return subs, nil
}

// GetMappings implements index.SearchV1.
func (s *SearchServiceV1) GetMappings(
	ctx context.Context, req *index.GetMappingsRequestV1,
) (*index.GetMappingsResponseV1, error) {
	_, err := RequireAnyScope(ctx, ScopeSearch, ScopeIndexAdmin)
	if err != nil {
		return nil, err
	}

	_, set := s.active.GetActiveIndex()

	if req.DocumentType == "" {
		return nil, twirp.RequiredArgumentError("document_type")
	}

	mappings, err := s.mappings.GetMappings(ctx, set, req.DocumentType)
	if err != nil {
		return nil, twirp.InternalErrorf("read mappings: %w", err)
	}

	res := index.GetMappingsResponseV1{
		Properties: make([]*index.MappingPropertyV1, 0, len(mappings)),
	}

	for name, prop := range mappings {
		t, ok := fieldTypeToExternalType(prop.Type)
		if !ok {
			return nil, twirp.InternalErrorf(
				"unknown mapping type %q for %q",
				prop.Type, prop.Path,
			)
		}

		p := index.MappingPropertyV1{
			Name: name,
			Path: prop.Path,
			Type: t,
		}

		for fName, sf := range prop.Fields {
			t, ok := fieldTypeToExternalType(sf.Type)
			if !ok {
				return nil, twirp.InternalErrorf(
					"unknown mapping type %q for field %q of %q",
					sf.Type, fName, name,
				)
			}

			switch sf.Analyzer {
			case "elephant_prefix_analyzer":
				t = "prefix"
			case "":
			default:
				// We don't want to miscategorise any fields
				// with special analysers.
				return nil, twirp.InternalErrorf(
					"unknown analyzer %q for field %q of %q",
					sf.Analyzer, fName, name,
				)
			}

			p.Fields = append(p.Fields, &index.MappingFieldV1{
				Name: fName,
				Type: t,
			})
		}

		res.Properties = append(res.Properties, &p)
	}

	slices.SortFunc(res.Properties, func(
		a *index.MappingPropertyV1,
		b *index.MappingPropertyV1,
	) int {
		return strings.Compare(a.Name, b.Name)
	})

	return &res, nil
}

func fieldTypeToExternalType(ft FieldType) (string, bool) {
	// Convert field type to external type. This switch statement must
	// always be exhaustive.
	switch ft {
	case TypeAlias, TypeBoolean, TypeDate, TypeDouble,
		TypeKeyword, TypeLong, TypeText:
		return string(ft), true
	case TypeICUKeyword:
		return "keyword", true
	case TypeUnknown, TypePercolator:
		return "", false
	}

	return "", false
}

// Query implements index.SearchV1.
func (s *SearchServiceV1) Query(
	ctx context.Context, req *index.QueryRequestV1,
) (_ *index.QueryResponseV1, outErr error) {
	auth, err := RequireAnyScope(ctx, ScopeSearch, ScopeIndexAdmin)
	if err != nil {
		return nil, err
	}

	if req.Size == 0 {
		req.Size = DefaultSearchSize
	}

	if req.LoadDocument && req.Size > 200 {
		return nil, twirp.InvalidArgumentError("documents",
			"document loading is not allowed for result sets over 200 items")
	}

	paginated := req.From != 0 || len(req.SearchAfter) > 0

	if req.Subscribe && paginated {
		return nil, twirp.InvalidArgumentError("subscribe",
			"pagination cannot be used with subscriptions")
	}

	if req.Subscribe && req.DocumentType == "" {
		return nil, twirp.InvalidArgumentError("subscribe",
			"document type is required for subscriptions")
	}

	client, indexSet := s.active.GetActiveIndex()

	indexPattern := "documents-" + indexSet

	if req.DocumentType != "" {
		indexPattern += "-" + nonAlphaNum.ReplaceAllString(req.DocumentType, "_")
	} else {
		indexPattern += "-*"
	}

	if req.Language != "" {
		indexPattern += "-" + req.Language

		// Add a tailing wildcard if no language region has been specified.
		if !strings.ContainsRune(req.Language, '-') {
			indexPattern += "-*"
		}
	} else {
		indexPattern += "-*"
	}

	var boolQuery boolConditionsV1

	userQuery, err := protoToQuery(req.Query)
	if err != nil {
		return nil, twirp.InternalErrorf("translate query: %w", err)
	}

	boolQuery.Must = append(boolQuery.Must, userQuery)

	// Whether a query is shared primarily affects subscriptions.
	shared := req.Shared

	var readers []string

	if !auth.Claims.HasScope("doc_admin") {
		if !req.Shared {
			readers = append(readers, auth.Claims.Subject)
		}

		readers = append(readers, auth.Claims.Units...)

		boolQuery.Filter = append(
			boolQuery.Filter,
			termsQueryV1("readers", readers, 0, false))
	} else {
		// Always treat doc admin searches and subscriptions as private.
		shared = false
	}

	osReq := searchRequestV1{
		Query:       boolQueryV1(boolQuery),
		Source:      req.Source,
		Fields:      req.Fields,
		From:        req.From,
		Size:        req.Size,
		SearchAfter: req.SearchAfter,
	}

	for _, s := range req.Sort {
		order := "asc"
		if s.Desc {
			order = "desc"
		}

		osReq.Sort = append(osReq.Sort, map[string]string{
			s.Field: order,
		})
	}

	queryPayload, err := json.Marshal(osReq)
	if err != nil {
		return nil, twirp.InternalErrorf(
			"marshal opensearch query: %w", err)
	}

	res, err := client.Search(
		client.Search.WithContext(ctx),
		client.Search.WithIndex(indexPattern),
		client.Search.WithBody(bytes.NewReader(queryPayload)))
	if err != nil {
		return nil, twirp.InternalErrorf(
			"perform opensearch search request: %w", err)
	}

	defer func() {
		err := res.Body.Close()
		if err != nil {
			outErr = errors.Join(outErr, fmt.Errorf(
				"close opensearch response body: %w", err))
		}
	}()

	dec := json.NewDecoder(res.Body)

	if res.IsError() {
		var elasticErr ElasticErrorResponse

		err := dec.Decode(&elasticErr)
		if err != nil {
			return nil, errors.Join(
				fmt.Errorf("opensearch responded with: %s", res.Status()),
				fmt.Errorf("decoded error response: %w", err),
			)
		}

		return nil, twirp.InternalErrorf(
			"error response from opensearch: %s", res.Status())
	}

	var response searchResponse

	err = dec.Decode(&response)
	if err != nil {
		return nil, twirp.InternalErrorf(
			"unmarshal opensearch response: %w", err)
	}

	pRes := index.QueryResponseV1{
		Took:     response.Took,
		TimedOut: response.TimedOut,
		Shards: &index.ShardsV1{
			Total:      response.Shards.Total,
			Successful: response.Shards.Successful,
			Skipped:    response.Shards.Skipped,
			Failed:     response.Shards.Failed,
		},
		Hits: &index.HitsV1{
			Total: &index.HitsTotalV1{
				Value:    response.Hits.Total.Value,
				Relation: response.Hits.Total.Relation,
			},
			Hits: make([]*index.HitV1, len(response.Hits.Hits)),
		},
	}

	var documents map[string]*newsdoc.Document

	if req.LoadDocument && len(response.Hits.Hits) > 0 {
		documents = make(map[string]*newsdoc.Document, len(response.Hits.Hits))
		load := make([]*repository.BulkGetReference, len(response.Hits.Hits))

		for i, hit := range response.Hits.Hits {
			load[i] = &repository.BulkGetReference{
				Uuid: hit.ID,
			}
		}

		// Forward the authentication header.
		authCtx, err := twirp.WithHTTPRequestHeaders(ctx, http.Header{
			"Authorization": []string{"Bearer " + auth.Token},
		})
		if err != nil {
			return nil, twirp.InternalErrorf(
				"invalid header handling: %w", err)
		}

		bulkRes, err := s.documents.BulkGet(authCtx,
			&repository.BulkGetRequest{Documents: load})
		if err != nil {
			return nil, twirp.InternalErrorf(
				"error response from repository: %w", err)
		}

		for _, item := range bulkRes.Items {
			documents[item.Document.Uuid] = item.Document
		}
	}

	for i, hit := range response.Hits.Hits {
		ph := index.HitV1{
			Id:     hit.ID,
			Fields: make(map[string]*index.FieldValuesV1, len(hit.Fields)),
		}

		if documents != nil {
			ph.Document = documents[hit.ID]
		}

		if hit.Score != nil {
			ph.Score = *hit.Score
		}

		for field, values := range hit.Fields {
			ph.Fields[field] = &index.FieldValuesV1{
				Values: anySliceToStrings(values),
			}
		}

		if hit.Source != nil {
			ph.Source = make(
				map[string]*index.FieldValuesV1,
				len(hit.Source))

			for field, values := range hit.Source {
				ph.Source[field] = &index.FieldValuesV1{
					Values: anySliceToStrings(values),
				}
			}
		}

		ph.Sort = anySliceToStrings(hit.Sort)

		pRes.Hits.Hits[i] = &ph
	}

	if req.Subscribe {
		subID, cursor, err := s.createSubscription(
			ctx,
			auth.Claims.Subject,
			shared,
			req.DocumentType,
			boolQueryV1(boolQuery),
			postgres.SubscriptionSpec{
				Source:        req.Source,
				LoadDocuments: req.LoadDocument,
				Fields:        req.Fields,
			},
		)
		if err != nil {
			// We just log here, issues with subscriptions should
			// not bring down search in general.
			s.log.ErrorContext(ctx, "set up subscription for search request",
				elephantine.LogKeyError, err)
		} else {
			pRes.Subscription = &index.SubscriptionReference{
				Id:     subID,
				Cursor: cursor,
			}
		}
	}

	return &pRes, nil
}

// TODO: Move to a store layer?
func (s *SearchServiceV1) createSubscription(
	ctx context.Context,
	sub string,
	shared bool,
	docType string,
	query map[string]any,
	spec postgres.SubscriptionSpec,
) (_ int64, _ int64, outErr error) {
	queryJSON, err := json.Marshal(query)
	if err != nil {
		return 0, 0, fmt.Errorf("marshal query spec: %w", err)
	}

	specJSON, err := json.Marshal(spec)
	if err != nil {
		return 0, 0, fmt.Errorf("marshal subscription spec: %w", err)
	}

	percHash := sha256.Sum256(queryJSON)
	subHash := sha256.Sum256(specJSON)

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("begin transaction: %w", err)
	}

	defer pg.Rollback(tx, &outErr)

	q := postgres.New(tx)

	owner := sub
	if shared {
		owner = ""
	}

	var (
		percID    int64
		createErr error
	)

	// Might have percolator creation contention, allow for a single retry.
	for range 2 {
		percID, err = q.CheckForPercolator(ctx, postgres.CheckForPercolatorParams{
			Hash:  percHash[:],
			Owner: pg.TextOrNull(owner),
		})
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return 0, 0, fmt.Errorf("check for existing percolator: %w", err)
		}

		if percID == 0 {
			percID, err = q.CreatePercolator(ctx, postgres.CreatePercolatorParams{
				Hash:    percHash[:],
				Owner:   pg.TextOrNull(owner),
				Created: pg.Time(time.Now()),
				DocType: docType,
				Query:   query,
			})
			if err != nil {
				createErr = fmt.Errorf("register percolator: %w", err)
			}
		}

		if percID != 0 {
			break
		}
	}

	if percID == 0 && createErr != nil {
		return 0, 0, createErr
	}

	subID, err := q.CreateSubscription(ctx, postgres.CreateSubscriptionParams{
		Percolator: percID,
		Client:     sub,
		Hash:       subHash[:],
		Touched:    pg.Time(time.Now()),
		Spec:       spec,
	})
	if err != nil {
		return 0, 0, fmt.Errorf("register subscription: %w", err)
	}

	// TODO: Only publish on initial create. CurrentID == 0 might be good
	// enough.
	err = s.percChanges.Publish(ctx, tx, PercolatorUpdate{
		ID:      percID,
		DocType: docType,
	})
	if err != nil {
		return 0, 0, fmt.Errorf("publish percolator update: %w", err)
	}

	cursor, err := q.GetLastPercolatorEventID(ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("get current cursor: %w", err)
	}

	err = tx.Commit(ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("commit changes: %w", err)
	}

	return subID, cursor, nil
}

type EventPayload struct {
	Document *newsdoc.Document
	Fields   map[string][]string
}

func anySliceToStrings(s []any) []string {
	if len(s) == 0 {
		return nil
	}

	r := make([]string, len(s))

	for i, v := range s {
		switch c := v.(type) {
		case string:
			r[i] = c
		case float64:
			r[i] = strconv.FormatFloat(c, 'f', -1, 64)
		case bool:
			r[i] = strconv.FormatBool(c)
		}
	}

	return r
}

type searchResponse struct {
	Took     int64          `json:"took"`
	TimedOut bool           `json:"timed_out"`
	Shards   responseShards `json:"_shards"`
	Hits     responseHits   `json:"hits"`
}

type responseShards struct {
	Total      int32 `json:"total"`
	Successful int32 `json:"successful"`
	Skipped    int32 `json:"skipped"`
	Failed     int32 `json:"failed"`
}

type responseHits struct {
	Total    responseHitsTotal `json:"total"`
	MaxScore float32           `json:"max_score"`
	Hits     []responseHit     `json:"hits"`
}

type responseHitsTotal struct {
	Value    int64  `json:"value"`
	Relation string `json:"relation"`
}

type responseHit struct {
	Index  string           `json:"_index"`
	ID     string           `json:"_id"`
	Score  *float32         `json:"_score"`
	Source map[string][]any `json:"_source"`
	Fields map[string][]any `json:"fields"`
	Sort   []any            `json:"sort"`
}

type searchRequestV1 struct {
	Query       map[string]any      `json:"query"`
	Fields      []string            `json:"fields,omitempty"`
	Sort        []map[string]string `json:"sort,omitempty"`
	Source      bool                `json:"_source"`
	From        int64               `json:"from,omitempty"`
	Size        int64               `json:"size,omitempty"`
	SearchAfter []string            `json:"search_after,omitempty"`
}

func protoToQuery(p *index.QueryV1) (map[string]any, error) {
	v := p.GetConditions()

	switch q := v.(type) {
	case *index.QueryV1_Bool:
		must, err := protosToQueries(q.Bool.Must)
		if err != nil {
			return nil, fmt.Errorf("bool must queries: %w", err)
		}

		mustNot, err := protosToQueries(q.Bool.MustNot)
		if err != nil {
			return nil, fmt.Errorf("bool must not queries: %w", err)
		}

		should, err := protosToQueries(q.Bool.Should)
		if err != nil {
			return nil, fmt.Errorf("bool should queries: %w", err)
		}

		filter, err := protosToQueries(q.Bool.Filter)
		if err != nil {
			return nil, fmt.Errorf("bool filter queries: %w", err)
		}

		return boolQueryV1(boolConditionsV1{
			Must:    must,
			MustNot: mustNot,
			Should:  should,
			Filter:  filter,
		}), nil
	case *index.QueryV1_Range:
		return rangeQueryV1(q.Range.Field, rangeConditionsV1{
			GT:  q.Range.Gt,
			GTE: q.Range.Gte,
			LT:  q.Range.Lt,
			LTE: q.Range.Lte,
		}), nil
	case *index.QueryV1_Exists:
		return existsQueryV1(q.Exists), nil
	case *index.QueryV1_MatchAll:
		return matchAllQueryV1(), nil
	case *index.QueryV1_Term:
		return termQueryV1(
			q.Term.Field, q.Term.Value,
			q.Term.Boost, false,
		), nil
	case *index.QueryV1_Terms:
		return termsQueryV1(
			q.Terms.Field, q.Terms.Values,
			q.Terms.Boost, false,
		), nil
	case *index.QueryV1_Match:
		return matchQueryV1(
			q.Match.Field, q.Match.Value,
			q.Match.Boost, false,
		), nil
	case *index.QueryV1_MultiMatch:
		return multiMatchQueryV1(q.MultiMatch), nil
	case *index.QueryV1_MatchPhrase:
		return matchPhraseQueryV1(q.MatchPhrase.Field, q.MatchPhrase.Value), nil
	case *index.QueryV1_QueryString:
		return queryStringQueryV1(q.QueryString), nil
	case *index.QueryV1_Prefix:
		return prefixQueryV1(
			q.Prefix.Field, q.Prefix.Value,
			q.Prefix.Boost, q.Prefix.CaseInsensitive,
		), nil
	default:
		return nil, fmt.Errorf("unknown query type %T", v)
	}
}

func protosToQueries(p []*index.QueryV1) ([]map[string]any, error) {
	if len(p) == 0 {
		return nil, nil
	}

	res := make([]map[string]any, len(p))

	for i := range p {
		q, err := protoToQuery(p[i])
		if err != nil {
			return nil, fmt.Errorf("query %d: %w", i+1, err)
		}

		res[i] = q
	}

	return res, nil
}

func qWrap(query string, cond any) map[string]any {
	return map[string]any{
		query: cond,
	}
}

func boolQueryV1(cond boolConditionsV1) map[string]any {
	return qWrap("bool", cond)
}

type boolConditionsV1 struct {
	Must    []map[string]any `json:"must,omitempty"`
	MustNot []map[string]any `json:"must_not,omitempty"`
	Should  []map[string]any `json:"should,omitempty"`
	Filter  []map[string]any `json:"filter,omitempty"`
}

func rangeQueryV1(field string, cond rangeConditionsV1) map[string]any {
	return qWrap("range", map[string]rangeConditionsV1{
		field: cond,
	})
}

type rangeConditionsV1 struct {
	GT  string `json:"gt,omitempty"`
	GTE string `json:"gte,omitempty"`
	LT  string `json:"lt,omitempty"`
	LTE string `json:"lte,omitempty"`
}

func existsQueryV1(field string) map[string]any {
	return qWrap("exists", map[string]string{
		"field": field,
	})
}

func matchAllQueryV1() map[string]any {
	return qWrap("match_all", struct{}{})
}

func termQueryV1(
	field string, term string,
	boost float64, caseInsensitive bool,
) map[string]any {
	spec := map[string]string{
		"value": term,
	}

	spec = addBoostCase(spec, boost, caseInsensitive)

	return qWrap("term", map[string]any{
		field: spec,
	})
}

func termsQueryV1(
	field string, terms []string,
	boost float64, caseInsensitive bool,
) map[string]any {
	spec := map[string]any{
		field: terms,
	}

	if boost != 0 {
		spec["boost"] = boost
	}

	if caseInsensitive {
		spec["case_insensitive"] = "true"
	}

	return qWrap("terms", spec)
}

func multiMatchQueryV1(q *index.MultiMatchQueryV1) map[string]any {
	spec := map[string]any{
		"fields": q.Fields,
		"query":  q.Query,
	}

	if q.Boost != 0 {
		spec["boost"] = q.Boost
	}

	if q.Type != "" {
		spec["type"] = q.Type
	}

	if q.BooleanAnd {
		spec["operator"] = "AND"
	}

	if q.MinimumShouldMatch != "" {
		spec["minimum_should_match"] = q.MinimumShouldMatch
	}

	if q.TieBreaker != 0 {
		spec["tie_breaker"] = q.TieBreaker
	}

	return qWrap("multi_match", spec)
}

func matchQueryV1(
	field string, match string,
	boost float64, caseInsensitive bool,
) map[string]any {
	spec := map[string]string{
		"query": match,
	}

	spec = addBoostCase(spec, boost, caseInsensitive)

	return qWrap("match", map[string]any{
		field: spec,
	})
}

func matchPhraseQueryV1(field string, phrase string) map[string]any {
	return qWrap("match_phrase", map[string]string{
		field: phrase,
	})
}

func queryStringQueryV1(query string) map[string]any {
	return qWrap("query_string", map[string]string{
		"query": query,
	})
}

func prefixQueryV1(
	field string, term string,
	boost float64, caseInsensitive bool,
) map[string]any {
	spec := map[string]string{
		"value": term,
	}

	spec = addBoostCase(spec, boost, caseInsensitive)

	return qWrap("prefix", map[string]any{
		field: spec,
	})
}

func addBoostCase(
	values map[string]string, boost float64, caseInsensitive bool,
) map[string]string {
	if boost != 0 {
		values["boost"] = fmt.Sprintf("%f", boost)
	}

	if caseInsensitive {
		values["case_insensitive"] = "true"
	}

	return values
}
