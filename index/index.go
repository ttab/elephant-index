// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package index

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/opensearch-project/opensearch-go/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/ttab/elephant-index/postgres"
	"github.com/ttab/elephant/repository"
	"github.com/ttab/elephant/revisor"
	rpc "github.com/ttab/elephant/rpc/repository"
	"github.com/ttab/elephantine"
	"github.com/ttab/elephantine/pg"
	"github.com/twitchtv/twirp"
	"golang.org/x/exp/slog"
	"golang.org/x/sync/errgroup"
)

type ValidatorSource interface {
	GetValidator() *revisor.Validator
}

type IndexerOptions struct {
	Logger            *slog.Logger
	SetName           string
	Database          *pgxpool.Pool
	Client            *opensearch.Client
	Documents         rpc.Documents
	Validator         ValidatorSource
	MetricsRegisterer prometheus.Registerer
}

func NewIndexer(ctx context.Context, opts IndexerOptions) (*Indexer, error) {
	if opts.MetricsRegisterer == nil {
		opts.MetricsRegisterer = prometheus.DefaultRegisterer
	}

	// TODO: When we get to running multiple indexers (reindexing) we will
	// need to create these metrics upstack and pass them to the indexers
	// instead. Deferring.

	indexerFailures := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "elephant_indexer_failures_total",
			Help: "Counts the number of times the indexer failed process a batch of events.",
		},
		[]string{"name"},
	)
	if err := opts.MetricsRegisterer.Register(indexerFailures); err != nil {
		return nil, fmt.Errorf("failed to register metric: %w", err)
	}

	unknownEvents := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "elephant_indexer_unknown_events_total",
			Help: "Unknown event types from the elephant event log.",
		},
		[]string{"type"},
	)
	if err := opts.MetricsRegisterer.Register(unknownEvents); err != nil {
		return nil, fmt.Errorf("failed to register metric: %w", err)
	}

	mappingUpdate := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "elephant_indexer_mapping_update_total",
			Help: "Number of times the index mapping has been updated.",
		},
		[]string{"index"},
	)
	if err := opts.MetricsRegisterer.Register(mappingUpdate); err != nil {
		return nil, fmt.Errorf("failed to register metric: %w", err)
	}

	indexedDocument := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "elephant_indexer_doc_total",
			Help: "Document indexing counter, tracks index and delete successes and failures.",
		},
		[]string{"type", "index", "result"},
	)
	if err := opts.MetricsRegisterer.Register(indexedDocument); err != nil {
		return nil, fmt.Errorf("failed to register metric: %w", err)
	}

	enrichErrors := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "elephant_indexer_enrich_errors_total",
			Help: "Document enricher errors.",
		},
		[]string{"type", "index"},
	)
	if err := opts.MetricsRegisterer.Register(enrichErrors); err != nil {
		return nil, fmt.Errorf("failed to register metric: %w", err)
	}

	logPos := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "elephant_indexer_log_position",
			Help: "Indexer eventlog position.",
		},
		[]string{"name"},
	)
	if err := opts.MetricsRegisterer.Register(logPos); err != nil {
		return nil, fmt.Errorf("failed to register metric: %w", err)
	}

	idx := &Indexer{
		logger:          opts.Logger,
		name:            opts.SetName,
		database:        opts.Database,
		client:          opts.Client,
		documents:       opts.Documents,
		vSource:         opts.Validator,
		q:               postgres.New(opts.Database),
		indexes:         make(map[string]*indexWorker),
		indexerFailures: indexerFailures,
		unknownEvents:   unknownEvents,
		logPos:          logPos,
		mappingUpdate:   mappingUpdate,
		indexedDocument: indexedDocument,
		enrichErrors:    enrichErrors,
	}

	// We speculatively try to create the index set here, but don't treat a
	// primary key constraint error as an actual error. Beats faffing around
	// with extra roundtrips for checks.
	err := idx.q.CreateIndexSet(ctx, postgres.CreateIndexSetParams{
		Name:     idx.name,
		Position: 0,
	})
	if err != nil && !pg.IsConstraintError(err, "index_set_pkey") {
		return nil, fmt.Errorf("failed to create index set: %w", err)
	}

	return idx, nil
}

type Indexer struct {
	logger    *slog.Logger
	name      string
	database  *pgxpool.Pool
	documents rpc.Documents
	vSource   ValidatorSource
	client    *opensearch.Client

	indexerFailures *prometheus.CounterVec
	unknownEvents   *prometheus.CounterVec
	mappingUpdate   *prometheus.CounterVec
	indexedDocument *prometheus.CounterVec
	enrichErrors    *prometheus.CounterVec
	logPos          *prometheus.GaugeVec

	q *postgres.Queries

	indexes map[string]*indexWorker
}

func (idx *Indexer) Run(ctx context.Context) error {
	pos, err := idx.q.GetIndexSetPosition(ctx, idx.name)
	if err != nil {
		return fmt.Errorf("fetching current index set position: %w", err)
	}

	idx.logger.DebugCtx(ctx, "starting from",
		elephantine.LogKeyEventID, pos)

	for {
		newPos, err := idx.loopIteration(ctx, pos)
		if err != nil {
			idx.indexerFailures.WithLabelValues(idx.name).Inc()

			idx.logger.ErrorCtx(ctx, "indexer failure",
				elephantine.LogKeyError, err,
				elephantine.LogKeyEventID, pos)

			select {
			case <-time.After(5 * time.Second):
			case <-ctx.Done():
				return nil
			}

			continue
		}

		if newPos != pos {
			err = idx.q.UpdateSetPosition(ctx, postgres.UpdateSetPositionParams{
				Name:     idx.name,
				Position: newPos,
			})
			if err != nil {
				return fmt.Errorf("update the position to %d: %w", newPos, err)
			}

			pos = newPos

			idx.logPos.WithLabelValues(idx.name).Set(float64(pos))
		}

		select {
		case <-ctx.Done():
			return nil
		default:
		}
	}
}

const (
	opUpdate = iota
	opDelete
)

type enrichJob struct {
	ctx   context.Context
	start time.Time
	done  chan struct{}
	err   error

	UUID      string
	Operation int
	State     *DocumentState
}

func (ij *enrichJob) Finish(state *DocumentState, err error) {
	ij.State = state
	ij.err = err
	close(ij.done)
}

func (idx *Indexer) loopIteration(
	ctx context.Context, pos int64,
) (int64, error) {
	log, err := idx.documents.Eventlog(ctx, &rpc.GetEventlogRequest{
		After:       pos,
		WaitMs:      10000,
		BatchWaitMs: 200,
		BatchSize:   100,
	})
	if err != nil {
		return 0, fmt.Errorf("get eventlog entries: %w", err)
	}

	changes := make(map[string]map[string]*enrichJob)

	for _, item := range log.Items {
		if item.Type == "" {
			continue
		}

		byType, ok := changes[item.Type]
		if !ok {
			byType = make(map[string]*enrichJob)
			changes[item.Type] = byType
		}

		switch item.Event {
		case "delete_document":
			byType[item.Uuid] = &enrichJob{
				UUID:      item.Uuid,
				Operation: opDelete,
			}
		case "document", "acl", "status":
			byType[item.Uuid] = &enrichJob{
				UUID:      item.Uuid,
				Operation: opUpdate,
			}
		default:
			idx.unknownEvents.WithLabelValues(item.Event).Inc()
		}

		pos = item.Id
	}

	group, gCtx := errgroup.WithContext(ctx)

	for docType := range changes {
		index, ok := idx.indexes[docType]
		if !ok {
			name, err := idx.ensureIndex(
				ctx, "documents", docType)
			if err != nil {
				return 0, fmt.Errorf(
					"ensure index for doc type %q: %w",
					docType, err)
			}

			percolateName, err := idx.ensureIndex(
				ctx, "percolate", docType)
			if err != nil {
				return 0, fmt.Errorf(
					"ensure index for doc type %q: %w",
					docType, err)
			}

			index, err = newIndexWorker(ctx, idx,
				name, percolateName, docType, 8)
			if err != nil {
				return 0, fmt.Errorf(
					"create index worker: %w", err)
			}

			idx.indexes[docType] = index
		}

		var jobs []*enrichJob

		for _, j := range changes[docType] {
			j.start = time.Now()
			j.done = make(chan struct{})

			jobs = append(jobs, j)
		}

		group.Go(func() error {
			return index.Process(gCtx, jobs)
		})
	}

	err = group.Wait()
	if err != nil {
		return 0, fmt.Errorf("index all types: %w", err)
	}

	return pos, nil
}

func (idx *Indexer) ensureIndex(
	ctx context.Context, indexType string, docType string,
) (string, error) {
	name := fmt.Sprintf("%s-%s-%s",
		indexType, idx.name, nonAlphaNum.ReplaceAllString(docType, "_"))

	existRes, err := idx.client.Indices.Exists([]string{name},
		idx.client.Indices.Exists.WithContext(ctx))
	if err != nil {
		return "", fmt.Errorf("check if index exists: %w", err)
	}

	if existRes.StatusCode == http.StatusOK {
		return name, nil
	}

	res, err := idx.client.Indices.Create(name,
		idx.client.Indices.Create.WithContext(ctx))
	if err != nil {
		return "", fmt.Errorf("create index %q: %w", name, err)
	}

	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return "", fmt.Errorf("server response: %s", res.Status())
	}

	return name, nil
}

func newIndexWorker(
	ctx context.Context, idx *Indexer,
	name, percolateIndex, contentType string,
	concurrency int,
) (*indexWorker, error) {
	iw := indexWorker{
		idx: idx,
		logger: idx.logger.With(
			elephantine.LogKeyIndex, name,
		),
		contentType:        contentType,
		indexName:          name,
		percolateIndexName: percolateIndex,
		knownMappings:      NewMappings(),
		jobQueue:           make(chan *enrichJob, concurrency),
	}

	mappingData, err := idx.q.GetIndexMappings(ctx, name)
	if errors.Is(err, pgx.ErrNoRows) {
		mappingData = []byte("{}")

		err := idx.q.CreateIndex(ctx, postgres.CreateIndexParams{
			Name:        name,
			SetName:     idx.name,
			ContentType: contentType,
			Mappings:    mappingData,
		})
		if err != nil {
			return nil, fmt.Errorf(
				"create index entry in database: %w", err)
		}
	} else if err != nil {
		return nil, fmt.Errorf(
			"get current index mappings: %w", err)
	}

	err = json.Unmarshal(mappingData, &iw.knownMappings.Properties)
	if err != nil {
		return nil, fmt.Errorf(
			"unmarshal current index mappings: %w", err)
	}

	for i := 0; i < concurrency; i++ {
		go iw.loop(ctx)
	}

	return &iw, nil
}

type indexWorker struct {
	idx                *Indexer
	logger             *slog.Logger
	contentType        string
	indexName          string
	percolateIndexName string
	jobQueue           chan *enrichJob

	knownMappings Mappings
}

func (iw *indexWorker) loop(ctx context.Context) {
	for {
		var job *enrichJob

		select {
		case job = <-iw.jobQueue:
			state, err := iw.enrich(job)

			job.Finish(state, err)
		case <-ctx.Done():
			return
		}
	}
}

func (iw *indexWorker) enrich(
	job *enrichJob,
) (*DocumentState, error) {
	state := DocumentState{
		Heads: make(map[string]repository.Status),
	}

	ctx, cancel := context.WithTimeout(job.ctx, 5*time.Second)
	defer cancel()

	metaRes, err := iw.idx.documents.GetMeta(ctx, &rpc.GetMetaRequest{
		Uuid: job.UUID,
	})
	if err != nil {
		return nil, fmt.Errorf("get document metadata: %w", err)
	}

	state.CurrentVersion = metaRes.Meta.CurrentVersion

	created, err := time.Parse(time.RFC3339, metaRes.Meta.Created)
	if err != nil {
		return nil, fmt.Errorf("parse document created time: %w",
			err)
	}

	modified, err := time.Parse(time.RFC3339, metaRes.Meta.Modified)
	if err != nil {
		return nil, fmt.Errorf("parse document created time: %w",
			err)
	}

	state.Created = created
	state.Modified = modified

	for _, v := range metaRes.Meta.Acl {
		state.ACL = append(state.ACL, repository.ACLEntry{
			URI:         v.Uri,
			Permissions: v.Permissions,
		})
	}

	for name, v := range metaRes.Meta.Heads {
		created, err := time.Parse(time.RFC3339, v.Created)
		if err != nil {
			return nil, fmt.Errorf("parse %q status created time: %w",
				name, err)
		}

		status := repository.Status{
			ID:      v.Id,
			Version: v.Version,
			Creator: v.Creator,
			Created: created,
			Meta:    v.Meta,
		}

		state.Heads[name] = status
	}

	docRes, err := iw.idx.documents.Get(ctx, &rpc.GetDocumentRequest{
		Uuid:    job.UUID,
		Version: state.CurrentVersion,
	})
	if err != nil {
		return nil, fmt.Errorf("get document: %w", err)
	}

	d := repository.RPCToDocument(docRes.Document)

	state.Document = *d

	return &state, nil
}

// Process indexes the documents in a batch, and should only return an error if
// we get an indication that indexing in ES/OS has become impossible.
func (iw *indexWorker) Process(
	ctx context.Context, documents []*enrichJob,
) error {
	go func() {
		for _, job := range documents {
			if job.Operation != opUpdate {
				job.Finish(nil, nil)

				continue
			}

			job.ctx = ctx

			iw.jobQueue <- job
		}
	}()

	counters := make(map[string]int)

	var buf bytes.Buffer

	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)

	for _, job := range documents {
		select {
		case <-ctx.Done():
			return ctx.Err() //nolint:wrapcheck
		case <-job.done:
		}

		var twErr twirp.Error
		if errors.As(job.err, &twErr) && twErr.Code() == twirp.NotFound {
			iw.logger.DebugCtx(ctx, "the document has been deleted, removing from index",
				elephantine.LogKeyDocumentUUID, job.UUID,
				elephantine.LogKeyError, job.err)

			job.Operation = opDelete
			job.err = nil
		} else if job.err != nil {
			iw.idx.enrichErrors.WithLabelValues(
				iw.contentType, iw.indexName,
			).Inc()

			iw.logger.ErrorCtx(ctx, "failed to enrich document for indexing",
				elephantine.LogKeyDocumentUUID, job.UUID,
				elephantine.LogKeyError, job.err)

			continue
		}

		if job.Operation == opDelete {
			err := enc.Encode(bulkHeader{Delete: &bulkOperation{
				Index: iw.indexName,
				ID:    job.UUID,
			}})
			if err != nil {
				return fmt.Errorf("marshal delete header: %w", err)
			}

			counters["deleted"]++

			continue
		}

		idxDoc := BuildDocument(
			iw.idx.vSource.GetValidator(), job.State,
		)

		mappings := idxDoc.Mappings()

		changes := mappings.ChangesFrom(iw.knownMappings)

		if changes.HasNew() {
			err := iw.attemptMappingUpdate(ctx, mappings)
			if err != nil {
				return err
			}
		}

		// TODO: metric for non-new mapping changes

		err := errors.Join(
			enc.Encode(bulkHeader{Index: &bulkOperation{
				Index: iw.indexName,
				ID:    job.UUID,
			}}),
			enc.Encode(idxDoc.Values()),
		)
		if err != nil {
			return fmt.Errorf("marshal document index instruction: %w", err)
		}

		counters["indexed"]++
	}

	res, err := iw.idx.client.Bulk(&buf,
		iw.idx.client.Bulk.WithContext(ctx))
	if err != nil {
		return fmt.Errorf("submit bulk request: %w", err)
	}

	defer res.Body.Close()

	dec := json.NewDecoder(res.Body)

	var result bulkResponse

	err = dec.Decode(&result)
	if err != nil {
		return fmt.Errorf("invalid response body from server: %w", err)
	}

	for _, item := range result.Items {
		switch {
		case item.Index != nil:
			counters["index_err"]++

			iw.logger.ErrorCtx(ctx, "failed to index document",
				elephantine.LogKeyDocumentUUID, item.Index.ID,
				elephantine.LogKeyError, item.Index.Error.String(),
			)
		case item.Delete != nil:
			if item.Delete.Error == nil {
				break
			}

			counters["delete_err"]++

			iw.logger.ErrorCtx(ctx, "failed to delete document from index",
				elephantine.LogKeyDocumentUUID, item.Index.ID,
				elephantine.LogKeyError, item.Index.Error.String(),
			)
		}
	}

	for res, count := range counters {
		iw.idx.indexedDocument.WithLabelValues(
			iw.contentType, iw.indexName, res,
		).Add(float64(count))
	}

	return nil
}

func (iw *indexWorker) attemptMappingUpdate(
	ctx context.Context, mappings Mappings,
) error {
	err := pg.WithTX(ctx, iw.logger, iw.idx.database, "mapping update", func(tx pgx.Tx) error {
		// Abort if another goroutine has updated the mappings and added
		// the mappings we were missing.
		changes := mappings.ChangesFrom(iw.knownMappings)
		if !changes.HasNew() {
			return nil
		}

		q := iw.idx.q.WithTx(tx)

		// Get the current index mappings with a row lock.
		mappingData, err := q.GetIndexMappings(ctx, iw.indexName)
		if err != nil {
			return fmt.Errorf(
				"get current index mappings: %w", err)
		}

		var current Mappings

		err = json.Unmarshal(mappingData, &current.Properties)
		if err != nil {
			return fmt.Errorf(
				"unmarshal current index mappings: %w", err)
		}

		changes = mappings.ChangesFrom(current)

		// If our mappings were different because they were outdated,
		// just update them and return.
		if !changes.HasNew() {
			iw.knownMappings = current

			return nil
		}

		newMappings := changes.Superset(iw.knownMappings)

		err = iw.updateIndexMapping(ctx, iw.indexName, newMappings)
		if err != nil {
			return fmt.Errorf("update document index mappings: %w", err)
		}

		percolatorMappings := Mappings{
			Properties: map[string]Mapping{
				"query": {
					Type: TypePercolator,
				},
			},
		}

		for k, v := range newMappings.Properties {
			if k == "query" {
				continue
			}

			percolatorMappings.Properties[k] = v
		}

		err = iw.updateIndexMapping(ctx, iw.percolateIndexName, percolatorMappings)
		if err != nil {
			return fmt.Errorf("update document index mappings: %w", err)
		}

		mappingData, err = json.Marshal(newMappings.Properties)
		if err != nil {
			return fmt.Errorf("marshal mappings: %w", err)
		}

		err = q.UpdateIndexMappings(ctx, postgres.UpdateIndexMappingsParams{
			Name:     iw.indexName,
			Mappings: mappingData,
		})
		if err != nil {
			return fmt.Errorf("persist mappings in db: %w", err)
		}

		iw.knownMappings = newMappings

		return nil
	})
	if err != nil {
		return fmt.Errorf("update mappings in transaction: %w", err)
	}

	return nil
}

func (iw *indexWorker) updateIndexMapping(
	ctx context.Context, index string, newMappings Mappings,
) error {
	mappingData, err := json.Marshal(newMappings)
	if err != nil {
		return fmt.Errorf("marshal mappings: %w", err)
	}

	res, err := iw.idx.client.Indices.PutMapping(
		bytes.NewReader(mappingData),
		iw.idx.client.Indices.PutMapping.WithContext(ctx),
		iw.idx.client.Indices.PutMapping.WithIndex(index),
	)
	if err != nil {
		return fmt.Errorf("mapping update request: %w", err)
	}

	err = res.Body.Close()
	if err != nil {
		return fmt.Errorf("close mapping response body: %w", err)
	}

	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("error response to mapping update: %s", res.Status())
	}

	return nil
}

type bulkHeader struct {
	Delete *bulkOperation `json:"delete,omitempty"`
	Index  *bulkOperation `json:"index,omitempty"`
}

type bulkOperation struct {
	Index string `json:"_index"`
	ID    string `json:"_id"`
}

type bulkResponse struct {
	Errors bool       `json:"errors"`
	Items  []bulkItem `json:"bulkItem"`
}

type bulkItem struct {
	Delete *bulkResult `json:"delete"`
	Index  *bulkResult `json:"index"`
}

type bulkResult struct {
	ID     string     `json:"_id"`
	Result string     `json:"result"`
	Status int        `json:"status"`
	Error  *bulkError `json:"error"`
}

type bulkError struct {
	Type   string `json:"type"`
	Reason string `json:"reason"`
}

func (be bulkError) String() string {
	return be.Type + ": " + be.Reason
}
