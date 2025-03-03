package index

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/ttab/elephant-api/newsdoc"
	"github.com/ttab/elephant-api/repository"
	"github.com/ttab/elephant-index/postgres"
	"github.com/ttab/elephantine"
	"github.com/ttab/elephantine/pg"
	"github.com/twitchtv/twirp"
)

func newIndexWorker(
	ctx context.Context, idx *Indexer,
	name, percolateIndex, contentType string,
	idxConf OpenSearchIndexConfig,
	concurrency int, percolator PercolatorWorker,
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
		featureFlags:       make(map[string]bool),
		config:             idxConf,
		percolator:         percolator,
	}

	conf, err := idx.q.GetIndexConfiguration(ctx, name)
	if errors.Is(err, pgx.ErrNoRows) {
		conf.Mappings = []byte("{}")

		err := idx.q.CreateDocumentIndex(ctx, postgres.CreateDocumentIndexParams{
			Name:        name,
			SetName:     idx.name,
			ContentType: contentType,
			Mappings:    conf.Mappings,
			FeatureFlags: []string{
				// Assumes that new indexes always support the
				// current features. If that changes this will
				// have to be parameterised.
				FeatureSortable,
				FeaturePrefix,
				FeatureOnlyICU,
			},
		})
		if err != nil {
			return nil, fmt.Errorf(
				"create index entry in database: %w", err)
		}

		c, err := idx.q.GetIndexConfiguration(ctx, name)
		if err != nil {
			return nil, fmt.Errorf(
				"read created index entry: %w", err)
		}

		conf = c
	} else if err != nil {
		return nil, fmt.Errorf(
			"get current index mappings: %w", err)
	}

	for _, feature := range conf.FeatureFlags {
		iw.featureFlags[feature] = true
	}

	err = json.Unmarshal(conf.Mappings, &iw.knownMappings.Properties)
	if err != nil {
		return nil, fmt.Errorf(
			"unmarshal current index mappings: %w", err)
	}

	for range concurrency {
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
	config             OpenSearchIndexConfig
	jobQueue           chan *enrichJob

	featureFlags  map[string]bool
	knownMappings Mappings
	percolator    PercolatorWorker
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
		Heads: make(map[string]Status),
	}

	if job.Operation == opDelete {
		return &state, nil
	}

	ctx, cancel := context.WithTimeout(job.ctx, 5*time.Second)
	defer cancel()

	docRes, err := iw.idx.documents.Get(ctx,
		&repository.GetDocumentRequest{
			Uuid:         job.UUID,
			MetaDocument: repository.GetMetaDoc_META_INCLUDE,
		})
	if err != nil {
		return nil, fmt.Errorf("get document: %w", err)
	}

	job.doc = docRes.Document

	if docRes.Meta != nil {
		job.metadoc = docRes.Meta.Document
	}

	metaRes, err := iw.idx.documents.GetMeta(ctx, &repository.GetMetaRequest{
		Uuid: job.UUID,
	})
	if err != nil {
		return nil, fmt.Errorf("get document metadata: %w", err)
	}

	state.CurrentVersion = metaRes.Meta.CurrentVersion
	state.WorkflowState = metaRes.Meta.WorkflowState
	state.WorkflowCheckpoint = metaRes.Meta.WorkflowCheckpoint

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
		state.ACL = append(state.ACL, ACLEntry{
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

		status := Status{
			ID:      v.Id,
			Version: v.Version,
			Creator: v.Creator,
			Created: created,
			Meta:    v.Meta,
		}

		state.Heads[name] = status
	}

	state.Document = newsdoc.DocumentFromRPC(job.doc)

	if job.metadoc != nil {
		md := newsdoc.DocumentFromRPC(job.metadoc)

		state.MetaDocument = &md
	}

	return &state, nil
}

// Process indexes the documents in a batch, and should only return an error if
// we get an indication that indexing in ES/OS has become impossible.
func (iw *indexWorker) Process(
	ctx context.Context, documents []*enrichJob, caughtUp bool,
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
			job.Operation = opDelete
			job.err = nil
		} else if job.err != nil {
			iw.idx.metrics.enrichErrors.WithLabelValues(
				iw.contentType, iw.indexName,
			).Inc()

			iw.logger.ErrorContext(ctx,
				"failed to enrich document for indexing",
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

			continue
		}

		idxDoc, err := BuildDocument(
			iw.idx.vSource.GetValidator(), job.State,
			iw.config, iw.featureFlags,
		)
		if err != nil {
			return fmt.Errorf("build flat document: %w", err)
		}

		mappings := idxDoc.Mappings()

		changes := mappings.ChangesFrom(iw.knownMappings)

		if changes.HasNew() {
			err := iw.attemptMappingUpdate(ctx, mappings)
			if err != nil {
				return err
			}
		}

		// Count properties that would have changed type if we followed
		// the generated mapping. This will be a good metric to keep
		// track of to see if we need to re-index or adjust the revisor
		// schema.
		for p, c := range changes {
			if c.Comparison != MappingBreaking {
				continue
			}

			iw.idx.metrics.ignoredMapping.WithLabelValues(
				iw.indexName, p,
			).Add(1)
		}

		values := idxDoc.Values()

		if iw.idx.enablePercolation && caughtUp {
			iw.percolator.PercolateDocument(
				ctx,
				iw.idx.name,
				postgres.PercolatorDocument{
					ID:       job.EventID,
					Fields:   values,
					Document: &job.State.Document,
				},
			)
		}

		err = errors.Join(
			enc.Encode(bulkHeader{Index: &bulkOperation{
				Index: iw.indexName,
				ID:    job.UUID,
			}}),
			enc.Encode(values),
		)
		if err != nil {
			return fmt.Errorf("marshal document index instruction: %w", err)
		}
	}

	res, err := iw.idx.client.Bulk(&buf,
		iw.idx.client.Bulk.WithContext(ctx))
	if err != nil {
		return fmt.Errorf("submit bulk request: %w", err)
	}

	defer res.Body.Close()

	counters, err := InterpretBulkResponse(ctx, iw.logger, res.Body)
	if err != nil {
		return fmt.Errorf("interpret bulk response: %w", err)
	}

	for res, count := range counters {
		iw.idx.metrics.indexedDocument.WithLabelValues(
			iw.contentType, iw.indexName, res,
		).Add(float64(count))
	}

	return nil
}

func InterpretBulkResponse(
	ctx context.Context, log *slog.Logger, r io.Reader,
) (map[string]int, error) {
	var result bulkResponse

	dec := json.NewDecoder(r)

	err := dec.Decode(&result)
	if err != nil {
		return nil, fmt.Errorf(
			"invalid response body from server: %w", err)
	}

	counters := make(map[string]int)

	for _, item := range result.Items {
		for operation, result := range item {
			if result.Status == http.StatusOK || result.Status == http.StatusCreated {
				counters[operation]++

				continue
			}

			// Treat 404s as success for deletes.
			if operation == "delete" && result.Status == http.StatusNotFound {
				counters[operation]++

				continue
			}

			counters[operation+"_err"]++

			log.ErrorContext(ctx, "failed update document in index",
				LogKeyIndexOperation, operation,
				LogKeyResponseStatus, result.Status,
				elephantine.LogKeyDocumentUUID, result.ID,
				elephantine.LogKeyError, result.Error.String(),
			)
		}
	}

	return counters, nil
}

func (iw *indexWorker) attemptMappingUpdate(
	ctx context.Context, mappings Mappings,
) error {
	err := pg.WithTX(ctx, iw.idx.database, func(tx pgx.Tx) error {
		// Abort if another goroutine has updated the mappings and added
		// the mappings we were missing.
		changes := mappings.ChangesFrom(iw.knownMappings)
		if !changes.HasNew() {
			return nil
		}

		q := iw.idx.q.WithTx(tx)

		// Get the current index mappings with a row lock.
		conf, err := q.GetIndexConfiguration(ctx, iw.indexName)
		if err != nil {
			return fmt.Errorf(
				"get current index mappings: %w", err)
		}

		var current Mappings

		err = json.Unmarshal(conf.Mappings, &current.Properties)
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

		if iw.percolateIndexName != "" {
			percolatorMappings := Mappings{
				Properties: map[string]Mapping{
					"query": {
						FieldOptions: FieldOptions{
							Type: TypePercolator,
						},
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
				return fmt.Errorf("update percolate index mappings: %w", err)
			}
		}

		mappingData, err := json.Marshal(newMappings.Properties)
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
	Items  []bulkItem `json:"items"`
}

type bulkItem map[string]bulkResult

type bulkResult struct {
	ID     string    `json:"_id"`
	Result string    `json:"result"`
	Status int       `json:"status"`
	Error  bulkError `json:"error"`
}

type bulkError struct {
	Type     string     `json:"type"`
	Reason   string     `json:"reason"`
	CausedBy *bulkError `json:"caused_by"`
}

func (be bulkError) String() string {
	msg := be.Type + " " + be.Reason

	if be.CausedBy != nil {
		msg = msg + ": " + be.CausedBy.String()
	}

	return msg
}
