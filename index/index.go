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
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/opensearch-project/opensearch-go/v2"
	"github.com/ttab/elephant-api/newsdoc"
	"github.com/ttab/elephant-api/repository"
	"github.com/ttab/elephant-index/postgres"
	"github.com/ttab/elephantine"
	"github.com/ttab/elephantine/pg"
	"github.com/ttab/revisor"
	"github.com/twitchtv/twirp"
	"golang.org/x/sync/errgroup"
)

type ValidatorSource interface {
	GetValidator() *revisor.Validator
}

type IndexerOptions struct {
	Logger          *slog.Logger
	SetName         string
	Database        *pgxpool.Pool
	Client          *opensearch.Client
	Documents       repository.Documents
	Validator       ValidatorSource
	Metrics         *Metrics
	DefaultLanguage string
}

func NewIndexer(ctx context.Context, opts IndexerOptions) (*Indexer, error) {
	idx := Indexer{
		logger:          opts.Logger,
		metrics:         opts.Metrics,
		name:            opts.SetName,
		database:        opts.Database,
		client:          opts.Client,
		documents:       opts.Documents,
		vSource:         opts.Validator,
		q:               postgres.New(opts.Database),
		indexes:         make(map[string]*indexWorker),
		defaultLanguage: opts.DefaultLanguage,
		stop:            make(chan struct{}),
		stopped:         make(chan struct{}),
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

	return &idx, nil
}

// Indexer takes care of indexing to a named set of indexes in a cluster.
type Indexer struct {
	logger          *slog.Logger
	metrics         *Metrics
	name            string
	defaultLanguage string
	database        *pgxpool.Pool
	documents       repository.Documents
	vSource         ValidatorSource
	client          *opensearch.Client

	q *postgres.Queries

	indexes map[string]*indexWorker

	stopOnce sync.Once
	stop     chan struct{}
	stopped  chan struct{}
}

func (idx *Indexer) Stopping() <-chan struct{} {
	return idx.stop
}

func (idx *Indexer) Run(ctx context.Context) error {
	if idx.askedToStop() {
		return errors.New("indexer has been stopped")
	}

	defer func() {
		close(idx.stopped)
		idx.logger.Info("indexer has stopped")
	}()

	lock, err := pg.NewJobLock(idx.database, idx.logger,
		"indexer-"+idx.name,
		pg.JobLockOptions{})
	if err != nil {
		return fmt.Errorf("create job lock: %w", err)
	}

	// Set up a context that will be cancelled if we're asked to stop.
	lockContext, cancel := context.WithCancel(ctx)

	go func() {
		<-idx.stop
		cancel()
	}()

	err = lock.RunWithContext(lockContext, idx.indexerLoop)
	if err != nil && !errors.Is(err, context.Canceled) {
		return fmt.Errorf("run indexer loop: %w", err)
	}

	return nil
}

// Stop the indexer. Blocks until it has stopped or the timeout has been
// reached.
func (idx *Indexer) Stop(timeout time.Duration) error {
	idx.stopOnce.Do(func() {
		idx.logger.Info("stopping indexer")
		close(idx.stop)
	})

	select {
	case <-time.After(timeout):
		return errors.New("timed out")
	case <-idx.stopped:
		return nil
	}
}

// Convenience function for cases where it's easier than doing a channel select
// on c.stop.
func (idx *Indexer) askedToStop() bool {
	select {
	case <-idx.stop:
		return true
	default:
		return false
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
	doc       *newsdoc.Document
}

func (ij *enrichJob) Finish(state *DocumentState, err error) {
	ij.State = state
	ij.err = err
	close(ij.done)
}

func (idx *Indexer) indexerLoop(ctx context.Context) error {
	pos, err := idx.q.GetIndexSetPosition(ctx, idx.name)
	if err != nil {
		return fmt.Errorf("fetching current index set position: %w", err)
	}

	idx.logger.DebugContext(ctx, "starting from",
		elephantine.LogKeyEventID, pos)

	for {
		if idx.askedToStop() {
			return nil
		}

		newPos, err := idx.loopIteration(ctx, pos)
		if err != nil {
			idx.metrics.indexerFailures.WithLabelValues(idx.name).Inc()

			idx.logger.ErrorContext(ctx, "indexer failure",
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

			idx.metrics.logPos.WithLabelValues(idx.name).Set(float64(pos))
		} else {
			time.Sleep(1 * time.Second)
		}

		select {
		case <-ctx.Done():
			return nil
		default:
		}
	}
}

func (idx *Indexer) loopIteration(
	ctx context.Context, pos int64,
) (int64, error) {
	log, err := idx.documents.CompactedEventlog(ctx,
		&repository.GetCompactedEventlogRequest{
			After: pos,
			Limit: 500,
		})
	if err != nil {
		return 0, fmt.Errorf("get eventlog entries: %w", err)
	}

	changes := make(map[string]map[string]map[string]*enrichJob)

	for _, item := range log.Items {
		if item.Type == "" {
			pos = item.Id

			continue
		}

		byType, ok := changes[item.Type]
		if !ok {
			byType = make(map[string]map[string]*enrichJob)
			changes[item.Type] = byType
		}

		language := idx.defaultLanguage

		docRes, err := idx.documents.Get(ctx,
			&repository.GetDocumentRequest{
				Uuid:    item.Uuid,
				Version: item.Version,
			})
		if elephantine.IsTwirpErrorCode(err, twirp.NotFound) {
			// TODO: we need to blanket delete the ID in all indexes
			// here. Resorting to doing a delete for default
			// language at the moment.
			item.Event = "delete_document"
		} else if err != nil {
			return 0, fmt.Errorf("get document: %w", err)
		}

		var doc *newsdoc.Document

		if docRes != nil {
			doc = docRes.Document
			// Normalize to lowercase
			language = strings.ToLower(doc.Language)
		}

		byLang, ok := byType[language]
		if !ok {
			byLang = make(map[string]*enrichJob)
			byType[language] = byLang
		}

		switch item.Event {
		case "delete_document":
			byLang[item.Uuid] = &enrichJob{
				UUID:      item.Uuid,
				Operation: opDelete,
				doc:       doc,
			}
		case "document", "acl", "status":
			// Find all existing documents with the same Id but a
			// different language
			obDocs, err := idx.findObsoleteDocuments(ctx, item, language)
			if err != nil {
				return 0, fmt.Errorf("find obsolete docs: %w", err)
			}

			for _, obDoc := range obDocs {
				for _, obLang := range obDoc.DocumentLanguage {
					byObLang, ok := byType[obLang]
					if !ok {
						byObLang = make(map[string]*enrichJob)
						byType[obLang] = byObLang
					}

					byObLang[item.Uuid] = &enrichJob{
						UUID:      item.Uuid,
						Operation: opDelete,
					}
				}
			}

			byLang[item.Uuid] = &enrichJob{
				UUID:      item.Uuid,
				Operation: opUpdate,
				doc:       doc,
			}
		default:
			idx.metrics.unknownEvents.WithLabelValues(item.Event).Inc()
		}

		pos = item.Id
	}

	group, gCtx := errgroup.WithContext(ctx)

	for docType := range changes {
		for lang := range changes[docType] {
			key := fmt.Sprintf("%s-%s", docType, lang)
			index, ok := idx.indexes[key]

			if !ok {
				name, err := idx.ensureIndex(
					ctx, "documents", docType, lang)
				if err != nil {
					return 0, fmt.Errorf(
						"ensure index for doc type %q: %w",
						docType, err)
				}

				percolateName, err := idx.ensureIndex(
					ctx, "percolate", docType, lang)
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

				idx.indexes[key] = index
			}

			var jobs []*enrichJob

			for _, j := range changes[docType][lang] {
				j.start = time.Now()
				j.done = make(chan struct{})

				jobs = append(jobs, j)
			}

			group.Go(func() error {
				return index.Process(gCtx, jobs)
			})
		}
	}

	err = group.Wait()
	if err != nil {
		return 0, fmt.Errorf("index all types: %w", err)
	}

	return pos, nil
}

func (idx *Indexer) findObsoleteDocuments(
	ctx context.Context,
	item *repository.EventlogItem,
	language string,
) ([]DocumentSource, error) {
	query, err := json.Marshal(createLanguageQuery(item.Uuid, language))
	if err != nil {
		return nil, fmt.Errorf("marshal json: %w", err)
	}

	rootAlias := idx.getIndexTypeRoot(item.Type, "documents")

	obDocsRes, err := idx.client.Search(
		idx.client.Search.WithIndex(rootAlias),
		idx.client.Search.WithBody(bytes.NewReader(query)),
		idx.client.Search.WithContext(ctx),
	)
	if err != nil {
		return nil, fmt.Errorf("lookup document: %w", err)
	}

	defer elephantine.SafeClose(idx.logger, "index exists", obDocsRes.Body)

	obDocsBody, err := io.ReadAll(obDocsRes.Body)
	if err != nil {
		return nil, fmt.Errorf("read response body: %w", err)
	}

	var obDocs SearchResponseBody

	err = json.Unmarshal(obDocsBody, &obDocs)
	if err != nil {
		return nil, fmt.Errorf("unmarshal existing document result: %w", err)
	}

	obsolete := make([]DocumentSource, len(obDocs.Hits.Hits))
	for i, doc := range obDocs.Hits.Hits {
		obsolete[i] = doc.Source
	}

	return obsolete, nil
}

func createLanguageQuery(uuid string, language string) ElasticSearchRequest {
	return ElasticSearchRequest{
		Query: ElasticQuery{
			Bool: &BooleanQuery{
				Filter: []ElasticQuery{
					{Ids: &IdsQuery{
						Values: []string{uuid},
					}},
				},
				MustNot: []ElasticQuery{
					{
						Term: map[string]string{
							"document.language": language,
						},
					},
				},
			},
		},
	}
}

func (idx *Indexer) ensureIndex(
	ctx context.Context, indexType string, docType string, lang string,
) (string, error) {
	config, err := GetLanguageConfig(lang, idx.defaultLanguage)
	if err != nil {
		return "", fmt.Errorf("could not get language config: %w", err)
	}

	indexTypeRoot := idx.getIndexTypeRoot(docType, indexType)

	index := fmt.Sprintf("%s-%s", indexTypeRoot, config.NameSuffix)
	aliases := []string{
		indexTypeRoot,
		fmt.Sprintf("%s-%s", indexTypeRoot, config.Language),
	}

	settings, err := json.Marshal(config.Settings)
	if err != nil {
		return "", fmt.Errorf("could not marshal index settings: %w", err)
	}

	existRes, err := idx.client.Indices.Exists([]string{index},
		idx.client.Indices.Exists.WithContext(ctx))
	if err != nil {
		return "", fmt.Errorf("check if index exists: %w", err)
	}

	defer elephantine.SafeClose(idx.logger, "index exists", existRes.Body)

	if existRes.StatusCode != http.StatusOK {
		res, err := idx.client.Indices.Create(index,
			idx.client.Indices.Create.WithBody(bytes.NewReader(settings)),
			idx.client.Indices.Create.WithContext(ctx))
		if err != nil {
			return "", fmt.Errorf("create index %q: %w", index, err)
		}

		defer elephantine.SafeClose(idx.logger, "index create", res.Body)

		if res.StatusCode != http.StatusOK {
			return "", fmt.Errorf("server response: %s", res.Status())
		}
	}

	for _, alias := range aliases {
		err = idx.ensureAlias(index, alias)
		if err != nil {
			return "", fmt.Errorf("could not ensure alias: %w", err)
		}
	}

	return index, nil
}

func (idx *Indexer) getIndexTypeRoot(docType string, indexType string) string {
	safeDocType := nonAlphaNum.ReplaceAllString(docType, "_")

	return fmt.Sprintf("%s-%s-%s", indexType, idx.name, safeDocType)
}

func (idx *Indexer) ensureAlias(index string, alias string) error {
	res, err := idx.client.Indices.PutAlias([]string{index}, alias)
	if err != nil {
		return fmt.Errorf("could not create alias %s for index %s: %w", alias, index, err)
	}

	defer elephantine.SafeClose(idx.logger, "put alias", res.Body)

	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("put alias status code: %s", res.Status())
	}

	return nil
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

		err := idx.q.CreateDocumentIndex(ctx, postgres.CreateDocumentIndexParams{
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
		Heads: make(map[string]Status),
	}

	if job.Operation == opDelete {
		return &state, nil
	}

	ctx, cancel := context.WithTimeout(job.ctx, 5*time.Second)
	defer cancel()

	metaRes, err := iw.idx.documents.GetMeta(ctx, &repository.GetMetaRequest{
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

	d := newsdoc.DocumentFromRPC(job.doc)

	state.Document = d

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
			iw.logger.DebugContext(ctx,
				"the document has been deleted, removing from index",
				elephantine.LogKeyDocumentUUID, job.UUID,
				elephantine.LogKeyError, job.err)

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

		// Count properties that would have changed type if we followed
		// the generated mapping. This will be a good metric to keep
		// track of to see if we need to re-index or adjust the revisor
		// schema.
		for p, c := range changes {
			if c.New {
				continue
			}

			iw.idx.metrics.ignoredMapping.WithLabelValues(
				iw.indexName, p,
			).Add(1)

		}

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

			iw.logger.ErrorContext(ctx, "failed to index document",
				elephantine.LogKeyDocumentUUID, item.Index.ID,
				elephantine.LogKeyError, item.Index.Error.String(),
			)
		case item.Delete != nil:
			if item.Delete.Error == nil {
				break
			}

			counters["delete_err"]++

			iw.logger.ErrorContext(ctx, "failed to delete document from index",
				elephantine.LogKeyDocumentUUID, item.Index.ID,
				elephantine.LogKeyError, item.Index.Error.String(),
			)
		}
	}

	for res, count := range counters {
		iw.idx.metrics.indexedDocument.WithLabelValues(
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
