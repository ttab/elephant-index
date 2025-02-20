package index

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/opensearch-project/opensearch-go/v2"
	"github.com/ttab/elephant-api/newsdoc"
	"github.com/ttab/elephant-api/repository"
	"github.com/ttab/elephant-index/postgres"
	"github.com/ttab/elephantine"
	"github.com/ttab/elephantine/pg"
	"github.com/ttab/revisor"
	"golang.org/x/sync/errgroup"
)

const (
	LogKeyIndexOperation = "index_operation"
	LogKeyResponseStatus = "response_status"
)

type PercolatorWorker interface {
	PercolateDocument(setName string, index string, document map[string][]string, doc *newsdoc.Document)
}

type ValidatorSource interface {
	GetValidator() *revisor.Validator
}

type IndexerOptions struct {
	Logger            *slog.Logger
	SetName           string
	Database          *pgxpool.Pool
	Client            *opensearch.Client
	Documents         repository.Documents
	Validator         ValidatorSource
	Metrics           *Metrics
	LanguageOptions   LanguageOptions
	EnablePercolation bool
	Percolator        PercolatorWorker
	Sharding          ShardingPolicy
}

func NewIndexer(ctx context.Context, opts IndexerOptions) (*Indexer, error) {
	idx := Indexer{
		logger:            opts.Logger,
		metrics:           opts.Metrics,
		name:              opts.SetName,
		database:          opts.Database,
		client:            opts.Client,
		documents:         opts.Documents,
		vSource:           opts.Validator,
		q:                 postgres.New(opts.Database),
		indexes:           make(map[string]*indexWorker),
		lang:              NewLanguageResolver(opts.LanguageOptions),
		sharding:          opts.Sharding,
		enablePercolation: opts.EnablePercolation,
		percolator:        opts.Percolator,
		stop:              make(chan struct{}),
		stopped:           make(chan struct{}),
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
	logger    *slog.Logger
	metrics   *Metrics
	name      string
	lang      *LanguageResolver
	sharding  ShardingPolicy
	database  *pgxpool.Pool
	documents repository.Documents
	vSource   ValidatorSource
	client    *opensearch.Client

	percolator        PercolatorWorker
	enablePercolation bool

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
	metadoc   *newsdoc.Document
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

const (
	DocumentEvent = "document"
	DeleteEvent   = "delete_document"
	ACLEvent      = "acl"
	StatusEvent   = "status"
	WorkflowEvent = "workflow"
)

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

	idx.logger.DebugContext(ctx, "got events",
		elephantine.LogKeyEventID, pos,
		"count", len(log.Items))

	changes := make(map[string]map[string]map[string]*enrichJob)

	for _, item := range log.Items {
		if item.Type == "" {
			pos = item.Id

			continue
		}

		docUUID := item.Uuid
		language, err := idx.lang.GetLanguageInfo(item.Language)
		if err != nil {
			return 0, fmt.Errorf(
				"get language information for event %d: %w",
				item.Id, err)
		}

		var oldLanguage *LanguageInfo

		if item.OldLanguage != "" {
			ol, err := idx.lang.GetLanguageInfo(item.OldLanguage)
			if err != nil {
				return 0, fmt.Errorf(
					"get old language information for event %d: %w",
					item.Id, err)
			}

			oldLanguage = &ol
		}

		// Switch to main document on meta doc updates.
		if item.MainDocument != "" {
			// Ignore meta doc events from before repo v0.10.0.
			if item.MainDocumentType == "" {
				continue
			}

			docUUID = item.MainDocument
			item.Uuid = docUUID
			item.Type = item.MainDocumentType

			// Treat deleted meta documents as document update
			// events.
			if item.Event == DeleteEvent {
				item.Event = DocumentEvent
			}
		}

		byType, ok := changes[item.Type]
		if !ok {
			byType = make(map[string]map[string]*enrichJob)
			changes[item.Type] = byType
		}

		byLang, ok := byType[language.Code]
		if !ok {
			byLang = make(map[string]*enrichJob)
			byType[language.Code] = byLang
		}

		switch item.Event {
		case DeleteEvent:
			byLang[docUUID] = &enrichJob{
				UUID:      docUUID,
				Operation: opDelete,
			}
		case DocumentEvent, ACLEvent, StatusEvent, WorkflowEvent:
			if oldLanguage != nil && language.Code != oldLanguage.Code {
				byObLang, ok := byType[oldLanguage.Code]
				if !ok {
					byObLang = make(map[string]*enrichJob)
					byType[oldLanguage.Code] = byObLang
				}

				byObLang[docUUID] = &enrichJob{
					UUID:      docUUID,
					Operation: opDelete,
				}
			}

			byLang[docUUID] = &enrichJob{
				UUID:      docUUID,
				Operation: opUpdate,
			}
		default:
			idx.metrics.unknownEvents.WithLabelValues(item.Event).Inc()
		}

		pos = item.Id
	}

	group, gCtx := errgroup.WithContext(ctx)

	for docType := range changes {
		for lang := range changes[docType] {
			// Get canonical language with defaults applied.
			language, err := idx.lang.GetLanguageInfo(lang)
			if err != nil {
				return 0, fmt.Errorf("get language information for %q: %w", lang, err)
			}

			key := fmt.Sprintf("%s-%s", docType, language.Code)

			index, ok := idx.indexes[key]
			if !ok {
				worker, err := idx.createIndexWorker(ctx, docType, language)
				if err != nil {
					return 0, err
				}

				index = worker
				idx.indexes[key] = worker
			}

			var jobs []*enrichJob

			for _, j := range changes[docType][lang] {
				j.start = time.Now()
				j.done = make(chan struct{})

				jobs = append(jobs, j)
			}

			group.Go(func() error {
				idx.logger.DebugContext(ctx, "process documents",
					"doc_type", docType,
					"language", lang,
					"count", len(jobs))

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

func (idx *Indexer) createIndexWorker(
	ctx context.Context,
	docType string,
	lang LanguageInfo,
) (*indexWorker, error) {
	conf := GetIndexConfig(lang)

	name, err := idx.ensureIndex(
		ctx, "documents", docType, conf)
	if err != nil {
		return nil, fmt.Errorf(
			"ensure index for doc type %q: %w",
			docType, err)
	}

	var percolateName string

	if idx.enablePercolation {
		n, err := idx.ensureIndex(
			ctx, "percolate", docType, conf)
		if err != nil {
			return nil, fmt.Errorf(
				"ensure percolate index for doc type %q: %w",
				docType, err)
		}

		percolateName = n
	}

	index, err := newIndexWorker(ctx, idx,
		name, percolateName, docType, conf, 8, idx.percolator)
	if err != nil {
		return nil, fmt.Errorf(
			"create index worker: %w", err)
	}

	return index, nil
}

func (idx *Indexer) ensureIndex(
	ctx context.Context, indexType string, docType string, config OpenSearchIndexConfig,
) (string, error) {
	indexTypeName := idx.getIndexName(docType)
	indexTypeRoot := idx.getQualifiedIndexName(indexType, indexTypeName)
	indexLanguageName := fmt.Sprintf("%s-%s", indexTypeName, config.NameSuffix)

	index := fmt.Sprintf("%s-%s", indexTypeRoot, config.NameSuffix)
	aliases := []string{
		indexTypeRoot,
		fmt.Sprintf("%s-%s", indexTypeRoot, config.Language),
	}

	createReq := config.Settings
	createReq.Settings.Index = idx.sharding.GetSettings(indexLanguageName)

	settingsData, err := json.Marshal(createReq)
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
			idx.client.Indices.Create.WithBody(bytes.NewReader(settingsData)),
			idx.client.Indices.Create.WithContext(ctx))
		if err != nil {
			return "", fmt.Errorf("create index %q: %w", index, err)
		}

		defer elephantine.SafeClose(idx.logger, "index create", res.Body)

		if res.StatusCode != http.StatusOK {
			return "", fmt.Errorf("server response: %s", res.String())
		}
	}

	for _, alias := range aliases {
		err = idx.ensureAlias(index, alias)
		if err != nil {
			return "", fmt.Errorf(
				"could not ensure alias %q for index %q: %w",
				alias, index, err)
		}
	}

	return index, nil
}

func (idx *Indexer) getIndexName(docType string) string {
	return nonAlphaNum.ReplaceAllString(docType, "_")
}

func (idx *Indexer) getQualifiedIndexName(indexType, name string) string {
	return fmt.Sprintf("%s-%s-%s", indexType, idx.name, name)
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
