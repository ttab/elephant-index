package index

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/opensearch-project/opensearch-go/v2"
	"github.com/ttab/elephant-api/newsdoc"
	"github.com/ttab/elephant-api/repository"
	"github.com/ttab/elephant-index/internal"
	"github.com/ttab/elephant-index/postgres"
	"github.com/ttab/elephantine"
	"github.com/ttab/elephantine/pg"
	"github.com/ttab/koonkie"
	"github.com/ttab/revisor"
	"golang.org/x/sync/errgroup"
)

const (
	LogKeyIndexOperation = "index_operation"
	LogKeyResponseStatus = "response_status"
)

type PercolatorWorker interface {
	RequestDocumentPercolation(
		ctx context.Context,
		setName string,
		documents []postgres.PercolatorDocument,
	)
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
	Language          *LanguageResolver
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
		lang:              opts.Language,
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

	EventID int64
	// SideEffect enrich jobs are generated as side-effects of an event and
	// dont represent the event as such. A language change would have a
	// primary job that creates the new document for a language, and a side
	// effect that deletes the old language document.
	SideEffect bool
	UUID       string
	Operation  int
	State      *DocumentState
	doc        *newsdoc.Document
	metadoc    *newsdoc.Document
	fields     map[string][]string
}

func (ij *enrichJob) Finish(state *DocumentState, err error) {
	ij.State = state
	ij.err = err
	close(ij.done)
}

func (idx *Indexer) indexerLoop(ctx context.Context) error {
	state, err := idx.q.GetIndexSetPosition(ctx, idx.name)
	if err != nil {
		return fmt.Errorf("fetching current index set position: %w", err)
	}

	pos := state.Position
	posMetric := idx.metrics.logPos.WithName(idx.name)

	follower := koonkie.NewLogFollower(idx.documents, koonkie.FollowerOptions{
		StartAfter:   state.Position,
		CaughtUp:     state.CaughtUp,
		Metrics:      posMetric,
		WaitDuration: 10 * time.Second,
	})

	idx.logger.DebugContext(ctx, "starting from",
		elephantine.LogKeyEventID, pos)

	for {
		if idx.askedToStop() {
			return nil
		}

		startPos, caughtUp := follower.GetState()

		err := idx.loopIteration(ctx, follower)
		if err != nil {
			idx.metrics.indexerFailures.WithLabelValues(idx.name).Inc()

			idx.logger.ErrorContext(ctx, "indexer failure",
				elephantine.LogKeyError, err,
				elephantine.LogKeyEventID, startPos)

			select {
			case <-time.After(5 * time.Second):
			case <-ctx.Done():
				return nil
			}

			// Rewind follower state.
			follower.SetState(startPos, caughtUp)

			continue
		}

		newPos, caughtUp := follower.GetState()

		if newPos != startPos {
			err = idx.q.UpdateSetPosition(ctx, postgres.UpdateSetPositionParams{
				Name:     idx.name,
				Position: newPos,
				CaughtUp: caughtUp,
			})
			if err != nil {
				return fmt.Errorf("update the position to %d: %w", newPos, err)
			}
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
	ctx context.Context, follower *koonkie.LogFollower,
) error {
	items, err := follower.GetNext(ctx)
	if err != nil {
		return fmt.Errorf("get eventlog entries: %w", err)
	}

	_, caughtUp := follower.GetState()

	changes := make(map[string]map[string]map[string]*enrichJob)

	for _, item := range items {
		if item.Type == "" {
			continue
		}

		docUUID := item.Uuid

		language, err := idx.lang.GetLanguageInfo(item.Language)
		if err != nil {
			return fmt.Errorf(
				"get language information for event %d: %w",
				item.Id, err)
		}

		var oldLanguage *LanguageInfo

		if item.OldLanguage != "" {
			ol, err := idx.lang.GetLanguageInfo(item.OldLanguage)
			if err != nil {
				return fmt.Errorf(
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
				EventID:   item.Id,
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
					EventID:    item.Id,
					SideEffect: true,
					UUID:       docUUID,
					Operation:  opDelete,
				}
			}

			byLang[docUUID] = &enrichJob{
				EventID:   item.Id,
				UUID:      docUUID,
				Operation: opUpdate,
			}
		default:
			idx.metrics.unknownEvents.WithLabelValues(item.Event).Inc()
		}
	}

	group, gCtx := errgroup.WithContext(ctx)

	for docType := range changes {
		for lang := range changes[docType] {
			// Get canonical language with defaults applied.
			language, err := idx.lang.GetLanguageInfo(lang)
			if err != nil {
				return fmt.Errorf("get language information for %q: %w", lang, err)
			}

			name := NewIndexName(IndexTypeDocuments, idx.name, docType, language)

			index, ok := idx.indexes[name.Full]
			if !ok {
				worker, err := idx.createIndexWorker(ctx, docType, language)
				if err != nil {
					return fmt.Errorf(
						"create index worker for %q %s: %w",
						docType, language.Code, err)
				}

				index = worker
				idx.indexes[name.Full] = worker
			}

			var jobs []*enrichJob

			for _, j := range changes[docType][lang] {
				j.start = time.Now()
				j.done = make(chan struct{})

				jobs = append(jobs, j)
			}

			group.Go(func() error {
				err := index.Process(gCtx, jobs)
				if err != nil {
					return fmt.Errorf("process %q: %w", index.indexName, err)
				}

				return nil
			})
		}
	}

	err = group.Wait()
	if err != nil {
		return fmt.Errorf("index all types: %w", err)
	}

	// Percolation only makes sense when we're tailing the log, as
	// it's used to detect what's currently happening.
	if idx.enablePercolation && caughtUp {
		var docs []postgres.PercolatorDocument

		// Reconstuct the sequence of events from change maps.
		for _, ofType := range changes {
			for _, ofLang := range ofType {
				for _, job := range ofLang {
					// Was a delete, abort.
					//
					// TODO: must handle this better.
					if job.State == nil {
						continue
					}

					docs = append(docs, postgres.PercolatorDocument{
						EventID:  job.EventID,
						Fields:   job.fields,
						Document: &job.State.Document,
					})
				}
			}
		}

		slices.SortFunc(docs, func(a, b postgres.PercolatorDocument) int {
			return int(a.EventID - b.EventID)
		})

		// Requesting percolation doesn't mean that we actually
		// percolate the document. For that to happen the index
		// worker needs to belong to the currently active index
		// set.
		idx.percolator.RequestDocumentPercolation(
			ctx,
			idx.name,
			docs,
		)

		idx.metrics.percolationEvent.WithLabelValues(
			"requested", idx.name,
		).Inc()
	}

	return nil
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
	ctx context.Context, indexType IndexType, docType string, config OpenSearchIndexConfig,
) (_ string, outErr error) {
	name := NewIndexName(indexType, idx.name, docType, config.Language)

	index := name.Full
	aliases := []string{
		name.Root,
		fmt.Sprintf("%s-%s", name.Root, config.Language.Language),
	}

	createReq := config.Settings
	createReq.Settings.Index = idx.sharding.GetSettings(name.Language)

	settingsData, err := json.Marshal(createReq)
	if err != nil {
		return "", fmt.Errorf("could not marshal index settings: %w", err)
	}

	// TODO: this might be causing issues when we do a blue/green upgrade
	// migration. New best practice is to always create a new cluster
	// instead. But it should also be possible to instead rely on the
	// database to carry the exists-information.
	existRes, err := idx.client.Indices.Exists([]string{index},
		idx.client.Indices.Exists.WithContext(ctx))
	if err != nil {
		return "", fmt.Errorf("check if index exists: %w", err)
	}

	defer elephantine.Close("index exists", existRes.Body, &outErr)

	if existRes.StatusCode != http.StatusOK {
		res, err := idx.client.Indices.Create(index,
			idx.client.Indices.Create.WithBody(bytes.NewReader(settingsData)),
			idx.client.Indices.Create.WithContext(ctx))
		if err != nil {
			return "", fmt.Errorf("create index %q: %w", index, err)
		}

		defer elephantine.Close("index create", res.Body, &outErr)

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

func (idx *Indexer) ensureAlias(index string, alias string) (outErr error) {
	res, err := idx.client.Indices.PutAlias([]string{index}, alias)
	if err != nil {
		return fmt.Errorf("could not create alias %s for index %s: %w", alias, index, err)
	}

	defer elephantine.Close("put alias", res.Body, &outErr)

	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("put alias status code: %s", res.Status())
	}

	return nil
}

type IndexType string //nolint: revive

const (
	IndexTypeDocuments IndexType = "documents"
	IndexTypePercolate IndexType = "percolate"
)

func NewIndexName(
	t IndexType,
	setName string,
	docType string,
	language LanguageInfo,
) IndexName {
	indexTypeName := internal.SanitizeDocType(docType)
	root := fmt.Sprintf("%s-%s-%s", t, setName, indexTypeName)
	localeSuffix := fmt.Sprintf("%s-%s", language.Language, language.RegionSuffix)
	languageName := fmt.Sprintf("%s-%s", indexTypeName, localeSuffix)
	full := fmt.Sprintf("%s-%s", root, localeSuffix)

	return IndexName{
		Root:     root,
		Language: languageName,
		Full:     full,
	}
}

type IndexName struct { //nolint: revive
	// Root of the index name, f.ex. "documents-setname-core_article"
	Root string
	// Language is the type and language name part of the index name, f.ex. "core_article-sv-se"
	Language string
	// Full is the full name of the index, f.ex.  "documents-setname-core_article-sv-se"
	Full string
}
