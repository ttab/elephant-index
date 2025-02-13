package index

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
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
	"github.com/twitchtv/twirp"
	"golang.org/x/sync/errgroup"
)

const (
	LogKeyIndexOperation = "index_operation"
	LogKeyResponseStatus = "response_status"
)

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
	DefaultLanguage   string
	DefaultRegions    map[string]string
	EnablePercolation bool
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
		defaultLanguage:   opts.DefaultLanguage,
		defaultRegions:    opts.DefaultRegions,
		sharding:          opts.Sharding,
		enablePercolation: opts.EnablePercolation,
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
	logger            *slog.Logger
	metrics           *Metrics
	name              string
	defaultLanguage   string
	defaultRegions    map[string]string
	sharding          ShardingPolicy
	database          *pgxpool.Pool
	documents         repository.Documents
	vSource           ValidatorSource
	client            *opensearch.Client
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
	DeleteEvent   = "document_delete"
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

	changes := make(map[string]map[string]map[string]*enrichJob)

	for _, item := range log.Items {
		if item.Type == "" {
			pos = item.Id

			continue
		}

		var doc, metaDoc *newsdoc.Document

		docUUID := item.Uuid
		language := strings.ToLower(item.Language)

		// Switch to main document on meta doc updates.
		if item.MainDocument != "" {
			docUUID = item.MainDocument

			// Treat deleted meta documents as document update
			// events.
			if item.Event == DeleteEvent {
				item.Event = DocumentEvent
			}
		}

		// Load document early so that we can pivot to a delete if the
		// document has been deleted. But only try to fetch it if we
		// don't already know that it has been deleted.
		if item.Event != DeleteEvent { //nolint: nestif
			docRes, err := idx.documents.Get(ctx,
				&repository.GetDocumentRequest{
					Uuid:         docUUID,
					MetaDocument: repository.GetMetaDoc_META_INCLUDE,
				})
			if elephantine.IsTwirpErrorCode(err, twirp.NotFound) {
				// TODO: we need to blanket delete the ID in all indexes
				// here. Resorting to doing a delete for default
				// language at the moment.
				item.Event = DeleteEvent
			} else if err != nil {
				return 0, fmt.Errorf("get document: %w", err)
			}

			if docRes != nil {
				doc = docRes.Document

				if docRes.Meta != nil {
					metaDoc = docRes.Meta.Document
				}

				// TODO: include main document type in the
				// eventlog?
				item.Type = doc.Type
				// Normalize to lowercase
				language = strings.ToLower(doc.Language)
			}
		}

		byType, ok := changes[item.Type]
		if !ok {
			byType = make(map[string]map[string]*enrichJob)
			changes[item.Type] = byType
		}

		byLang, ok := byType[language]
		if !ok {
			byLang = make(map[string]*enrichJob)
			byType[language] = byLang
		}

		switch item.Event {
		case DeleteEvent:
			byLang[docUUID] = &enrichJob{
				UUID:      docUUID,
				Operation: opDelete,
				doc:       doc,
			}
		case DocumentEvent, ACLEvent, StatusEvent, WorkflowEvent:
			if item.OldLanguage != "" && item.Language != item.OldLanguage {
				byObLang, ok := byType[item.OldLanguage]
				if !ok {
					byObLang = make(map[string]*enrichJob)
					byType[item.OldLanguage] = byObLang
				}

				byObLang[docUUID] = &enrichJob{
					UUID:      docUUID,
					Operation: opDelete,
				}
			}

			byLang[docUUID] = &enrichJob{
				UUID:      docUUID,
				Operation: opUpdate,
				doc:       doc,
				metadoc:   metaDoc,
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
				worker, err := idx.createIndexWorker(ctx, docType, lang)
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
	lang string,
) (*indexWorker, error) {
	conf, err := GetIndexConfig(
		lang, idx.defaultLanguage, idx.defaultRegions)
	if err != nil {
		return nil, fmt.Errorf(
			"could not get language config: %w", err)
	}

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
		name, percolateName, docType, conf, 8)
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
