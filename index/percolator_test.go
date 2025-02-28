package index_test

import (
	"context"
	"log/slog"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ttab/elephant-api/repository"
	"github.com/ttab/elephant-index/index"
	"github.com/ttab/elephant-index/postgres"
	"github.com/ttab/elephant-index/schema"
	"github.com/ttab/elephantine"
	"github.com/ttab/elephantine/test"
	"github.com/ttab/eltest"
)

func TestSubscriptions(t *testing.T) {
	os := eltest.NewOpenSearch(t, "2.19.0")
	pg := eltest.NewPostgres(t)

	ctx := test.Context(t)
	logger := slog.New(test.NewLogHandler(t, slog.LevelInfo))

	pgEnv := pg.Database(t, schema.Migrations, true)

	db, err := pgxpool.New(ctx, pgEnv.PostgresURI)
	test.Must(t, err, "create DB connection pool")

	clients := index.NewOSClientProvider(postgres.New(db))

	index.RunIndex(ctx, index.Parameters{
		APIServer:          elephantine.NewTestAPIServer(t, logger),
		Logger:             logger,
		Database:           db,
		DefaultCluster:     os.GetEndpoint(),
		Client:             clients.GetClientForCluster,
		Documents:          nil,
		RepositoryEndpoint: "",
		Validator:          nil,
		Metrics:            &index.Metrics{},
		Languages:          index.LanguageOptions{},
		NoIndexer:          false,
		AuthInfoParser:     nil,
		Sharding:           index.ShardingPolicy{},
	})
}

var _ repository.Documents = &PercDocs{}

type PercDocs struct {
	repository.Documents
}

// BulkGet implements repository.Documents.
func (p *PercDocs) BulkGet(context.Context, *repository.BulkGetRequest) (*repository.BulkGetResponse, error) {
	panic("unimplemented")
}

// CompactedEventlog implements repository.Documents.
func (p *PercDocs) CompactedEventlog(context.Context, *repository.GetCompactedEventlogRequest) (*repository.GetCompactedEventlogResponse, error) {
	panic("unimplemented")
}

// Delete implements repository.Documents.
func (p *PercDocs) Delete(context.Context, *repository.DeleteDocumentRequest) (*repository.DeleteDocumentResponse, error) {
	panic("unimplemented")
}

// Eventlog implements repository.Documents.
func (p *PercDocs) Eventlog(context.Context, *repository.GetEventlogRequest) (*repository.GetEventlogResponse, error) {
	panic("unimplemented")
}

// ExtendLock implements repository.Documents.
func (p *PercDocs) ExtendLock(context.Context, *repository.ExtendLockRequest) (*repository.LockResponse, error) {
	panic("unimplemented")
}

// Get implements repository.Documents.
func (p *PercDocs) Get(context.Context, *repository.GetDocumentRequest) (*repository.GetDocumentResponse, error) {
	panic("unimplemented")
}

// GetHistory implements repository.Documents.
func (p *PercDocs) GetHistory(context.Context, *repository.GetHistoryRequest) (*repository.GetHistoryResponse, error) {
	panic("unimplemented")
}

// GetMeta implements repository.Documents.
func (p *PercDocs) GetMeta(context.Context, *repository.GetMetaRequest) (*repository.GetMetaResponse, error) {
	panic("unimplemented")
}

// GetPermissions implements repository.Documents.
func (p *PercDocs) GetPermissions(context.Context, *repository.GetPermissionsRequest) (*repository.GetPermissionsResponse, error) {
	panic("unimplemented")
}

// GetStatus implements repository.Documents.
func (p *PercDocs) GetStatus(context.Context, *repository.GetStatusRequest) (*repository.GetStatusResponse, error) {
	panic("unimplemented")
}

// GetStatusHistory implements repository.Documents.
func (p *PercDocs) GetStatusHistory(context.Context, *repository.GetStatusHistoryRequest) (*repository.GetStatusHistoryReponse, error) {
	panic("unimplemented")
}

// GetStatusOverview implements repository.Documents.
func (p *PercDocs) GetStatusOverview(context.Context, *repository.GetStatusOverviewRequest) (*repository.GetStatusOverviewResponse, error) {
	panic("unimplemented")
}

// ListDeleted implements repository.Documents.
func (p *PercDocs) ListDeleted(context.Context, *repository.ListDeletedRequest) (*repository.ListDeletedResponse, error) {
	panic("unimplemented")
}

// Lock implements repository.Documents.
func (p *PercDocs) Lock(context.Context, *repository.LockRequest) (*repository.LockResponse, error) {
	panic("unimplemented")
}

// Purge implements repository.Documents.
func (p *PercDocs) Purge(context.Context, *repository.PurgeRequest) (*repository.PurgeResponse, error) {
	panic("unimplemented")
}

// Restore implements repository.Documents.
func (p *PercDocs) Restore(context.Context, *repository.RestoreRequest) (*repository.RestoreResponse, error) {
	panic("unimplemented")
}

// Unlock implements repository.Documents.
func (p *PercDocs) Unlock(context.Context, *repository.UnlockRequest) (*repository.UnlockResponse, error) {
	panic("unimplemented")
}

// Update implements repository.Documents.
func (p *PercDocs) Update(context.Context, *repository.UpdateRequest) (*repository.UpdateResponse, error) {
	panic("unimplemented")
}

// Validate implements repository.Documents.
func (p *PercDocs) Validate(context.Context, *repository.ValidateRequest) (*repository.ValidateResponse, error) {
	panic("unimplemented")
}
