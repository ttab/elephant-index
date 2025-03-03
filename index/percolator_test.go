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

	err = index.RunIndex(ctx, index.Parameters{
		APIServer:          elephantine.NewTestAPIServer(t, logger),
		Logger:             logger,
		Database:           db,
		DefaultCluster:     os.GetEndpoint(),
		Client:             clients.GetClientForCluster,
		Documents:          nil,
		AnonymousDocuments: nil,
		Validator:          nil,
		Metrics:            &index.Metrics{},
		Languages:          index.LanguageOptions{},
		NoIndexer:          false,
		AuthInfoParser:     nil,
		Sharding:           index.ShardingPolicy{},
	})
	test.Must(t, err, "run index")
}

var _ repository.Documents = &PercDocs{}

type PercDocs struct {
	repository.Documents
}

// BulkGet implements repository.Documents.
func (p *PercDocs) BulkGet(
	_ context.Context, _ *repository.BulkGetRequest,
) (*repository.BulkGetResponse, error) {
	panic("unimplemented")
}

// CompactedEventlog implements repository.Documents.
func (p *PercDocs) CompactedEventlog(
	_ context.Context, _ *repository.GetCompactedEventlogRequest,
) (*repository.GetCompactedEventlogResponse, error) {
	panic("unimplemented")
}

// Delete implements repository.Documents.
func (p *PercDocs) Delete(
	_ context.Context, _ *repository.DeleteDocumentRequest,
) (*repository.DeleteDocumentResponse, error) {
	panic("unimplemented")
}

// Eventlog implements repository.Documents.
func (p *PercDocs) Eventlog(
	_ context.Context, _ *repository.GetEventlogRequest,
) (*repository.GetEventlogResponse, error) {
	panic("unimplemented")
}

// ExtendLock implements repository.Documents.
func (p *PercDocs) ExtendLock(
	_ context.Context, _ *repository.ExtendLockRequest,
) (*repository.LockResponse, error) {
	panic("unimplemented")
}

// Get implements repository.Documents.
func (p *PercDocs) Get(
	_ context.Context, _ *repository.GetDocumentRequest,
) (*repository.GetDocumentResponse, error) {
	panic("unimplemented")
}

// GetHistory implements repository.Documents.
func (p *PercDocs) GetHistory(
	_ context.Context, _ *repository.GetHistoryRequest,
) (*repository.GetHistoryResponse, error) {
	panic("unimplemented")
}

// GetMeta implements repository.Documents.
func (p *PercDocs) GetMeta(
	_ context.Context, _ *repository.GetMetaRequest,
) (*repository.GetMetaResponse, error) {
	panic("unimplemented")
}

// GetPermissions implements repository.Documents.
func (p *PercDocs) GetPermissions(
	_ context.Context, _ *repository.GetPermissionsRequest,
) (*repository.GetPermissionsResponse, error) {
	panic("unimplemented")
}

// GetStatus implements repository.Documents.
func (p *PercDocs) GetStatus(
	_ context.Context, _ *repository.GetStatusRequest,
) (*repository.GetStatusResponse, error) {
	panic("unimplemented")
}

// GetStatusHistory implements repository.Documents.
func (p *PercDocs) GetStatusHistory(
	_ context.Context, _ *repository.GetStatusHistoryRequest,
) (*repository.GetStatusHistoryReponse, error) {
	panic("unimplemented")
}

// GetStatusOverview implements repository.Documents.
func (p *PercDocs) GetStatusOverview(
	_ context.Context, _ *repository.GetStatusOverviewRequest,
) (*repository.GetStatusOverviewResponse, error) {
	panic("unimplemented")
}

// ListDeleted implements repository.Documents.
func (p *PercDocs) ListDeleted(
	_ context.Context, _ *repository.ListDeletedRequest,
) (*repository.ListDeletedResponse, error) {
	panic("unimplemented")
}

// Lock implements repository.Documents.
func (p *PercDocs) Lock(
	_ context.Context, _ *repository.LockRequest,
) (*repository.LockResponse, error) {
	panic("unimplemented")
}

// Purge implements repository.Documents.
func (p *PercDocs) Purge(
	_ context.Context, _ *repository.PurgeRequest,
) (*repository.PurgeResponse, error) {
	panic("unimplemented")
}

// Restore implements repository.Documents.
func (p *PercDocs) Restore(
	_ context.Context, _ *repository.RestoreRequest,
) (*repository.RestoreResponse, error) {
	panic("unimplemented")
}

// Unlock implements repository.Documents.
func (p *PercDocs) Unlock(
	_ context.Context, _ *repository.UnlockRequest,
) (*repository.UnlockResponse, error) {
	panic("unimplemented")
}

// Update implements repository.Documents.
func (p *PercDocs) Update(
	_ context.Context, _ *repository.UpdateRequest,
) (*repository.UpdateResponse, error) {
	panic("unimplemented")
}

// Validate implements repository.Documents.
func (p *PercDocs) Validate(
	_ context.Context, _ *repository.ValidateRequest,
) (*repository.ValidateResponse, error) {
	panic("unimplemented")
}
