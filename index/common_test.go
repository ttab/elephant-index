package index_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/opensearch-project/opensearch-go/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/ttab/elephant-api/repository"
	"github.com/ttab/elephant-index/index"
	"github.com/ttab/elephantine"
	"github.com/ttab/elephantine/test"
	"golang.org/x/oauth2"
)

func regenerateTestFixtures() bool {
	return os.Getenv("REGENERATE") == "true"
}

type TestContext struct {
	Env           Environment
	Auth          *elephantine.AuthenticationConfig
	IndexEndpoint string
	Server        *elephantine.APIServer
}

func (tc *TestContext) AuthenticatedClient(t T, scopes ...string) *http.Client {
	t.Helper()

	src, err := tc.Auth.NewTokenSource(t.Context(), scopes)
	test.Must(t, err, "get token source for client")

	_, err = src.Token()
	test.Must(t, err, "fetch token source for client")

	return oauth2.NewClient(t.Context(), src)
}

func testingAPIServer(
	t *testing.T, logger *slog.Logger,
) TestContext {
	t.Helper()

	reg := prometheus.NewRegistry()

	instrumentation, err := elephantine.NewHTTPClientIntrumentation(reg)
	test.Must(t, err, "set up HTTP client instrumentation")

	env := SetUpBackingServices(t, instrumentation, false)

	ctx := t.Context()

	auth, err := elephantine.AuthenticationConfigFromSettings(ctx,
		elephantine.AuthenticationSettings{
			OIDCConfig:   env.OIDCConfig,
			ClientID:     t.Name(),
			ClientSecret: "pass",
		},
		[]string{"eventlog_read", "doc_read_all", "schema_read"})
	test.Must(t, err, "create authentication config")

	_, err = auth.TokenSource.Token()
	test.Must(t, err, "get an access token")

	client := oauth2.NewClient(ctx, auth.TokenSource)

	server := elephantine.NewTestAPIServer(t, logger)

	dbpool, err := pgxpool.New(ctx, env.PostgresURI)
	test.Must(t, err, "connect to index database")

	t.Cleanup(func() {
		// Don't block for close
		go dbpool.Close()
	})

	schemas := repository.NewSchemasProtobufClient(
		env.Repository.GetAPIEndpoint(), client)

	loader, err := index.NewSchemaLoader(ctx, logger.With(
		elephantine.LogKeyComponent, "schema-loader"), schemas)
	test.Must(t, err, "create schema loader")

	metrics, err := index.NewMetrics(prometheus.DefaultRegisterer)
	test.Must(t, err, "set up metrics")

	appExited := make(chan struct{})

	go func() {
		defer close(appExited)

		err = index.RunIndex(ctx, index.Parameters{
			APIServer: server,
			Logger:    logger,
			Database:  dbpool,
			Client: func(_ context.Context, _ string) (*opensearch.Client, error) {
				searchClient, err := opensearch.NewClient(opensearch.Config{
					Addresses: []string{env.OpenSearchURI},
				})
				if err != nil {
					return nil, fmt.Errorf(
						"create opensearch client: %w", err)
				}

				return searchClient, nil
			},
			DefaultCluster: env.OpenSearchURI,
			Documents: repository.NewDocumentsProtobufClient(
				env.Repository.GetAPIEndpoint(), client),
			AnonymousDocuments: repository.NewDocumentsProtobufClient(
				env.Repository.GetAPIEndpoint(), http.DefaultClient),
			Validator:      loader,
			Metrics:        metrics,
			Languages:      index.StandardLanguageOptions("sv-se"),
			NoIndexer:      false,
			AuthInfoParser: auth.AuthParser,
			Sharding: index.ShardingPolicy{
				Default: index.ShardingSettings{
					Shards:   1,
					Replicas: 0,
				},
			},
		})
		if err != nil && !errors.Is(err, context.Canceled) {
			test.Must(t, err, "run application")
		}
	}()

	readyEndpoint := fmt.Sprintf("http://%s/health/ready", server.Health.Addr())

	deadline := time.After(5 * time.Second)

	for {
		select {
		case <-appExited:
			t.Fatal("failed to start index")
		case <-ctx.Done():
			t.Fatal("test cancelled")
		case <-deadline:
			t.Fatal("index didn't become healthy in time")
		case <-time.After(100 * time.Millisecond):
		}

		res, err := http.Get(readyEndpoint) //nolint: gosec
		if err != nil {
			continue
		}

		_ = res.Body.Close()

		if res.StatusCode != http.StatusOK {
			continue
		}

		break
	}

	return TestContext{
		Env:           env,
		Auth:          auth,
		Server:        server,
		IndexEndpoint: "http://" + server.Addr(),
	}
}
