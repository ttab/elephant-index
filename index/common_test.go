package index_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/opensearch-project/opensearch-go/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/ttab/elephant-api/index"
	"github.com/ttab/elephant-api/index/indexconnect"
	"github.com/ttab/elephant-api/repository"
	"github.com/ttab/elephant-api/repository/repositoryconnect"
	indexsvc "github.com/ttab/elephant-index/index"
	"github.com/ttab/elephantine"
	"github.com/ttab/elephantine/rpc"
	"github.com/ttab/elephantine/test"
	"github.com/ttab/revisor"
	"github.com/ttab/revisorschemas"
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
	IndexDB       *pgxpool.Pool
}

func (tc *TestContext) AuthenticatedClient(t T, scopes ...string) *http.Client {
	t.Helper()

	src, err := tc.Auth.NewTokenSource(t.Context(), scopes)
	test.Mustf(t, err, "get token source for client")

	_, err = src.Token()
	test.Mustf(t, err, "fetch token source for client")

	return oauth2.NewClient(t.Context(), src)
}

func testingAPIServer(
	t *testing.T, logger *slog.Logger,
) TestContext {
	t.Helper()

	reg := prometheus.NewRegistry()

	instrumentation, err := elephantine.NewHTTPClientIntrumentation(reg)
	test.Mustf(t, err, "set up HTTP client instrumentation")

	env := SetUpBackingServices(t, instrumentation, false)

	ctx := t.Context()

	auth, err := elephantine.AuthenticationConfigFromSettings(ctx,
		elephantine.AuthenticationSettings{
			OIDCConfig:   env.OIDCConfig,
			ClientID:     t.Name(),
			ClientSecret: "pass",
		},
		[]string{"eventlog_read", "doc_read_all", "schema_read"})
	test.Mustf(t, err, "create authentication config")

	_, err = auth.TokenSource.Token()
	test.Mustf(t, err, "get an access token")

	client := oauth2.NewClient(ctx, auth.TokenSource)

	server, _ := elephantine.NewTestAPIServer(t, logger)

	dbpool, err := pgxpool.New(ctx, env.PostgresURI)
	test.Mustf(t, err, "connect to index database")

	t.Cleanup(func() {
		// Don't block for close
		go dbpool.Close()
	})

	schemas := repositoryconnect.NewSchemasServiceClient(
		client, env.Repository.GetAPIEndpoint())

	registerCoreSchemas(ctx, t, auth, env)

	loader, err := indexsvc.NewSchemaLoader(ctx, logger.With(
		elephantine.LogKeyComponent, "schema-loader"), schemas)
	test.Mustf(t, err, "create schema loader")

	metrics, err := indexsvc.NewMetrics(reg)
	test.Mustf(t, err, "set up metrics")

	appExited := make(chan struct{})

	openSearchURL, err := url.Parse(env.OpenSearchURI)
	test.Mustf(t, err, "parse Open Search URL")

	go func() {
		defer close(appExited)

		err = indexsvc.RunIndex(ctx, indexsvc.Parameters{
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
			DefaultCluster: openSearchURL,
			Documents: repositoryconnect.NewDocumentsServiceClient(
				client, env.Repository.GetAPIEndpoint()),
			// PropagateHeaders as in cmd/index: the handlers that
			// serve a caller forward the caller's own token on
			// this client, so without the interceptor the call
			// goes out anonymous and the repository refuses it.
			// TestGetFlatDocument is what notices.
			AnonymousDocuments: repositoryconnect.NewDocumentsServiceClient(
				http.DefaultClient, env.Repository.GetAPIEndpoint(),
				connect.WithInterceptors(rpc.PropagateHeaders())),
			Validator:      loader,
			Metrics:        metrics,
			Languages:      indexsvc.StandardLanguageOptions("sv-se"),
			NoIndexer:      false,
			AuthInfoParser: auth.AuthParser,
			Sharding: indexsvc.ShardingPolicy{
				Default: indexsvc.ShardingSettings{
					Shards:   1,
					Replicas: 0,
				},
			},
		})
		if err != nil && !errors.Is(err, context.Canceled) {
			test.Mustf(t, err, "run application")
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
		IndexDB:       dbpool,
	}
}

// rpcStack is the stack the test clients speak. TEST_RPC_STACK selects it for
// a whole run, so CI can run the suite twice against the one server that
// serves both, and a test that has to pin a stack calls the typed
// constructors below directly.
type rpcStack string

const (
	stackTwirp   rpcStack = "twirp"
	stackConnect rpcStack = "connect"
)

// defaultRPCStack reads TEST_RPC_STACK, defaulting to Twirp so that a plain
// `go test ./...` exercises the stack the service's callers are still on.
func defaultRPCStack(t *testing.T) rpcStack {
	t.Helper()

	switch v := os.Getenv("TEST_RPC_STACK"); v {
	case "", string(stackTwirp):
		return stackTwirp
	case string(stackConnect):
		return stackConnect
	default:
		t.Fatalf("unknown TEST_RPC_STACK %q, want %q or %q",
			v, stackTwirp, stackConnect)

		return ""
	}
}

// SearchClient returns a SearchV1 client on the stack TEST_RPC_STACK selects.
func (tc *TestContext) SearchClient(
	t *testing.T, scopes ...string,
) index.SearchV1 {
	t.Helper()

	return tc.SearchClientOn(t, defaultRPCStack(t), scopes...)
}

// SearchClientOn returns a SearchV1 client on the given stack. Both
// constructors return the same plain interface, and the bearer token is
// attached the same way, so a test cannot tell them apart except through the
// wire differences that are the point of testing both.
func (tc *TestContext) SearchClientOn(
	t *testing.T, stack rpcStack, scopes ...string,
) index.SearchV1 {
	t.Helper()

	client := tc.AuthenticatedClient(t, scopes...)

	if stack == stackConnect {
		return indexconnect.NewSearchV1ServiceClient(
			client, tc.IndexEndpoint)
	}

	return index.NewSearchV1ProtobufClient(tc.IndexEndpoint, client)
}

// ManagementClient returns a Management client on the stack TEST_RPC_STACK
// selects.
func (tc *TestContext) ManagementClient(
	t *testing.T, scopes ...string,
) index.Management {
	t.Helper()

	return tc.ManagementClientOn(t, defaultRPCStack(t), scopes...)
}

// ManagementClientOn returns a Management client on the given stack.
func (tc *TestContext) ManagementClientOn(
	t *testing.T, stack rpcStack, scopes ...string,
) index.Management {
	t.Helper()

	client := tc.AuthenticatedClient(t, scopes...)

	if stack == stackConnect {
		return indexconnect.NewManagementServiceClient(
			client, tc.IndexEndpoint)
	}

	return index.NewManagementProtobufClient(tc.IndexEndpoint, client)
}

// registerCoreSchemas activates the core revisor schemas in the test
// repository, without which every document write fails validation with
// "undeclared document type".
//
// The repository used to do this itself: up to v1.2.5 it registered the
// embedded core schemas at startup unless --no-core-schema was passed, and
// this suite relied on that. Schema generations replaced it, so the schemas
// now have to be pushed over the API, and the set is the same three that
// EnsureCoreSchema used.
//
// RegisterGeneration takes the whole set and activates it in one call, and it
// is idempotent on the schema versions, so it does not matter that every test
// gets its own repository.
func registerCoreSchemas(
	ctx context.Context, t *testing.T,
	auth *elephantine.AuthenticationConfig, env Environment,
) {
	t.Helper()

	src, err := auth.NewTokenSource(ctx, []string{"schema_admin"})
	test.Mustf(t, err, "get a schema admin token source")

	client := repositoryconnect.NewSchemasServiceClient(
		oauth2.NewClient(ctx, src), env.Repository.GetAPIEndpoint())

	files := []string{
		"se.ecms.json", "se.ecms.metadoc.json", "se.ecms.planning.json",
	}

	// Decoding is what gives us the name the schema declares, which is not
	// the file name, and it fails here rather than in the repository if a
	// schema does not parse.
	sets, err := revisor.DecodeConstraintSetsFS(revisorschemas.Files(), files...)
	test.Mustf(t, err, "decode the embedded core schemas")

	specs := make([]*repository.Schema, len(sets))

	for i, set := range sets {
		data, err := revisorschemas.Files().ReadFile(files[i])
		test.Mustf(t, err, "read %s", files[i])

		specs[i] = &repository.Schema{
			Name:    set.Name,
			Version: revisorschemas.Version(),
			Spec:    string(data),
		}
	}

	_, err = client.RegisterGeneration(ctx,
		&repository.RegisterGenerationRequest{
			Schemas:    specs,
			Activation: repository.SchemaActivation_ACTIVATION_ACTIVE,
		})
	test.Mustf(t, err, "register and activate the core schemas")
}
