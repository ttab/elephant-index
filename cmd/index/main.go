package main

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"runtime/debug"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/golang-jwt/jwt/v4"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/opensearch-project/opensearch-go/v2"
	"github.com/opensearch-project/opensearch-go/v2/signer/awsv2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rakutentech/jwk-go/jwk"
	"github.com/ttab/elephant-api/repository"
	"github.com/ttab/elephant-index/index"
	"github.com/ttab/elephant-index/postgres"
	"github.com/ttab/elephantine"
	"github.com/urfave/cli/v2"
	"golang.org/x/oauth2"
	"golang.org/x/sync/errgroup"
)

func main() {
	runCmd := cli.Command{
		Name:        "run",
		Description: "Runs the index server",
		Action:      runIndexer,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "addr",
				Value: ":1080",
			},
			&cli.StringFlag{
				Name:  "profile-addr",
				Value: ":1081",
			},
			&cli.StringFlag{
				Name:    "log-level",
				EnvVars: []string{"LOG_LEVEL"},
				Value:   "debug",
			},
			&cli.StringFlag{
				Name:     "jwks-endpoint",
				EnvVars:  []string{"JWKS_ENDPOINT"},
				Required: true,
			},
			&cli.StringFlag{
				Name:    "default-language",
				EnvVars: []string{"DEFAULT_LANGUAGE"},
				// Required for now, but shouldn't be as we want
				// the repository to enforce that language is
				// set.
				Required: true,
			},
			&cli.StringFlag{
				Name:     "token-endpoint",
				EnvVars:  []string{"TOKEN_ENDPOINT"},
				Required: true,
			},
			&cli.StringFlag{
				Name:     "repository-endpoint",
				EnvVars:  []string{"REPOSITORY_ENDPOINT"},
				Required: true,
			},
			&cli.StringFlag{
				Name:     "opensearch-endpoint",
				EnvVars:  []string{"OPENSEARCH_ENDPOINT"},
				Required: true,
			},
			&cli.StringFlag{
				Name:    "parameter-source",
				EnvVars: []string{"PARAMETER_SOURCE"},
			},
			&cli.StringFlag{
				Name:    "db",
				Value:   "postgres://indexer:pass@localhost/indexer",
				EnvVars: []string{"CONN_STRING"},
			},
			&cli.StringFlag{
				Name:    "db-parameter",
				EnvVars: []string{"CONN_STRING_PARAMETER"},
			},
			&cli.StringFlag{
				Name:    "shared-secret",
				EnvVars: []string{"SHARED_SECRET"},
			},
			&cli.StringFlag{
				Name:    "shared-secret-parameter",
				EnvVars: []string{"SHARED_SECRET_PARAMETER"},
			},
			&cli.BoolFlag{
				Name:    "managed-opensearch",
				EnvVars: []string{"MANAGED_OPEN_SEARCH"},
			},
			&cli.BoolFlag{
				Name:    "no-indexer",
				EnvVars: []string{"NO_INDEXER"},
			},
		},
	}

	app := cli.App{
		Name:  "index",
		Usage: "The Elephant indexer",
		Commands: []*cli.Command{
			&runCmd,
		},
	}

	if err := app.Run(os.Args); err != nil {
		slog.Error("failed to run application",
			elephantine.LogKeyError, err)
		os.Exit(1)
	}
}

func runIndexer(c *cli.Context) error {
	var (
		addr               = c.String("addr")
		paramSourceName    = c.String("parameter-source")
		profileAddr        = c.String("profile-addr")
		logLevel           = c.String("log-level")
		tokenEndpoint      = c.String("token-endpoint")
		defaultLanguage    = c.String("default-language")
		jwksEndpoint       = c.String("jwks-endpoint")
		opensearchEndpoint = c.String("opensearch-endpoint")
		repositoryEndpoint = c.String("repository-endpoint")
		managedOS          = c.Bool("managed-opensearch")
		noIndexer          = c.Bool("no-indexer")
	)

	logger := elephantine.SetUpLogger(logLevel, os.Stdout)

	defer func() {
		if p := recover(); p != nil {
			slog.ErrorContext(c.Context, "panic during setup",
				elephantine.LogKeyError, p,
				"stack", string(debug.Stack()),
			)

			os.Exit(2)
		}
	}()

	_, err := index.GetLanguageConfig(defaultLanguage, "")
	if err != nil {
		return fmt.Errorf("invalid default language: %w", err)
	}

	paramSource, err := elephantine.GetParameterSource(paramSourceName)
	if err != nil {
		return fmt.Errorf("get parameter source: %w", err)
	}

	connString, err := elephantine.ResolveParameter(
		c.Context, c, paramSource, "db")
	if err != nil {
		return fmt.Errorf("resolve db parameter: %w", err)
	}

	sharedSecret, err := elephantine.ResolveParameter(
		c.Context, c, paramSource, "shared-secret")
	if err != nil {
		return fmt.Errorf("resolve shared secret parameter: %w", err)
	}

	dbpool, err := pgxpool.New(c.Context, connString)
	if err != nil {
		return fmt.Errorf("create connection pool: %w", err)
	}

	defer func() {
		// Don't block for close
		go dbpool.Close()
	}()

	err = dbpool.Ping(c.Context)
	if err != nil {
		return fmt.Errorf("connect to database: %w", err)
	}

	var keySet jwk.KeySpecSet

	err = elephantine.UnmarshalHTTPResource(jwksEndpoint, &keySet)
	if err != nil {
		return fmt.Errorf("retrieve JWT keys: %w", err)
	}

	jwtKey := keySet.Filter(func(key *jwk.KeySpec) bool {
		return key.Use == "sig" && key.Algorithm == jwt.SigningMethodES384.Alg()
	}).PrimaryKey("EC")

	if jwtKey == nil {
		return errors.New("no usable JWT key")
	}

	publicJWTKey, ok := jwtKey.Key.(*ecdsa.PublicKey)
	if !ok {
		return errors.New("the returned signing key wasn't a public ECDSA key")
	}

	authConf := oauth2.Config{
		Endpoint: oauth2.Endpoint{
			TokenURL: tokenEndpoint,
		},
		Scopes: []string{"eventlog_read", "doc_read_all", "schema_read"},
	}

	pwToken, err := authConf.PasswordCredentialsToken(c.Context,
		"Indexer <system://indexer>", sharedSecret)
	if err != nil {
		return fmt.Errorf("get Elephant access token: %w", err)
	}

	authClient := oauth2.NewClient(c.Context,
		authConf.TokenSource(c.Context, pwToken))

	documents := repository.NewDocumentsProtobufClient(
		repositoryEndpoint, authClient)

	schemas := repository.NewSchemasProtobufClient(
		repositoryEndpoint, authClient)

	loader, err := index.NewSchemaLoader(c.Context, logger.With(
		elephantine.LogKeyComponent, "schema-loader"), schemas)
	if err != nil {
		return fmt.Errorf("create schema loader: %w", err)
	}

	healthServer := elephantine.NewHealthServer(profileAddr)
	router := http.NewServeMux()
	serverGroup, gCtx := errgroup.WithContext(c.Context)

	osConfig := opensearch.Config{
		Addresses: []string{opensearchEndpoint},
	}

	if managedOS {
		logger.DebugContext(c.Context, "using AWS request signing for opensearch")

		awsCfg, err := config.LoadDefaultConfig(c.Context)
		if err != nil {
			return fmt.Errorf("load default AWS config: %w", err)
		}

		// Create an AWS request Signer and load AWS configuration using default config folder or env vars.
		signer, err := awsv2.NewSignerWithService(awsCfg, "es")
		if err != nil {
			return fmt.Errorf("create request signer for opensearch: %w", err)
		}

		osConfig.Signer = signer
	}

	searchClient, err := opensearch.NewClient(osConfig)
	if err != nil {
		return fmt.Errorf(
			"failed to create opensearch client: %w", err)
	}

	metrics, err := index.NewMetrics(prometheus.DefaultRegisterer)
	if err != nil {
		return fmt.Errorf("set up metrics: %w", err)
	}

	indexer, err := index.NewIndexer(c.Context, index.IndexerOptions{
		Logger: logger.With(
			elephantine.LogKeyComponent, "indexer"),
		Metrics:         metrics,
		SetName:         "v2",
		DefaultLanguage: defaultLanguage,
		Client:          searchClient,
		Database:        dbpool,
		Documents:       documents,
		Validator:       loader,
	})
	if err != nil {
		return fmt.Errorf("create indexer: %w", err)
	}

	proxy := index.NewElasticProxy(logger, searchClient, publicJWTKey)

	router.Handle("/", proxy)

	router.Handle("/health/alive", http.HandlerFunc(func(
		w http.ResponseWriter, req *http.Request,
	) {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)

		_, _ = fmt.Fprintln(w, "I AM ALIVE!")
	}))

	healthServer.AddReadyFunction("api_liveness", func(ctx context.Context) error {
		req, err := http.NewRequestWithContext(
			ctx, http.MethodGet, fmt.Sprintf(
				"http://localhost%s/health/alive",
				addr,
			), nil,
		)
		if err != nil {
			return fmt.Errorf(
				"failed to create liveness check request: %w", err)
		}

		var client http.Client

		res, err := client.Do(req)
		if err != nil {
			return fmt.Errorf(
				"failed to perform liveness check request: %w", err)
		}

		_ = res.Body.Close()

		if res.StatusCode != http.StatusOK {
			return fmt.Errorf(
				"api liveness endpoint returned non-ok status:: %s",
				res.Status)
		}

		return nil
	})

	healthServer.AddReadyFunction("postgres", func(ctx context.Context) error {
		q := postgres.New(dbpool)

		_, err := q.ListIndexSets(ctx)
		if err != nil {
			return fmt.Errorf("failed to list index sets: %w", err)
		}

		return nil
	})

	healthServer.AddReadyFunction("opensearch", func(ctx context.Context) error {
		get := searchClient.Indices.Get

		res, err := get([]string{"documents-*"}, get.WithContext(ctx))
		if err != nil {
			return fmt.Errorf("list indices: %w", err)
		}

		_ = res.Body.Close()

		if res.StatusCode != http.StatusOK {
			return fmt.Errorf(
				"error response from server: %v", res.Status())
		}

		return nil
	})

	serverGroup.Go(func() error {
		if noIndexer {
			return nil
		}

		err := indexer.Run(gCtx)
		if err != nil {
			return fmt.Errorf("indexer error: %w", err)
		}

		return nil
	})

	serverGroup.Go(func() error {
		logger.Debug("starting health server")

		err := healthServer.ListenAndServe(gCtx)
		if err != nil {
			return fmt.Errorf("health server error: %w", err)
		}

		return nil
	})

	serverGroup.Go(func() error {
		server := http.Server{
			Addr:              addr,
			Handler:           router,
			ReadHeaderTimeout: 5 * time.Second,
		}

		err := elephantine.ListenAndServeContext(gCtx, &server)
		if err != nil {
			return fmt.Errorf("API server error: %w", err)
		}

		return nil
	})

	err = serverGroup.Wait()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	} else if err != nil {
		return fmt.Errorf("server failed to start: %w", err)
	}

	return nil
}
