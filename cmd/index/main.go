package main

import (
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"runtime/debug"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/ttab/elephant-api/repository"
	"github.com/ttab/elephant-index/index"
	"github.com/ttab/elephant-index/postgres"
	"github.com/ttab/elephantine"
	"github.com/urfave/cli/v2"
	"golang.org/x/oauth2"
)

func main() {
	err := godotenv.Load()
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		slog.Error("exiting: ",
			elephantine.LogKeyError, err)
		os.Exit(1)
	}

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
				Name:    "jwks-endpoint",
				EnvVars: []string{"JWKS_ENDPOINT"},
			},
			&cli.StringFlag{
				Name:    "jwks-endpoint-parameter",
				EnvVars: []string{"JWKS_ENDPOINT_PARAMETER"},
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
				Value:   "postgres://elephant-index:pass@localhost/elephant-index",
				EnvVars: []string{"CONN_STRING"},
			},
			&cli.StringFlag{
				Name:    "db-parameter",
				EnvVars: []string{"CONN_STRING_PARAMETER"},
			},
			&cli.BoolFlag{
				Name:    "managed-opensearch",
				EnvVars: []string{"MANAGED_OPENSEARCH"},
			},
			&cli.BoolFlag{
				Name:    "no-indexer",
				EnvVars: []string{"NO_INDEXER"},
			},
			&cli.StringFlag{
				Name:    "sharding-policy",
				EnvVars: []string{"SHARDING_POLICY"},
			},
		},
	}

	runCmd.Flags = append(runCmd.Flags, elephantine.AuthenticationCLIFlags()...)

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

var Scopes = []string{"eventlog_read", "doc_read_all", "schema_read"}

func runIndexer(c *cli.Context) error {
	var (
		addr               = c.String("addr")
		paramSourceName    = c.String("parameter-source")
		profileAddr        = c.String("profile-addr")
		logLevel           = c.String("log-level")
		defaultLanguage    = c.String("default-language")
		opensearchEndpoint = c.String("opensearch-endpoint")
		repositoryEndpoint = c.String("repository-endpoint")
		managedOS          = c.Bool("managed-opensearch")
		noIndexer          = c.Bool("no-indexer")
		shardingPolicy     = c.String("sharding-policy")
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

	sharding, err := index.ParseShardingPolicy(
		shardingPolicy, index.ShardingSettings{
			Shards:   2,
			Replicas: 2,
		})
	if err != nil {
		return fmt.Errorf("invalid sharding policy: %w", err)
	}

	_, err = index.GetIndexConfig(defaultLanguage, "", nil)
	if err != nil {
		return fmt.Errorf("create index config: %w", err)
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

	auth, err := elephantine.AuthenticationConfigFromCLI(
		c, paramSource, Scopes)
	if err != nil {
		return fmt.Errorf("set up authentication: %w", err)
	}

	authClient := oauth2.NewClient(c.Context, auth.TokenSource)

	documents := repository.NewDocumentsProtobufClient(
		repositoryEndpoint, authClient)

	schemas := repository.NewSchemasProtobufClient(
		repositoryEndpoint, authClient)

	loader, err := index.NewSchemaLoader(c.Context, logger.With(
		elephantine.LogKeyComponent, "schema-loader"), schemas)
	if err != nil {
		return fmt.Errorf("create schema loader: %w", err)
	}

	clients := index.NewOSClientProvider(postgres.New(dbpool))

	metrics, err := index.NewMetrics(prometheus.DefaultRegisterer)
	if err != nil {
		return fmt.Errorf("set up metrics: %w", err)
	}

	err = index.RunIndex(c.Context, index.Parameters{
		Addr:           addr,
		ProfileAddr:    profileAddr,
		Logger:         logger,
		Database:       dbpool,
		Client:         clients.GetClientForCluster,
		DefaultCluster: opensearchEndpoint,
		ClusterAuth: index.ClusterAuth{
			IAM: managedOS,
		},
		Documents:          documents,
		RepositoryEndpoint: repositoryEndpoint,
		Validator:          loader,
		Metrics:            metrics,
		DefaultLanguage:    defaultLanguage,
		NoIndexer:          noIndexer,
		AuthInfoParser:     auth.AuthParser,
		Sharding:           sharding,
	})
	if err != nil {
		return fmt.Errorf("run application: %w", err)
	}

	return nil
}
