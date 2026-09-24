package main

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"math"
	"net/url"
	"os"
	"runtime/debug"
	"time"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/ttab/elephant-api/repository/repositoryconnect"
	"github.com/ttab/elephant-index/index"
	"github.com/ttab/elephant-index/postgres"
	"github.com/ttab/elephantine"
	"github.com/ttab/elephantine/pg"
	"github.com/ttab/elephantine/rpc"
	"github.com/urfave/cli/v3"
)

// defaultDBMaxConns is the size of the query pool, set here rather than left
// to pgx: its default is max(4, NumCPU()) read from the node's cpuset rather
// than the cgroup quota, so an unset pool tracks whichever node the pod lands
// on and changes size invisibly on reschedule.
//
// The background work holds about four connections at its busiest. Each
// indexer — one per enabled index set, so two during a re-index — runs one
// short statement at a time, the percolator's event loop and its update
// handler can each hold a transaction across an OpenSearch write, and the job
// locks (one per indexer plus the percolator's), the coordinator's
// reconciliation and the cleanup loops run a statement every few seconds or
// less often. The search API runs one to three
// short queries per request and holds no connection while a subscription long
// poll waits. Eight covers the background work with as much again for a burst
// of requests. The one thing that can exceed it is a schema change reaching
// many document types in the same batch, where each index worker holds a
// transaction across its OpenSearch mapping update; those queue for a
// connection rather than fail, and are rare. Revisit once
// pgxpool_empty_acquire_wait_seconds_total says what the pool actually needs.
const defaultDBMaxConns = 8

// listenPoolMaxConns is the size of the direct pool when queries go through a
// bouncer: it then carries only the coordinator's LISTEN session, which the
// subscriber hijacks out of the pool, and the subscriber's ping.
const listenPoolMaxConns = 2

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
				Name:    "addr",
				Sources: cli.EnvVars("ADDR"),
				Value:   ":1080",
			},
			&cli.StringFlag{
				Name:    "profile-addr",
				Sources: cli.EnvVars("PROFILE_ADDR"),
				Value:   ":1081",
			},
			&cli.StringFlag{
				Name:    "tls-addr",
				Value:   ":1443",
				Sources: cli.EnvVars("TLS_ADDR", "TLS_LISTEN_ADDR"),
			},
			&cli.StringFlag{
				Name:    "cert-file",
				Sources: cli.EnvVars("TLS_CERT_PATH"),
			},
			&cli.StringFlag{
				Name:    "key-file",
				Sources: cli.EnvVars("TLS_KEY_PATH"),
			},
			&cli.StringFlag{
				Name:    "log-level",
				Sources: cli.EnvVars("LOG_LEVEL"),
				Value:   "debug",
			},
			&cli.StringFlag{
				Name:    "default-language",
				Sources: cli.EnvVars("DEFAULT_LANGUAGE"),
				// Required for now, but shouldn't be as we want
				// the repository to enforce that language is
				// set.
				Required: true,
			},
			&cli.StringFlag{
				Name:     "repository-endpoint",
				Sources:  cli.EnvVars("REPOSITORY_ENDPOINT"),
				Required: true,
			},
			&cli.StringFlag{
				Name:    "opensearch-endpoint",
				Sources: cli.EnvVars("OPENSEARCH_ENDPOINT"),
			},
			&cli.StringFlag{
				Name:     "password-key",
				Sources:  cli.EnvVars("PASSWORD_ENCRYPTION_KEY"),
				Usage:    "32 byte hex encoded encryption key",
				Required: true,
			},
			&cli.StringFlag{
				Name:    "parameter-source",
				Sources: cli.EnvVars("PARAMETER_SOURCE"),
			},
			&cli.StringFlag{ //nolint:gosec // local dev default, not a real secret
				Name:    "db",
				Value:   "postgres://elephant-index:pass@localhost/elephant-index",
				Sources: cli.EnvVars("CONN_STRING"),
			},
			&cli.StringFlag{
				Name:    "db-bouncer",
				Usage:   "Connection string routed through PgBouncer, used for all DB operations except the LISTEN session",
				Sources: cli.EnvVars("BOUNCER_CONN_STRING"),
			},
			&cli.IntFlag{
				Name:    "db-max-conns",
				Sources: cli.EnvVars("DB_MAX_CONNS"),
				Value:   defaultDBMaxConns,
				Usage: `Maximum size of the Postgres connection pool used for
queries. Overrides pool_max_conns in the connection string. Zero or less leaves
the pool to size itself, which means max(4, NumCPU()) read from the node's
cpuset. With a bouncer configured the direct pool is fixed at 2 and this applies
to the bouncer pool.`,
			},
			&cli.StringFlag{
				Name:    "db-parameter",
				Sources: cli.EnvVars("CONN_STRING_PARAMETER"),
			},
			&cli.BoolFlag{
				Name:    "managed-opensearch",
				Sources: cli.EnvVars("MANAGED_OPENSEARCH"),
			},
			&cli.BoolFlag{
				Name:    "no-indexer",
				Sources: cli.EnvVars("NO_INDEXER"),
			},
			&cli.StringFlag{
				Name:    "sharding-policy",
				Sources: cli.EnvVars("SHARDING_POLICY"),
			},
			&cli.StringSliceFlag{
				Name:    "cors-host",
				Usage:   "CORS hosts to allow, supports wildcards",
				Sources: cli.EnvVars("CORS_HOSTS"),
			},
		},
	}

	runCmd.Flags = append(runCmd.Flags, elephantine.AuthenticationCLIFlags()...)

	app := cli.Command{
		Name:  "index",
		Usage: "The Elephant indexer",
		Commands: []*cli.Command{
			&runCmd,
		},
	}

	if err := app.Run(context.Background(), os.Args); err != nil {
		slog.Error("failed to run application",
			elephantine.LogKeyError, err)
		os.Exit(1)
	}
}

var Scopes = []string{"eventlog_read", "doc_read_all", "schema_read"}

func runIndexer(ctx context.Context, cmd *cli.Command) error {
	var (
		addr               = cmd.String("addr")
		profileAddr        = cmd.String("profile-addr")
		tlsAddr            = cmd.String("tls-addr")
		certFile           = cmd.String("cert-file")
		keyFile            = cmd.String("key-file")
		logLevel           = cmd.String("log-level")
		defaultLanguage    = cmd.String("default-language")
		connString         = cmd.String("db")
		bouncerConnString  = cmd.String("db-bouncer")
		dbMaxConns         = cmd.Int("db-max-conns")
		opensearchEndpoint = cmd.String("opensearch-endpoint")
		repositoryEndpoint = cmd.String("repository-endpoint")
		managedOS          = cmd.Bool("managed-opensearch")
		noIndexer          = cmd.Bool("no-indexer")
		shardingPolicy     = cmd.String("sharding-policy")
		corsHosts          = cmd.StringSlice("cors-host")
		passwordKeyStr     = cmd.String("password-key")
	)

	logger := elephantine.SetUpLogger(logLevel, os.Stdout)

	defer func() {
		if p := recover(); p != nil {
			slog.ErrorContext(ctx, "panic during setup",
				elephantine.LogKeyError, p,
				"stack", string(debug.Stack()),
			)

			os.Exit(2)
		}
	}()

	// Decode and validate password key.
	passwordKeyData, err := hex.DecodeString(passwordKeyStr)
	if err != nil {
		return fmt.Errorf("invalid password key: %w", err)
	}

	if len(passwordKeyData) != 32 {
		return fmt.Errorf("invalid password key length %d, expected %d",
			len(passwordKeyData), 32)
	}

	var passwordKey [32]byte

	copy(passwordKey[:], passwordKeyData)

	sharding, err := index.ParseShardingPolicy(
		shardingPolicy, index.ShardingSettings{
			Shards:   2,
			Replicas: 2,
		})
	if err != nil {
		return fmt.Errorf("invalid sharding policy: %w", err)
	}

	langOpts := index.StandardLanguageOptions(defaultLanguage)

	useBouncer := bouncerConnString != "" && bouncerConnString != connString

	listenMaxConns := dbMaxConns
	if useBouncer {
		listenMaxConns = listenPoolMaxConns
	}

	// The direct pool carries the coordinator's LISTEN session, which
	// cannot go through a transaction pooler. Without a bouncer it is also
	// the pool everything else runs on.
	listenPool, err := newPool(ctx, connString, listenMaxConns)
	if err != nil {
		return fmt.Errorf("direct database: %w", err)
	}

	defer func() {
		// Don't block for close
		go listenPool.Close()
	}()

	dbpool := listenPool

	if useBouncer {
		dbpool, err = newPool(ctx, bouncerConnString, dbMaxConns)
		if err != nil {
			return fmt.Errorf("bouncer database: %w", err)
		}

		defer func() {
			go dbpool.Close()
		}()
	}

	logger.InfoContext(ctx, "created connection pools",
		"max_conns", dbMaxConns,
		"direct_max_conns", listenMaxConns,
		"bouncer", useBouncer)

	// The listen pool doubles as the main pool when no bouncer is
	// configured, and is only registered separately when it is separate.
	poolMetrics := elephantine.NewMetricsHelper(prometheus.DefaultRegisterer)

	poolMetrics.Collector("main", pg.NewPoolStatCollector(dbpool, "main"))

	if listenPool != dbpool {
		poolMetrics.Collector("pubsub",
			pg.NewPoolStatCollector(listenPool, "pubsub"))
	}

	err = poolMetrics.Err()
	if err != nil {
		return fmt.Errorf("register connection pool metrics: %w", err)
	}

	auth, err := elephantine.AuthenticationConfigFromCLI(ctx, cmd, Scopes)
	if err != nil {
		return fmt.Errorf("set up authentication: %w", err)
	}

	anonClient := elephantine.NewHTTPClient(30 * time.Second)

	authClient := elephantine.NewHTTPClient(
		30*time.Second,
		elephantine.WithTokenSource(auth.TokenSource),
		elephantine.LongpollClient())

	// The anonymous client is the one the handlers that serve a caller use,
	// and they put the caller's own token on the context per request, so it
	// needs the interceptor that copies those headers onto the wire. The
	// authenticated client is for the service's own background work and
	// carries its token in the http.Client.
	anonymousDocuments := repositoryconnect.NewDocumentsServiceClient(
		anonClient, repositoryEndpoint,
		connect.WithInterceptors(rpc.PropagateHeaders()))

	authDocuments := repositoryconnect.NewDocumentsServiceClient(
		authClient, repositoryEndpoint)

	schemas := repositoryconnect.NewSchemasServiceClient(
		authClient, repositoryEndpoint)

	loader, err := index.NewSchemaLoader(ctx, logger.With(
		elephantine.LogKeyComponent, "schema-loader"), schemas)
	if err != nil {
		return fmt.Errorf("create schema loader: %w", err)
	}

	clients := index.NewOSClientProvider(postgres.New(dbpool), passwordKey)

	metrics, err := index.NewMetrics(prometheus.DefaultRegisterer)
	if err != nil {
		return fmt.Errorf("set up metrics: %w", err)
	}

	serverOpts := []elephantine.APIServerOption{
		elephantine.APIServerCORSHosts(corsHosts...),
	}

	if certFile != "" {
		serverOpts = append(serverOpts,
			elephantine.APIServerTLS(tlsAddr, certFile, keyFile))
	}

	server := elephantine.NewAPIServer(logger, addr, profileAddr, serverOpts...)

	osURL, defaultAuth, err := parseDefaultCluster(
		opensearchEndpoint, managedOS, passwordKey)
	if err != nil {
		return err
	}

	err = index.RunIndex(ctx, index.Parameters{
		APIServer:          server,
		Logger:             logger,
		Database:           dbpool,
		ListenDatabase:     listenPool,
		Client:             clients.GetClientForCluster,
		DefaultCluster:     osURL,
		DefaultClusterAuth: defaultAuth,
		Documents:          authDocuments,
		AnonymousDocuments: anonymousDocuments,
		Validator:          loader,
		Metrics:            metrics,
		Languages:          langOpts,
		NoIndexer:          noIndexer,
		AuthInfoParser:     auth.AuthParser,
		Sharding:           sharding,
		PasswordKey:        passwordKey,
	})
	if err != nil {
		return fmt.Errorf("run application: %w", err)
	}

	return nil
}

// newPool creates a connection pool and verifies that the database answers.
// A positive maxConns sizes the pool; zero or less leaves that to the
// connection string or pgx.
func newPool(
	ctx context.Context, connString string, maxConns int,
) (*pgxpool.Pool, error) {
	conf, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, fmt.Errorf("parse connection string: %w", err)
	}

	if maxConns > math.MaxInt32 {
		return nil, fmt.Errorf("max conns %d exceeds %d", maxConns, math.MaxInt32)
	}

	if maxConns > 0 {
		conf.MaxConns = int32(maxConns)
	}

	pool, err := pgxpool.NewWithConfig(ctx, conf)
	if err != nil {
		return nil, fmt.Errorf("create connection pool: %w", err)
	}

	err = pool.Ping(ctx)
	if err != nil {
		pool.Close()

		return nil, fmt.Errorf("connect to database: %w", err)
	}

	return pool, nil
}

// parseDefaultCluster turns --opensearch-endpoint into the URL and credentials
// of the cluster to register on a fresh installation. An empty endpoint yields
// a nil URL, which is what tells RunIndex not to create a default index set.
//
// Credentials are moved out of the URL and into the returned ClusterAuth,
// which encrypts the password, so that the URL stored on the cluster row
// carries no secret.
//
// This used to be an inline block, and the history is worth knowing: it
// assigned the parsed URL with ":=" inside an "if", which declared a new
// variable rather than filling in the outer one. The outer URL stayed nil, so
// DefaultCluster was always nil and EnsureDefaultIndexSet never ran -- the
// flag was silently ignored on every fresh installation, while the credential
// handling next to it worked, because it assigned to fields of a struct
// declared outside the block. Returning the values instead of assigning to
// captured ones is what stops that from being reintroduced.
func parseDefaultCluster(
	endpoint string, managedOS bool, passwordKey [32]byte,
) (*url.URL, index.ClusterAuth, error) {
	auth := index.ClusterAuth{
		IAM: managedOS,
	}

	if endpoint == "" {
		return nil, auth, nil
	}

	osURL, err := url.Parse(endpoint)
	if err != nil {
		return nil, index.ClusterAuth{}, fmt.Errorf(
			"invalid open search endpoint: %w", err)
	}

	if osURL.User == nil {
		return osURL, auth, nil
	}

	auth.Username = osURL.User.Username()

	pw, _ := osURL.User.Password()

	err = auth.SetPassword(pw, passwordKey)
	if err != nil {
		return nil, index.ClusterAuth{}, fmt.Errorf(
			"set default cluster auth password: %w", err)
	}

	// A username and password in the endpoint is an explicit choice of basic
	// authentication over IAM signing.
	auth.IAM = false

	osURL.User = nil

	return osURL, auth, nil
}
