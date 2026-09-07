package main

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net/url"
	"os"
	"runtime/debug"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/ttab/elephant-api/repository"
	"github.com/ttab/elephant-index/index"
	"github.com/ttab/elephant-index/postgres"
	"github.com/ttab/elephantine"
	"github.com/urfave/cli/v3"
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

	dbpool, err := pgxpool.New(ctx, connString)
	if err != nil {
		return fmt.Errorf("create connection pool: %w", err)
	}

	defer func() {
		// Don't block for close
		go dbpool.Close()
	}()

	err = dbpool.Ping(ctx)
	if err != nil {
		return fmt.Errorf("connect to database: %w", err)
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

	anonymousDocuments := repository.NewDocumentsProtobufClient(
		repositoryEndpoint, anonClient)

	authDocuments := repository.NewDocumentsProtobufClient(
		repositoryEndpoint, authClient)

	schemas := repository.NewSchemasProtobufClient(
		repositoryEndpoint, authClient)

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
