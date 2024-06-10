package main

import (
	"fmt"
	"log/slog"
	"os"
	"runtime/debug"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/ttab/elephant-api/repository"
	"github.com/ttab/elephant-index/index"
	"github.com/ttab/elephant-index/postgres"
	"github.com/ttab/elephantine"
	"github.com/urfave/cli/v2"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
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
				Name:     "jwks-endpoint-parameter",
				EnvVars:  []string{"JWKS_ENDPOINT_PARAMETER"},
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
				Name:     "token-endpoint-parameter",
				EnvVars:  []string{"TOKEN_ENDPOINT_PARAMETER"},
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
				Name:     "auth-strategy",
				EnvVars:  []string{"AUTH_STRATEGY"},
				Usage:    fmt.Sprintf("Authentication strategy (%s, %s, %s)", Password, ClientCredentials, MockJWT),
				Required: true,
			},
			&cli.StringFlag{
				Name:    "shared-secret",
				EnvVars: []string{"SHARED_SECRET"},
				Usage:   fmt.Sprintf("optional shared secret for the %s auth strategy", MockJWT),
			},
			&cli.StringFlag{
				Name:    "shared-secret-parameter",
				EnvVars: []string{"SHARED_SECRET_PARAMETER"},
			},
			&cli.StringFlag{
				Name:    "client-id",
				EnvVars: []string{"CLIENT_ID"},
			},
			&cli.StringFlag{
				Name:    "client-id-parameter",
				EnvVars: []string{"CLIENT_ID_PARAMETER"},
			},
			&cli.StringFlag{
				Name:    "client-secret",
				EnvVars: []string{"CLIENT_SECRET"},
			},
			&cli.StringFlag{
				Name:    "client-secret-parameter",
				EnvVars: []string{"CLIENT_SECRET_PARAMETER"},
			},
			&cli.StringFlag{
				Name:    "username",
				EnvVars: []string{"USERNAME"},
			},
			&cli.StringFlag{
				Name:    "username-parameter",
				EnvVars: []string{"USERNAME_PARAMETER"},
			},
			&cli.StringFlag{
				Name:    "password",
				EnvVars: []string{"password"},
			},
			&cli.StringFlag{
				Name:    "password-parameter",
				EnvVars: []string{"PASSWORD_PARAMETER"},
			},
			&cli.BoolFlag{
				Name:    "managed-opensearch",
				EnvVars: []string{"MANAGED_OPENSEARCH"},
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

var Scopes = []string{"eventlog_read", "doc_read_all", "schema_read"}

type AuthStrategy string

const (
	Password          AuthStrategy = "password"
	ClientCredentials AuthStrategy = "client_credentials"
	MockJWT           AuthStrategy = "mock_jwt"
)

func parseAuthStrategy(str string) (AuthStrategy, error) {
	for _, s := range []AuthStrategy{Password, ClientCredentials, MockJWT} {
		if str == string(s) {
			return s, nil
		}
	}

	return "", fmt.Errorf("unknown auth strategy: %s", str)
}

func runIndexer(c *cli.Context) error {
	var (
		addr               = c.String("addr")
		paramSourceName    = c.String("parameter-source")
		profileAddr        = c.String("profile-addr")
		logLevel           = c.String("log-level")
		authStrategyStr    = c.String("auth-strategy")
		defaultLanguage    = c.String("default-language")
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

	jwksEndpoint, err := elephantine.ResolveParameter(
		c.Context, c, paramSource, "jwks-endpoint")
	if err != nil {
		return fmt.Errorf("resolve jwks endpoint parameter: %w", err)
	}

	tokenEndpoint, err := elephantine.ResolveParameter(
		c.Context, c, paramSource, "token-endpoint")
	if err != nil {
		return fmt.Errorf("resolve token endpoint parameter: %w", err)
	}

	authStrategy, err := parseAuthStrategy(authStrategyStr)
	if err != nil {
		return fmt.Errorf("resolve auth strategy parameter: %w", err)
	}

	var tokenSource oauth2.TokenSource

	switch authStrategy {
	case Password:
		clientID, err := elephantine.ResolveParameter(
			c.Context, c, paramSource, "client-id",
		)
		if err != nil {
			return fmt.Errorf("resolve client id parameter: %w", err)
		}

		clientSecret, err := elephantine.ResolveParameter(
			c.Context, c, paramSource, "client-secret",
		)
		if err != nil {
			return fmt.Errorf("resolve client secret parameter: %w", err)
		}

		username, err := elephantine.ResolveParameter(
			c.Context, c, paramSource, "username",
		)
		if err != nil {
			return fmt.Errorf("resolve username parameter: %w", err)
		}

		password, err := elephantine.ResolveParameter(
			c.Context, c, paramSource, "password",
		)
		if err != nil {
			return fmt.Errorf("resolve password parameter: %w", err)
		}

		authConf := oauth2.Config{
			Endpoint: oauth2.Endpoint{
				TokenURL: tokenEndpoint,
			},
			Scopes:       Scopes,
			ClientID:     clientID,
			ClientSecret: clientSecret,
		}

		token, err := authConf.PasswordCredentialsToken(c.Context, username, password)
		if err != nil {
			return fmt.Errorf("could not create password grant token: %w", err)
		}

		tokenSource = authConf.TokenSource(c.Context, token)

	case ClientCredentials:
		clientID, err := elephantine.ResolveParameter(
			c.Context, c, paramSource, "client-id",
		)
		if err != nil {
			return fmt.Errorf("resolve client id parameter: %w", err)
		}

		clientSecret, err := elephantine.ResolveParameter(
			c.Context, c, paramSource, "client-secret",
		)
		if err != nil {
			return fmt.Errorf("resolve client secret parameter: %w", err)
		}

		clientCredentialsConf := clientcredentials.Config{
			ClientID:     clientID,
			ClientSecret: clientSecret,
			TokenURL:     tokenEndpoint,
			Scopes:       Scopes,
		}

		tokenSource = clientCredentialsConf.TokenSource(c.Context)

	case MockJWT:
		sharedSecret, err := elephantine.ResolveParameter(
			c.Context, c, paramSource, "shared-secret")
		if err != nil {
			return fmt.Errorf("resolve shared secret parameter: %w", err)
		}

		authConf := oauth2.Config{
			Endpoint: oauth2.Endpoint{
				TokenURL: tokenEndpoint,
			},
			Scopes: Scopes,
		}

		pwToken, err := authConf.PasswordCredentialsToken(c.Context,
			"Indexer <system://indexer>", sharedSecret)
		if err != nil {
			return fmt.Errorf("get Elephant access token: %w", err)
		}

		tokenSource = authConf.TokenSource(c.Context, pwToken)
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

	authInfoParser, err := elephantine.NewJWKSAuthInfoParser(
		c.Context, jwksEndpoint, elephantine.AuthInfoParserOptions{})
	if err != nil {
		return fmt.Errorf("retrieve JWKS: %w", err)
	}

	authClient := oauth2.NewClient(c.Context, tokenSource)

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
		Documents:       documents,
		Validator:       loader,
		Metrics:         metrics,
		DefaultLanguage: defaultLanguage,
		NoIndexer:       noIndexer,
		AuthInfoParser:  authInfoParser,
	})
	if err != nil {
		return fmt.Errorf("run application: %w", err)
	}

	return nil
}
