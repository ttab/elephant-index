// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package main

import (
	"errors"
	"fmt"
	"net/http"
	"os"
	"runtime/debug"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/julienschmidt/httprouter"
	"github.com/ttab/elephant-index/index"
	"github.com/ttab/elephant/rpc/repository"
	"github.com/ttab/elephantine"
	"github.com/urfave/cli/v2"
	"golang.org/x/exp/slog"
	"golang.org/x/oauth2"
	"golang.org/x/sync/errgroup"
)

func main() {
	runCmd := cli.Command{
		Name:        "run",
		Description: "Runs the indexing serve",
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
				Value:   "error",
			},
			&cli.StringFlag{
				Name:     "jwks-endpoint",
				EnvVars:  []string{"JWKS_ENDPOINT"},
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
		repositoryEndpoint = c.String("repository-endpoint")
	)

	logger := elephantine.SetUpLogger(logLevel, os.Stdout)

	defer func() {
		if p := recover(); p != nil {
			slog.ErrorCtx(c.Context, "panic during setup",
				elephantine.LogKeyError, p,
				"stack", string(debug.Stack()),
			)

			os.Exit(2)
		}
	}()

	paramSource, err := elephantine.GetParameterSource(paramSourceName)
	if err != nil {
		return fmt.Errorf("get parameter source: %w", err)
	}

	connString, err := elephantine.ResolveParameter(
		c, paramSource, "db")
	if err != nil {
		return fmt.Errorf("resolve db parameter: %w", err)
	}

	sharedSecret, err := elephantine.ResolveParameter(
		c, paramSource, "shared-secret")
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

	authConf := oauth2.Config{
		Endpoint: oauth2.Endpoint{
			TokenURL: tokenEndpoint,
		},
		Scopes: []string{"doc_read", "superuser"},
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
	router := httprouter.New()
	serverGroup, gCtx := errgroup.WithContext(c.Context)

	indexer, err := index.NewIndexer(c.Context, index.IndexerOptions{
		Logger: logger.With(
			elephantine.LogKeyComponent, "indexer"),
		SetName:   "v1",
		Database:  dbpool,
		Documents: documents,
		Validator: loader,
	})
	if err != nil {
		return fmt.Errorf("create indexer: %w", err)
	}

	serverGroup.Go(func() error {
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
