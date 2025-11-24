package index

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ttab/elephant-api/index"
	"github.com/ttab/elephant-api/repository"
	"github.com/ttab/elephant-index/postgres"
	"github.com/ttab/elephantine"
	"github.com/twitchtv/twirp"
)

type Parameters struct {
	APIServer          *elephantine.APIServer
	Logger             *slog.Logger
	Database           *pgxpool.Pool
	DefaultCluster     *url.URL
	DefaultClusterAuth ClusterAuth
	Client             OpenSearchClientFunc
	Documents          repository.Documents
	AnonymousDocuments repository.Documents
	Validator          ValidatorSource
	Metrics            *Metrics
	Languages          LanguageOptions
	NoIndexer          bool
	AuthInfoParser     elephantine.AuthInfoParser
	Sharding           ShardingPolicy
	PasswordKey        [32]byte
}

func RunIndex(ctx context.Context, p Parameters) error {
	logger := p.Logger
	server := p.APIServer

	opts, err := elephantine.NewDefaultServiceOptions(
		p.Logger, p.AuthInfoParser, p.Metrics.Registerer,
		elephantine.ServiceAuthRequired)
	if err != nil {
		return fmt.Errorf("set up service config: %w", err)
	}

	percDocs := NewPercolatorDocCache(p.Database)

	coord, err := NewCoordinator(p.Database, CoordinatorOptions{
		Logger:          logger,
		Metrics:         p.Metrics,
		Languages:       p.Languages,
		ClientGetter:    p.Client,
		Documents:       p.Documents,
		Validator:       p.Validator,
		Sharding:        p.Sharding,
		NoIndexing:      p.NoIndexer,
		PercolatorCache: percDocs,
	})
	if err != nil {
		return fmt.Errorf("create coordinator: %w", err)
	}

	if !p.NoIndexer && p.DefaultCluster != nil {
		err = coord.EnsureDefaultIndexSet(ctx, p.DefaultCluster, p.DefaultClusterAuth)
		if err != nil {
			return fmt.Errorf("ensure default index set: %w", err)
		}
	}

	grace := elephantine.NewGracefulShutdown(logger, 10*time.Second)

	serverGroup := elephantine.NewErrGroup(grace.CancelOnQuit(ctx), logger)

	service, err := NewManagementService(logger, p.Database, p.PasswordKey)
	if err != nil {
		return fmt.Errorf("create management service: %w", err)
	}

	api := index.NewManagementServer(
		service,
		twirp.WithServerJSONSkipDefaults(true),
		twirp.WithServerHooks(opts.Hooks),
	)

	server.RegisterAPI(api, opts)

	searchAPI := index.NewSearchV1Server(
		NewSearchServiceV1(
			logger, p.Database,
			NewPostgresMappingSource(postgres.New(p.Database)),
			coord, p.AnonymousDocuments,
			coord.percolatorUpdate, coord.eventPercolated, percDocs,
		),
		twirp.WithServerJSONSkipDefaults(true),
		twirp.WithServerHooks(opts.Hooks),
	)

	server.RegisterAPI(searchAPI, opts)

	// TODO: retire the proxy.
	proxy := NewElasticProxy(logger, coord, p.AuthInfoParser)

	proxyHandler := elephantine.CORSMiddleware(elephantine.CORSOptions{
		AllowInsecure:          false,
		AllowInsecureLocalhost: true,
		Hosts:                  []string{"localhost", "tt.se"},
		AllowedMethods:         []string{"GET"},
		AllowedHeaders:         []string{"Authorization", "Content-Type"},
	}, proxy)

	server.Mux.Handle("/", proxyHandler)

	server.Health.AddReadyFunction("postgres", func(ctx context.Context) error {
		q := postgres.New(p.Database)

		_, err := q.ListIndexSets(ctx)
		if err != nil {
			return fmt.Errorf("failed to list index sets: %w", err)
		}

		return nil
	})

	server.Health.AddReadyFunction("opensearch", func(ctx context.Context) error {
		q := postgres.New(p.Database)

		active, err := q.GetActiveIndexSet(ctx)
		if err != nil {
			return fmt.Errorf("get active index set: %w", err)
		}

		client, err := p.Client(ctx, active.Cluster.String)
		if err != nil {
			return fmt.Errorf("gect cluster client: %w", err)
		}

		get := client.Indices.Get

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

	serverGroup.Go("coordinator", func(ctx context.Context) error {
		err := coord.Run(grace.CancelOnStop(ctx))
		if err != nil {
			return fmt.Errorf("coordinator error: %w", err)
		}

		return nil
	})

	serverGroup.Go("server", func(ctx context.Context) error {
		err := server.ListenAndServe(ctx)
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		} else if err != nil {
			return fmt.Errorf("run server: %w", err)
		}

		return nil
	})

	err = serverGroup.Wait()
	if err != nil {
		return fmt.Errorf("service failed to start: %w", err)
	}

	return nil
}
