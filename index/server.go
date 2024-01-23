package index

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/opensearch-project/opensearch-go/v2"
	"github.com/ttab/elephant-api/repository"
	"github.com/ttab/elephant-index/postgres"
	"github.com/ttab/elephantine"
	"golang.org/x/sync/errgroup"
)

type IndexParameters struct {
	Addr            string
	ProfileAddr     string
	Logger          *slog.Logger
	DefaultSetName  string
	Database        *pgxpool.Pool
	Client          *opensearch.Client
	Documents       repository.Documents
	Validator       ValidatorSource
	Metrics         *Metrics
	DefaultLanguage string
	NoIndexer       bool
	PublicJWTKey    *ecdsa.PublicKey
}

func RunIndex(ctx context.Context, p IndexParameters) error {
	logger := p.Logger

	indexer, err := NewIndexer(ctx, IndexerOptions{
		Logger: logger.With(
			elephantine.LogKeyComponent, "indexer"),
		Metrics:         p.Metrics,
		SetName:         p.DefaultSetName,
		DefaultLanguage: p.DefaultLanguage,
		Client:          p.Client,
		Database:        p.Database,
		Documents:       p.Documents,
		Validator:       p.Validator,
	})
	if err != nil {
		return fmt.Errorf("create indexer: %w", err)
	}

	healthServer := elephantine.NewHealthServer(logger, p.ProfileAddr)
	router := http.NewServeMux()
	serverGroup, gCtx := errgroup.WithContext(ctx)

	// TODO: make the proxy aware of clusters and index set names.
	proxy := NewElasticProxy(logger, p.Client, p.PublicJWTKey)

	proxyHandler := elephantine.CORSMiddleware(elephantine.CORSOptions{
		AllowInsecure:          false,
		AllowInsecureLocalhost: true,
		Hosts:                  []string{"localhost", "tt.se"},
		AllowedMethods:         []string{"GET"},
		AllowedHeaders:         []string{"Authorization", "Content-Type"},
	}, proxy)

	router.Handle("/", proxyHandler)

	router.Handle("/health/alive", http.HandlerFunc(func(
		w http.ResponseWriter, req *http.Request,
	) {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)

		_, _ = fmt.Fprintln(w, "I AM ALIVE!")
	}))

	aliveEndpoint := fmt.Sprintf(
		"http://localhost%s/health/alive",
		p.Addr,
	)

	healthServer.AddReadyFunction("api_liveness",
		elephantine.LivenessReadyCheck(aliveEndpoint))

	healthServer.AddReadyFunction("postgres", func(ctx context.Context) error {
		q := postgres.New(p.Database)

		_, err := q.ListIndexSets(ctx)
		if err != nil {
			return fmt.Errorf("failed to list index sets: %w", err)
		}

		return nil
	})

	healthServer.AddReadyFunction("opensearch", func(ctx context.Context) error {
		get := p.Client.Indices.Get

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
		if p.NoIndexer {
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
			Addr:              p.Addr,
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
