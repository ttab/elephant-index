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
	"github.com/ttab/elephant-api/index"
	"github.com/ttab/elephant-api/repository"
	"github.com/ttab/elephant-index/internal"
	"github.com/ttab/elephant-index/postgres"
	"github.com/ttab/elephantine"
	"github.com/twitchtv/twirp"
	"golang.org/x/sync/errgroup"
)

type IndexParameters struct {
	Addr            string
	ProfileAddr     string
	Logger          *slog.Logger
	Database        *pgxpool.Pool
	DefaultCluster  string
	ClusterAuth     ClusterAuth
	Client          OpenSearchClientFunc
	Documents       repository.Documents
	Validator       ValidatorSource
	Metrics         *Metrics
	DefaultLanguage string
	NoIndexer       bool
	PublicJWTKey    *ecdsa.PublicKey
}

func RunIndex(ctx context.Context, p IndexParameters) error {
	logger := p.Logger

	coord, err := NewCoordinator(ctx, p.Database, CoordinatorOptions{
		Logger:          logger,
		Metrics:         p.Metrics,
		DefaultLanguage: p.DefaultLanguage,
		ClientGetter:    p.Client,
		Documents:       p.Documents,
		Validator:       p.Validator,
	})

	err = coord.EnsureDefaultIndexSet(ctx, p.DefaultCluster, p.ClusterAuth)
	if err != nil {
		return fmt.Errorf("ensure default index set: %w", err)
	}

	grace := elephantine.NewGracefulShutdown(logger, 10*time.Second)
	ctx = grace.CancelOnStop(ctx)

	healthServer := elephantine.NewHealthServer(logger, p.ProfileAddr)
	router := http.NewServeMux()
	serverGroup, gCtx := errgroup.WithContext(ctx)

	var opts ServerOptions

	opts.SetJWTValidation(p.PublicJWTKey)

	service, err := NewManagementService(logger, p.Database)
	if err != nil {
		return fmt.Errorf("create management service: %w", err)
	}

	api := index.NewManagementServer(
		service,
		twirp.WithServerJSONSkipDefaults(true),
		twirp.WithServerHooks(opts.Hooks),
	)

	registerAPI(router, opts, api)

	proxy := NewElasticProxy(logger, coord, p.PublicJWTKey)

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

	serverGroup.Go(func() error {
		if p.NoIndexer {
			return nil
		}

		err := coord.Run(gCtx)
		if err != nil {
			return fmt.Errorf("coordinator error: %w", err)
		}

		return errors.New("coordinator stopped")
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

		err := elephantine.ListenAndServeContext(
			gCtx, &server, 10*time.Second,
		)
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

func ListenAndServe(ctx context.Context, addr string, h http.Handler) error {
	var handler http.HandlerFunc = func(w http.ResponseWriter, r *http.Request) {
		ctx := elephantine.WithLogMetadata(r.Context())

		h.ServeHTTP(w, r.WithContext(ctx))
	}

	server := http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
	}

	//nolint:wrapcheck
	return elephantine.ListenAndServeContext(ctx, &server, 10*time.Second)
}

type ServerOptions struct {
	Hooks          *twirp.ServerHooks
	AuthMiddleware func(
		w http.ResponseWriter, r *http.Request, next http.Handler,
	) error
}

func (so *ServerOptions) SetJWTValidation(jwtKey *ecdsa.PublicKey) {
	// TODO: This feels like an initial sketch that should be further
	// developed to address the JWT cacheing.
	so.AuthMiddleware = func(
		w http.ResponseWriter, r *http.Request, next http.Handler,
	) error {
		auth, err := elephantine.AuthInfoFromHeader(jwtKey,
			r.Header.Get("Authorization"))
		if err != nil && !errors.Is(err, elephantine.ErrNoAuthorization) {
			// TODO: Move the response part to a hook instead?
			return elephantine.HTTPErrorf(http.StatusUnauthorized,
				"invalid authorization method: %v", err)
		}

		if auth != nil {
			ctx := elephantine.SetAuthInfo(r.Context(), auth)

			elephantine.SetLogMetadata(ctx,
				elephantine.LogKeySubject, auth.Claims.Subject,
			)

			r = r.WithContext(ctx)
		}

		next.ServeHTTP(w, r)

		return nil
	}
}

type apiServerForRouter interface {
	http.Handler

	PathPrefix() string
}

func registerAPI(
	router *http.ServeMux, opt ServerOptions,
	api apiServerForRouter,
) {
	router.Handle(api.PathPrefix(), internal.RHandleFunc(func(
		w http.ResponseWriter, r *http.Request,
	) error {
		if r.Method != http.MethodPost {
			return elephantine.HTTPErrorf(
				http.StatusMethodNotAllowed,
				"the API only accepts POST requests")
		}

		if opt.AuthMiddleware != nil {
			return opt.AuthMiddleware(w, r, api)
		}

		api.ServeHTTP(w, r)

		return nil
	}))
}
