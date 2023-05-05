package elephantine

import (
	"context"
	"encoding/json"
	"expvar"
	"fmt"
	"net/http"
	"net/http/pprof" //nolint:gosec
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type ReadyFunc func(ctx context.Context) error

// HealthServer exposes health endpoints, metrics, and PPROF endpoints.
type HealthServer struct {
	server         *http.Server
	readyFunctions map[string]ReadyFunc
}

func NewHealthServer(addr string) *HealthServer {
	mux := http.NewServeMux()

	server := http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 1 * time.Second,
	}

	s := HealthServer{
		server:         &server,
		readyFunctions: make(map[string]ReadyFunc),
	}

	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	mux.Handle("/debug/vars", expvar.Handler())
	mux.Handle("/metrics", promhttp.Handler())
	mux.Handle("/health/ready", http.HandlerFunc(s.readyHandler))

	return &s
}

type ReadyResult struct {
	Ok    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
}

func (s *HealthServer) readyHandler(
	w http.ResponseWriter, req *http.Request,
) {
	var failed bool

	result := make(map[string]ReadyResult)

	for name, fn := range s.readyFunctions {
		err := fn(req.Context())
		if err != nil {
			failed = true

			result[name] = ReadyResult{
				Ok:    false,
				Error: err.Error(),
			}

			continue
		}

		result[name] = ReadyResult{Ok: true}
	}

	w.Header().Set("Content-Type", "application/json")

	if failed {
		w.WriteHeader(http.StatusInternalServerError)
	}

	enc := json.NewEncoder(w)

	// Making health endpoints human-readable is always a nice touch.
	enc.SetIndent("", "  ")

	_ = enc.Encode(result)
}

func (s *HealthServer) AddReadyFunction(name string, fn ReadyFunc) {
	s.readyFunctions[name] = fn
}

func (s *HealthServer) Close() error {
	err := s.server.Close()
	if err != nil {
		return fmt.Errorf("failed to close http server: %w", err)
	}

	return nil
}

func (s *HealthServer) ListenAndServe(ctx context.Context) error {
	return ListenAndServeContext(ctx, s.server)
}
