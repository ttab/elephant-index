package index

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
)

type Metrics struct {
	indexerFailures *prometheus.CounterVec
	unknownEvents   *prometheus.CounterVec
	mappingUpdate   *prometheus.CounterVec
	indexedDocument *prometheus.CounterVec
	enrichErrors    *prometheus.CounterVec
	logPos          *prometheus.GaugeVec
}

func NewMetrics(reg prometheus.Registerer) (*Metrics, error) {
	indexerFailures := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "elephant_indexer_failures_total",
			Help: "Counts the number of times the indexer failed process a batch of events.",
		},
		[]string{"name"},
	)
	if err := reg.Register(indexerFailures); err != nil {
		return nil, fmt.Errorf("failed to register metric: %w", err)
	}

	unknownEvents := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "elephant_indexer_unknown_events_total",
			Help: "Unknown event types from the elephant event log.",
		},
		[]string{"type"},
	)
	if err := reg.Register(unknownEvents); err != nil {
		return nil, fmt.Errorf("failed to register metric: %w", err)
	}

	mappingUpdate := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "elephant_indexer_mapping_update_total",
			Help: "Number of times the index mapping has been updated.",
		},
		[]string{"index"},
	)
	if err := reg.Register(mappingUpdate); err != nil {
		return nil, fmt.Errorf("failed to register metric: %w", err)
	}

	indexedDocument := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "elephant_indexer_doc_total",
			Help: "Document indexing counter, tracks index and delete successes and failures.",
		},
		[]string{"type", "index", "result"},
	)
	if err := reg.Register(indexedDocument); err != nil {
		return nil, fmt.Errorf("failed to register metric: %w", err)
	}

	enrichErrors := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "elephant_indexer_enrich_errors_total",
			Help: "Document enricher errors.",
		},
		[]string{"type", "index"},
	)
	if err := reg.Register(enrichErrors); err != nil {
		return nil, fmt.Errorf("failed to register metric: %w", err)
	}

	logPos := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "elephant_indexer_log_position",
			Help: "Indexer eventlog position.",
		},
		[]string{"name"},
	)
	if err := reg.Register(logPos); err != nil {
		return nil, fmt.Errorf("failed to register metric: %w", err)
	}

	m := Metrics{
		indexerFailures: indexerFailures,
		unknownEvents:   unknownEvents,
		logPos:          logPos,
		mappingUpdate:   mappingUpdate,
		indexedDocument: indexedDocument,
		enrichErrors:    enrichErrors,
	}

	return &m, nil
}
