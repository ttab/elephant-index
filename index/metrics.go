package index

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/ttab/koonkie"
)

type Metrics struct {
	indexerFailures  *prometheus.CounterVec
	unknownEvents    *prometheus.CounterVec
	mappingUpdate    *prometheus.CounterVec
	ignoredMapping   *prometheus.CounterVec
	percolationEvent *prometheus.CounterVec
	percolatorPos    prometheus.Gauge
	indexedDocument  *prometheus.CounterVec
	enrichErrors     *prometheus.CounterVec
	logPos           *koonkie.PrometheusFollowerMetrics

	Registerer prometheus.Registerer
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
		return nil, fmt.Errorf("register indexer failure metric: %w", err)
	}

	unknownEvents := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "elephant_indexer_unknown_events_total",
			Help: "Unknown event types from the elephant event log.",
		},
		[]string{"type"},
	)
	if err := reg.Register(unknownEvents); err != nil {
		return nil, fmt.Errorf("register unknown events metric: %w", err)
	}

	mappingUpdate := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "elephant_indexer_mapping_update_total",
			Help: "Number of times the index mapping has been updated.",
		},
		[]string{"index"},
	)
	if err := reg.Register(mappingUpdate); err != nil {
		return nil, fmt.Errorf("register mapping updates metric: %w", err)
	}

	ignoredMapping := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "elephant_indexer_ignored_mapping_total",
			Help: "Number of times we have ignored a mapping change.",
		},
		[]string{"index", "property"},
	)
	if err := reg.Register(ignoredMapping); err != nil {
		return nil, fmt.Errorf("register ignored mappings metric: %w", err)
	}

	percolationEvent := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "elephant_indexer_percolation_total",
			Help: "Percolation events.",
		},
		[]string{"event", "location"},
	)
	if err := reg.Register(percolationEvent); err != nil {
		return nil, fmt.Errorf("register ignored mappings metric: %w", err)
	}

	percolatorPosition := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "elephant_indexer_percolator_position",
		Help: "The position last processed by the percolator",
	})

	indexedDocument := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "elephant_indexer_doc_total",
			Help: "Document indexing counter, tracks index and delete successes and failures.",
		},
		[]string{"type", "index", "result"},
	)
	if err := reg.Register(indexedDocument); err != nil {
		return nil, fmt.Errorf("register doc count metric: %w", err)
	}

	enrichErrors := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "elephant_indexer_enrich_errors_total",
			Help: "Document enricher errors.",
		},
		[]string{"type", "index"},
	)
	if err := reg.Register(enrichErrors); err != nil {
		return nil, fmt.Errorf("register enrich errors metric: %w", err)
	}

	logPos, err := koonkie.NewPrometheusFollowerMetrics(reg, "elephant-index")
	if err != nil {
		return nil, fmt.Errorf("register follower metrics: %w", err)
	}

	m := Metrics{
		indexerFailures:  indexerFailures,
		unknownEvents:    unknownEvents,
		logPos:           logPos,
		mappingUpdate:    mappingUpdate,
		ignoredMapping:   ignoredMapping,
		percolationEvent: percolationEvent,
		percolatorPos:    percolatorPosition,
		indexedDocument:  indexedDocument,
		enrichErrors:     enrichErrors,
		Registerer:       reg,
	}

	return &m, nil
}
