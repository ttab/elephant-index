package index

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/ttab/koonkie"
)

// labelSetName is the metric label that holds an index set name.
const labelSetName = "set_name"

type Metrics struct {
	indexerFailures  *prometheus.CounterVec
	unknownEvents    *prometheus.CounterVec
	mappingUpdate    *prometheus.CounterVec
	ignoredMapping   *prometheus.CounterVec
	percolationEvent *prometheus.CounterVec
	percolatorPos    prometheus.Gauge
	percolatorLife   *prometheus.CounterVec
	indexedDocument  *prometheus.CounterVec
	enrichErrors     *prometheus.CounterVec
	activeIndexSet   *prometheus.GaugeVec
	indexSetSync     *prometheus.CounterVec
	indexSetLag      *prometheus.GaugeVec
	indexSetPosition *prometheus.GaugeVec
	eventlogHead     prometheus.Gauge
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

	percolatorLifecycle := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "elephant_indexer_percolator_lifecycle_total",
			Help: "Percolation lifecycle events.",
		},
		[]string{"event"},
	)
	if err := reg.Register(percolatorLifecycle); err != nil {
		return nil, fmt.Errorf("register percolator lifecycle metric: %w", err)
	}

	percolationEvent := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "elephant_indexer_percolation_total",
			Help: "Percolation events.",
		},
		[]string{"event", "location"},
	)
	if err := reg.Register(percolationEvent); err != nil {
		return nil, fmt.Errorf("register percolation event metric: %w", err)
	}

	percolatorPosition := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "elephant_indexer_percolator_position",
		Help: "The position last processed by the percolator",
	})
	if err := reg.Register(percolatorPosition); err != nil {
		return nil, fmt.Errorf("register percolator position metric: %w", err)
	}

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

	activeIndexSet := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "elephant_indexer_active_index_set",
			Help: "The index set this replica currently answers searches from, as a label.",
		},
		[]string{labelSetName, "cluster"},
	)
	if err := reg.Register(activeIndexSet); err != nil {
		return nil, fmt.Errorf("register active index set metric: %w", err)
	}

	indexSetSync := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "elephant_indexer_index_set_sync_total",
			Help: "Attempts to bring a replica in line with the index sets in the database, by trigger and outcome.",
		},
		[]string{"trigger", "result"},
	)
	if err := reg.Register(indexSetSync); err != nil {
		return nil, fmt.Errorf("register index set sync metric: %w", err)
	}

	indexSetLag := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "elephant_indexer_index_set_lag",
			Help: "How many events an enabled index set is behind the head of the event log, " +
				"as read from the database by every replica.",
		},
		[]string{labelSetName},
	)
	if err := reg.Register(indexSetLag); err != nil {
		return nil, fmt.Errorf("register index set lag metric: %w", err)
	}

	indexSetPosition := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "elephant_indexer_index_set_position",
			Help: "The event log position persisted for an enabled index set, " +
				"as read from the database by every replica.",
		},
		[]string{labelSetName},
	)
	if err := reg.Register(indexSetPosition); err != nil {
		return nil, fmt.Errorf("register index set position metric: %w", err)
	}

	eventlogHead := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "elephant_indexer_eventlog_head",
		Help: "The newest event ID in the repository event log, as last read by this replica.",
	})
	if err := reg.Register(eventlogHead); err != nil {
		return nil, fmt.Errorf("register eventlog head metric: %w", err)
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
		percolatorLife:   percolatorLifecycle,
		indexedDocument:  indexedDocument,
		enrichErrors:     enrichErrors,
		activeIndexSet:   activeIndexSet,
		indexSetSync:     indexSetSync,
		indexSetLag:      indexSetLag,
		indexSetPosition: indexSetPosition,
		eventlogHead:     eventlogHead,
		Registerer:       reg,
	}

	return &m, nil
}
