package index

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/opensearch-project/opensearch-go/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/ttab/elephant-index/postgres"
	"github.com/ttab/elephantine/test"
)

// unreachableCluster is the cluster name testCoordinator refuses to build a
// client for, so that a test can fail one index set and no other.
const unreachableCluster = "unreachable"

var errNoSuchCluster = errors.New("cluster is unreachable")

// testCoordinator builds the slice of a coordinator that the read-path switch
// and the reconciliation need: a logger, metrics, a client getter and the
// indexer map. Everything else is only touched once a database is involved.
func testCoordinator(t *testing.T) (*Coordinator, *[]string) {
	t.Helper()

	metrics, err := NewMetrics(prometheus.NewRegistry())
	test.Mustf(t, err, "create metrics")

	var clusters []string

	c := Coordinator{
		logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		indexers: make(map[string]*Indexer),
		opt: CoordinatorOptions{
			Metrics: metrics,
			ClientGetter: func(
				_ context.Context, cluster string,
			) (*opensearch.Client, error) {
				clusters = append(clusters, cluster)

				if cluster == unreachableCluster {
					return nil, errNoSuchCluster
				}

				return &opensearch.Client{}, nil
			},
		},
	}

	return &c, &clusters
}

func indexSet(name string, cluster string) postgres.IndexSet {
	return postgres.IndexSet{
		Name:    name,
		Active:  true,
		Enabled: true,
		Cluster: pgtype.Text{String: cluster, Valid: true},
	}
}

// stoppedIndexer is an indexer that has already finished, so that Stop returns
// without waiting out IndexerStopTimeout.
func stoppedIndexer(t *testing.T, name string) *Indexer {
	t.Helper()

	stopped := make(chan struct{})
	close(stopped)

	return &Indexer{
		logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
		name:    name,
		stop:    make(chan struct{}),
		stopped: stopped,
	}
}

// TestEnsureActiveClientClusterChange pins that the read-path switch keys off
// the cluster as well as the set name. A re-index into another cluster can
// reuse a set name, and reacting only to the name would leave searches going
// to the cluster the set is no longer in.
func TestEnsureActiveClientClusterChange(t *testing.T) {
	t.Parallel()

	c, clusters := testCoordinator(t)

	err := c.ensureActiveClient(indexSet("factual-tiger", "a"))
	test.Mustf(t, err, "set the initial active client")

	first, set := c.GetActiveIndex()
	if set != "factual-tiger" {
		t.Fatalf("got active set %q, wanted %q", set, "factual-tiger")
	}

	// Same set, same cluster: nothing to do, and no new client.
	err = c.ensureActiveClient(indexSet("factual-tiger", "a"))
	test.Mustf(t, err, "re-assert the active client")

	again, _ := c.GetActiveIndex()
	if again != first {
		t.Error("the client was rebuilt for an unchanged set and cluster")
	}

	// Same set, different cluster: the client has to follow the set.
	err = c.ensureActiveClient(indexSet("factual-tiger", "b"))
	test.Mustf(t, err, "move the active set to another cluster")

	moved, set := c.GetActiveIndex()
	if set != "factual-tiger" {
		t.Fatalf("got active set %q, wanted %q", set, "factual-tiger")
	}

	if moved == first {
		t.Error("the client was not rebuilt for a cluster change")
	}

	if got, want := len(*clusters), 2; got != want {
		t.Errorf("built %d clients, wanted %d: %v", got, want, *clusters)
	}

	gauge := testutil.ToFloat64(
		c.opt.Metrics.activeIndexSet.WithLabelValues("factual-tiger", "b"))
	if gauge != 1 {
		t.Errorf("got active index set gauge %v, wanted 1", gauge)
	}

	// The gauge is reset on a switch, so only the current set is reported.
	if got := testutil.CollectAndCount(
		c.opt.Metrics.activeIndexSet); got != 1 {
		t.Errorf("got %d active index set series, wanted 1", got)
	}
}

// TestApplyIndexSetsContinuesPastAFailure is the important one: sets are
// independent, and a set whose cluster cannot be reached must not stop the
// reconciliation from resolving the active set or from stopping the indexer of
// a set that has been deleted. Aborting the sweep on the first failure would
// leave the replica serving the old index set, which is the drift the
// reconciliation exists to prevent.
func TestApplyIndexSetsContinuesPastAFailure(t *testing.T) {
	t.Parallel()

	c, _ := testCoordinator(t)

	c.indexers["gone"] = stoppedIndexer(t, "gone")

	broken := indexSet("broken-walrus", unreachableCluster)
	broken.Enabled = false

	healthy := indexSet("vivid-otter", "a")
	healthy.Enabled = false

	err := c.applyIndexSets(t.Context(), []postgres.IndexSet{broken, healthy})
	if err == nil {
		t.Fatal("the unreachable cluster was not reported")
	}

	if !errors.Is(err, errNoSuchCluster) {
		t.Errorf("got error %v, wanted it to carry the cluster failure", err)
	}

	_, set := c.GetActiveIndex()
	if set != "vivid-otter" {
		t.Errorf("got active set %q, wanted the sweep to have reached %q",
			set, "vivid-otter")
	}

	if _, ok := c.indexers["gone"]; ok {
		t.Error("the indexer of a removed set was not stopped")
	}
}

// TestApplyIndexSetsKeepsServingWithoutAnActiveSet pins that a database with
// no active set leaves the read path alone. Dropping the client would make
// GetActiveIndex return nil, which the percolator's goroutines dereference
// without a nil check and without a recover.
func TestApplyIndexSetsKeepsServingWithoutAnActiveSet(t *testing.T) {
	t.Parallel()

	c, _ := testCoordinator(t)

	err := c.ensureActiveClient(indexSet("vivid-otter", "a"))
	test.Mustf(t, err, "set the initial active client")

	inactive := indexSet("vivid-otter", "a")
	inactive.Active = false
	inactive.Enabled = false

	err = c.applyIndexSets(t.Context(), []postgres.IndexSet{inactive})
	test.Mustf(t, err, "apply index sets")

	client, set := c.GetActiveIndex()
	if client == nil || set != "vivid-otter" {
		t.Errorf("got active index (%v, %q), wanted it left in place",
			client, set)
	}

	if !c.activeMissing {
		t.Error("the missing active set was not noted")
	}

	// And the note is cleared again once a set is active, so the warning is
	// logged per transition rather than per pass.
	reactivated := indexSet("vivid-otter", "a")
	reactivated.Enabled = false

	err = c.applyIndexSets(t.Context(), []postgres.IndexSet{reactivated})
	test.Mustf(t, err, "apply index sets")

	if c.activeMissing {
		t.Error("the missing active set was not cleared")
	}
}

// TestRecordSetSync pins the counter that says whether a replica is keeping
// up with the index sets at all. The successes carry as much as the failures:
// a tick rate that has gone to zero on one replica is a stalled event loop,
// and nothing else reports that.
func TestRecordSetSync(t *testing.T) {
	t.Parallel()

	c, _ := testCoordinator(t)

	c.recordSetSync(syncTriggerTick, nil)
	c.recordSetSync(syncTriggerTick, nil)
	c.recordSetSync(syncTriggerTick, errNoSuchCluster)
	c.recordSetSync(syncTriggerNotification, errNoSuchCluster)

	for _, tc := range []struct {
		trigger string
		result  string
		want    float64
	}{
		{syncTriggerTick, "ok", 2},
		{syncTriggerTick, "failed", 1},
		{syncTriggerNotification, "failed", 1},
	} {
		got := testutil.ToFloat64(c.opt.Metrics.indexSetSync.
			WithLabelValues(tc.trigger, tc.result))
		if got != tc.want {
			t.Errorf("got %v for {trigger=%q,result=%q}, wanted %v",
				got, tc.trigger, tc.result, tc.want)
		}
	}
}

// TestRequestReconcileCoalesces pins that a reconciliation request never
// blocks its caller. The subscriber calls it from its reconnect callback, and
// an error or a block there tears the subscriber down.
func TestRequestReconcileCoalesces(t *testing.T) {
	t.Parallel()

	c := Coordinator{reconcile: make(chan struct{}, 1)}

	for range 5 {
		c.requestReconcile()
	}

	select {
	case <-c.reconcile:
	default:
		t.Fatal("no reconciliation was requested")
	}

	select {
	case <-c.reconcile:
		t.Fatal("the requests did not coalesce")
	default:
	}
}
