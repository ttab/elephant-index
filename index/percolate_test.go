package index_test

import (
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ttab/elephant-api/index"
	"github.com/ttab/elephant-api/newsdoc"
	"github.com/ttab/elephant-api/repository"
	"github.com/ttab/elephant-index/postgres"
	"github.com/ttab/elephantine/test"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func TestPercolate(t *testing.T) {
	ctx := t.Context()
	logger := slog.New(test.NewLogHandler(t, slog.LevelWarn))

	tc := testingAPIServer(t, logger)

	documents := repository.NewDocumentsProtobufClient(
		tc.Env.Repository.GetAPIEndpoint(),
		tc.AuthenticatedClient(t, "doc_read", "doc_write", "eventlog_read"))

	search := index.NewSearchV1ProtobufClient(tc.IndexEndpoint,
		tc.AuthenticatedClient(t, "doc_read", "search"))

	testDataDir := filepath.Join("..", "testdata", t.Name())
	docDataDir := filepath.Join("..", "testdata", "documents")

	// Seed the repo with documents that correspond to the types that we
	// want indexes for.
	loadDocuments(t, documents, docDataDir,
		"russia_v1.json", "cyber_v1.json")

	deadline := time.After(3 * time.Second)

	var found bool

	for !found {
		select {
		case <-ctx.Done():
			t.Fatal("cancelled while waiting for documents to become searchable")
		case <-deadline:
			t.Fatal("timed out waiting for documents to become searchable")
		case <-time.After(200 * time.Millisecond):
		}

		res, err := search.Query(ctx, &index.QueryRequestV1{
			DocumentType: "core/planning-item",
			Language:     "sv-se",
			Fields: []string{
				"document.title",
				"document.meta.core_planning_item.data.start_date",
			},
			Query: index.RangeQuery(&index.RangeQueryV1{
				Field: "document.meta.core_planning_item.data.start_date",
				Gte:   "2025-08-29T00:00:00.000Z",
				Lte:   "2025-08-29T23:59:59.999Z",
			}),
		})
		test.Must(t, err, "perform search")

		if len(res.Hits.Hits) == 0 {
			continue
		}

		test.TestMessageAgainstGolden(t, regenerateTestFixtures(), res.Hits,
			filepath.Join(testDataDir, "initial-result.json"))

		break
	}

	// Get latest event from repo.
	evtLogRes, err := documents.Eventlog(ctx, &repository.GetEventlogRequest{
		After: -1,
	})
	test.Must(t, err, "get last event from repo")

	test.Equal(t, 1, len(evtLogRes.Items), "get one event from the repo")

	lastEvent := evtLogRes.Items[0].Id

	// Accessing internal state, but it's just to get a consistent starting
	// point from which we start the actual percolation test.
	q := postgres.New(tc.IndexDB)

	pSyncDeadline := time.After(3 * time.Second)

	for {
		select {
		case <-pSyncDeadline:
			t.Fatal("timed out waiting for percolator to catch up")
		case <-time.After(100 * time.Millisecond):
		}

		pEvt, err := q.GetLastPercolatorEventID(ctx)
		test.Must(t, err, "get last percolator event ID")

		if pEvt == lastEvent {
			break
		}
	}

	qRes, err := search.Query(ctx, &index.QueryRequestV1{
		DocumentType: "core/planning-item",
		Language:     "sv-se",
		Subscribe:    true,
		Fields:       []string{"document.title", "current_version"},
		Query: index.RangeQuery(&index.RangeQueryV1{
			Field: "document.meta.core_planning_item.data.start_date",
			Gte:   "2025-09-01T00:00:00.000Z",
			Lte:   "2025-09-01T23:59:59.999Z",
		}),
	})
	test.Must(t, err, "do initial subscription search")

	t.Logf("initial subscription position: %d", qRes.Subscription.Cursor)

	test.Equal(t, 0, len(qRes.Hits.Hits), "no initial hits expected")

	subPos := qRes.Subscription
	gotBatch := make(chan struct{}, 1)

	defer close(gotBatch)

	go func() {
		// Just make sure that we have our percolator registered
		// properly before we start writing.
		time.Sleep(100 * time.Millisecond)

		// Using the ericsson article as a batch marker.
		loadDocuments(t, documents, docDataDir,
			"ericsson_v1.json",
		)

		<-gotBatch

		loadDocuments(t, documents, docDataDir,
			"russia_v2.json",
			"lions_v1.json",
			"ericsson_v1.json",
		)

		<-gotBatch

		loadDocuments(t, documents, docDataDir,
			"lions_v2.json",
			"ericsson_v1.json",
		)

		// Always drain got batch until its closed.
		for range gotBatch {
		}
	}()

	items := index.SubscriptionPollResult{
		Subscription: subPos,
	}

	pollDeadline := time.Now().Add(10 * time.Second)

	var batchCount int

	for time.Until(pollDeadline) > 0 {
		pollRes, err := search.PollSubscription(ctx,
			&index.PollSubscriptionRequest{
				Subscriptions: []*index.SubscriptionReference{
					subPos,
				},
				MaxWaitMs: 3000,
			})
		test.Must(t, err, "poll subscription from %d", subPos.Cursor)

		for _, sub := range pollRes.Result {
			if subPos.Id != qRes.Subscription.Id {
				t.Fatal("unexpected subscription ID")
			}

			for _, it := range sub.Items {
				if it.Id == "9b774de2-9d16-4b9d-91e1-a598ad675455" {
					batchCount++

					gotBatch <- struct{}{}
				}
			}

			items.Items = append(items.Items, sub.Items...)

			subPos.Cursor = sub.Subscription.Cursor
		}

		if batchCount == 3 {
			break
		}
	}

	test.TestMessageAgainstGolden(t, regenerateTestFixtures(), &items,
		filepath.Join(testDataDir, "poll-result.json"))
}

func loadDocuments(
	t *testing.T,
	documents repository.Documents,
	dir string,
	names ...string,
) {
	t.Helper()

	for _, name := range names {
		var doc newsdoc.Document

		unmarshalMessage(t, filepath.Join(dir, name), &doc)

		_, err := documents.Update(t.Context(), &repository.UpdateRequest{
			Uuid:     doc.Uuid,
			Document: &doc,
		})
		test.Must(t, err, "write document %q (%s)", name, doc.Uuid)
	}
}

func unmarshalMessage(t *testing.T, path string, msg proto.Message) {
	t.Helper()

	data, err := os.ReadFile(path)
	test.Must(t, err, "read proto json file")

	err = protojson.Unmarshal(data, msg)
	test.Must(t, err, "unmarshal %q proto json file", path)
}
