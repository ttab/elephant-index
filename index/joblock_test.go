package index_test

import (
	"errors"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/ttab/elephant-api/index"
	"github.com/ttab/elephant-api/repository/repositoryconnect"
	"github.com/ttab/elephantine/test"
)

// TestIndexerSurvivesLockLoss takes the indexer's job lock away from it, which
// is what a ping that times out after committing does, and requires the
// indexer to take the lock again and keep indexing. It used to stop for good,
// leaving the replica serving search with no indexer behind it.
func TestIndexerSurvivesLockLoss(t *testing.T) {
	ctx := t.Context()
	logger := slog.New(test.NewLogHandler(t, slog.LevelWarn))

	tc := testingAPIServer(t, logger)

	documents := repositoryconnect.NewDocumentsServiceClient(
		tc.AuthenticatedClient(t, "doc_read", "doc_write", "eventlog_read"),
		tc.Env.Repository.GetAPIEndpoint())

	search := tc.SearchClient(t, "doc_read", "search")

	lockName, holder := waitForIndexerLock(t, tc, "")

	// Index a document before the lock is lost, so that the indexer has
	// state from its first tenure to carry into the second: a worker for
	// the index the documents below share. A worker that outlived the lock
	// it was started under had no goroutines left to do its work, and the
	// first batch routed to it after the lock was re-acquired blocked for
	// good, with the lock held and pinged the whole time.
	const lionsUUID = "7bf0c4e7-aff7-4ddd-97b0-e4d5aca7d3eb"

	loadDocuments(t, documents,
		filepath.Join("..", "testdata", "documents"), "lions_v1.json")

	waitForIndexedDocument(t, search, lionsUUID,
		"before the lock was lost")

	// Delete the row out from under the holder: its next ping updates
	// nothing and it reads the lock as lost.
	_, err := tc.IndexDB.Exec(ctx,
		"DELETE FROM job_lock WHERE name = $1 AND holder = $2",
		lockName, holder)
	test.Mustf(t, err, "delete the indexer job lock")

	waitForIndexerLock(t, tc, holder)

	const russiaUUID = "f5d2e4c5-01ba-4dae-9f09-a86701e06ecd"

	loadDocuments(t, documents,
		filepath.Join("..", "testdata", "documents"), "russia_v1.json")

	waitForIndexedDocument(t, search, russiaUUID,
		"after the lock was re-acquired")

	// And once more, for the same index: a worker has to last for the
	// whole tenure and not just for the pass that created it.
	const ericssonUUID = "9b774de2-9d16-4b9d-91e1-a598ad675455"

	loadDocuments(t, documents,
		filepath.Join("..", "testdata", "documents"), "ericsson_v1.json")

	waitForIndexedDocument(t, search, ericssonUUID,
		"in a later pass of the same tenure")
}

// waitForIndexedDocument waits for a document to be readable from the index.
func waitForIndexedDocument(
	t *testing.T, search index.SearchV1,
	docUUID string, when string,
) {
	t.Helper()

	deadline := time.After(30 * time.Second)

	for {
		_, err := search.GetFlatDocument(t.Context(),
			&index.GetFlatDocumentRequest{
				Uuid:   docUUID,
				Stored: true,
			})
		if err == nil {
			return
		}

		select {
		case <-t.Context().Done():
			t.Fatal("cancelled while waiting for the document to be indexed")
		case <-deadline:
			t.Fatalf("document %s was not indexed %s, last error: %v",
				docUUID, when, err)
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// waitForIndexerLock waits for an indexer job lock to be held by someone other
// than notHolder, and returns the lock name and its holder.
func waitForIndexerLock(
	t *testing.T, tc TestContext, notHolder string,
) (string, string) {
	t.Helper()

	// Covers a ping interval for the loss to be noticed, and the
	// re-acquisition after it.
	deadline := time.After(45 * time.Second)

	for {
		var name, holder string

		err := tc.IndexDB.QueryRow(t.Context(), `
SELECT name, holder FROM job_lock
WHERE name LIKE 'indexer-%' AND holder != $1`,
			notHolder).Scan(&name, &holder)
		if err == nil {
			return name, holder
		}

		if !errors.Is(err, pgx.ErrNoRows) {
			test.Mustf(t, err, "read the indexer job lock")
		}

		select {
		case <-t.Context().Done():
			t.Fatal("cancelled while waiting for the indexer job lock")
		case <-deadline:
			t.Fatalf("no indexer job lock held by anyone but %q",
				notHolder)
		case <-time.After(200 * time.Millisecond):
		}
	}
}
