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

	lockName, holder := waitForIndexerLock(t, tc, "")

	// Delete the row out from under the holder: its next ping updates
	// nothing and it reads the lock as lost.
	_, err := tc.IndexDB.Exec(ctx,
		"DELETE FROM job_lock WHERE name = $1 AND holder = $2",
		lockName, holder)
	test.Mustf(t, err, "delete the indexer job lock")

	waitForIndexerLock(t, tc, holder)

	documents := repositoryconnect.NewDocumentsServiceClient(
		tc.AuthenticatedClient(t, "doc_read", "doc_write", "eventlog_read"),
		tc.Env.Repository.GetAPIEndpoint())

	search := tc.SearchClient(t, "doc_read", "search")

	loadDocuments(t, documents,
		filepath.Join("..", "testdata", "documents"), "russia_v1.json")

	const russiaUUID = "f5d2e4c5-01ba-4dae-9f09-a86701e06ecd"

	deadline := time.After(30 * time.Second)

	for {
		_, err = search.GetFlatDocument(ctx, &index.GetFlatDocumentRequest{
			Uuid:   russiaUUID,
			Stored: true,
		})
		if err == nil {
			return
		}

		select {
		case <-ctx.Done():
			t.Fatal("cancelled while waiting for the document to be indexed")
		case <-deadline:
			t.Fatalf("the document was not indexed after the lock was"+
				" re-acquired, last error: %v", err)
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
