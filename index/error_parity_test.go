package index_test

import (
	"log/slog"
	"testing"

	"connectrpc.com/connect"
	"github.com/ttab/elephant-api/index"
	"github.com/ttab/elephantine/test"
)

// TestErrorParity performs the same failing call on both stacks and asserts
// that the code, the message and the error metadata come out the same. That is
// what makes the move to the rpc error vocabulary checkable: the handlers now
// return Connect errors, a Twirp caller is answered through
// rpc.TwirpInterceptor, and nothing but this says the translation preserves
// what a client reads.
//
// One server serves both stacks, so the calls differ only in the client.
func TestErrorParity(t *testing.T) {
	ctx := t.Context()
	logger := slog.New(test.NewLogHandler(t, slog.LevelWarn))

	tc := testingAPIServer(t, logger)

	searchTwirp := tc.SearchClientOn(t, stackTwirp, "doc_read", "search")
	searchConnect := tc.SearchClientOn(t, stackConnect, "doc_read", "search")

	adminTwirp := tc.ManagementClientOn(t, stackTwirp, "index_admin")
	adminConnect := tc.ManagementClientOn(t, stackConnect, "index_admin")

	// A caller that is identified but holds none of the scopes the method
	// accepts.
	deniedTwirp := tc.ManagementClientOn(t, stackTwirp, "doc_read")
	deniedConnect := tc.ManagementClientOn(t, stackConnect, "doc_read")

	// The active set, so that the failed precondition below has something
	// real to refuse.
	sets, err := adminTwirp.ListIndexSets(ctx, &index.ListIndexSetsRequest{
		OnlyActive: true,
	})
	test.Mustf(t, err, "list the active index set")
	test.Equalf(t, 1, len(sets.IndexSets), "exactly one active index set")

	activeSet := sets.IndexSets[0].Name

	t.Run("required_argument_carries_argument_meta", func(t *testing.T) {
		_, twirpErr := searchTwirp.GetFlatDocument(ctx,
			&index.GetFlatDocumentRequest{})
		_, connectErr := searchConnect.GetFlatDocument(ctx,
			&index.GetFlatDocumentRequest{})

		test.IsRPCError(t, connectErr, connect.CodeInvalidArgument)
		test.ErrorParity(t, twirpErr, connectErr)
	})

	t.Run("required_argument_on_management", func(t *testing.T) {
		_, twirpErr := adminTwirp.DeleteCluster(ctx,
			&index.DeleteClusterRequest{})
		_, connectErr := adminConnect.DeleteCluster(ctx,
			&index.DeleteClusterRequest{})

		test.IsRPCError(t, connectErr, connect.CodeInvalidArgument)
		test.ErrorParity(t, twirpErr, connectErr)
	})

	t.Run("missing_scope", func(t *testing.T) {
		_, twirpErr := deniedTwirp.ListIndexSets(ctx,
			&index.ListIndexSetsRequest{})
		_, connectErr := deniedConnect.ListIndexSets(ctx,
			&index.ListIndexSetsRequest{})

		test.IsRPCError(t, connectErr, connect.CodePermissionDenied)
		test.ErrorParity(t, twirpErr, connectErr)
	})

	t.Run("not_found", func(t *testing.T) {
		req := &index.SetIndexSetStatusRequest{
			Name:    "no-such-index-set",
			Enabled: true,
		}

		_, twirpErr := adminTwirp.SetIndexSetStatus(ctx, req)
		_, connectErr := adminConnect.SetIndexSetStatus(ctx, req)

		test.IsRPCError(t, connectErr, connect.CodeNotFound)
		test.ErrorParity(t, twirpErr, connectErr)
	})

	// The characteristic failed precondition for this service. It is worth
	// its own case because it is the code whose HTTP status differs between
	// the stacks -- 412 on Twirp and 400 on Connect -- so the parity that
	// matters is of the code and the message, not of the status.
	t.Run("failed_precondition", func(t *testing.T) {
		req := &index.DeleteIndexSetRequest{Name: activeSet}

		_, twirpErr := adminTwirp.DeleteIndexSet(ctx, req)
		_, connectErr := adminConnect.DeleteIndexSet(ctx, req)

		test.IsRPCError(t, connectErr, connect.CodeFailedPrecondition)
		test.ErrorParity(t, twirpErr, connectErr)
	})

	// A malformed query used to be reported as internal, because the
	// handler recoded the request parser's error. It now keeps the
	// invalid_argument the parser gave it.
	t.Run("invalid_query_is_an_invalid_argument", func(t *testing.T) {
		req := &index.QueryRequestV1{
			DocumentType: "core/article",
			Subscribe:    true,
			From:         10,
		}

		_, twirpErr := searchTwirp.Query(ctx, req)
		_, connectErr := searchConnect.Query(ctx, req)

		test.IsRPCError(t, connectErr, connect.CodeInvalidArgument)
		test.ErrorParity(t, twirpErr, connectErr)
	})
}
