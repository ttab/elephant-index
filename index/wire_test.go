package index_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"path/filepath"
	"testing"

	"github.com/ttab/elephantine/test"
)

// The wire tests pin what the two stacks actually put on the wire, which is
// the one thing a shared service implementation cannot make identical. They
// go through raw HTTP rather than a generated client on purpose: a client
// parses the body into the generated types and so cannot see the difference
// that reaches a caller reading JSON with fetch or curl.

// twirpPath is the Twirp path for a method, and connectPath the Connect one.
// They never overlap, which is what lets one server mount both.
func twirpPath(service, method string) string {
	return fmt.Sprintf("/twirp/elephant.index.%s/%s", service, method)
}

func connectPath(service, method string) string {
	return fmt.Sprintf("/elephant.index.%s/%s", service, method)
}

// postJSON sends a JSON request body to a path and returns the status and the
// decoded response. No Connect-specific header is set: a plain JSON POST is
// all the Connect protocol requires, and that is the shape a curl caller
// uses.
func postJSON(
	t *testing.T, client *http.Client, endpoint, path, body string,
) (int, map[string]any) {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(),
		http.MethodPost, endpoint+path, bytes.NewReader([]byte(body)))
	test.Mustf(t, err, "create request for %s", path)

	req.Header.Set("Content-Type", "application/json")

	res, err := client.Do(req)
	test.Mustf(t, err, "post to %s", path)

	defer func() {
		_ = res.Body.Close()
	}()

	data, err := io.ReadAll(res.Body)
	test.Mustf(t, err, "read response body from %s", path)

	decoded := map[string]any{}

	err = json.Unmarshal(data, &decoded)
	test.Mustf(t, err, "decode response body from %s: %s", path, data)

	return res.StatusCode, decoded
}

// maskVolatile replaces the values that move between runs, so the golden
// records the shape of the body rather than the state of the database. The
// field names it masks are single words and so spelled the same on both
// stacks.
func maskVolatile(value any) {
	switch v := value.(type) {
	case map[string]any:
		for key, inner := range v {
			switch key {
			case "name", "cluster", "position":
				v[key] = "<masked>"
			default:
				maskVolatile(inner)
			}
		}
	case []any:
		for i := range v {
			maskVolatile(v[i])
		}
	}
}

// TestWireShapeSuccess records a success body per stack. ListIndexSets is the
// method to do it with: its response has a repeated nested message under a
// multi-word field, so the goldens differ in exactly the way that reaches a
// caller — index_sets on Twirp and indexSets on Connect.
func TestWireShapeSuccess(t *testing.T) {
	logger := slog.New(test.NewLogHandler(t, slog.LevelWarn))

	tc := testingAPIServer(t, logger)

	client := tc.AuthenticatedClient(t, "index_admin")

	for _, stack := range []struct {
		name string
		path string
	}{
		{name: "twirp", path: twirpPath("Management", "ListIndexSets")},
		{name: "connect", path: connectPath("Management", "ListIndexSets")},
	} {
		t.Run(stack.name, func(t *testing.T) {
			status, body := postJSON(
				t, client, tc.IndexEndpoint, stack.path, "{}")
			test.Equalf(t, http.StatusOK, status,
				"status for %s", stack.path)

			maskVolatile(body)

			test.AgainstGolden(t, regenerateTestFixtures(), body,
				filepath.Join("..", "testdata", t.Name()+".json"))
		})
	}
}

// TestWireShapeError records an error body per stack. Twirp renders code, msg
// and a meta map; Connect renders code, message and the metadata as an
// elephantine.rpc.ErrorMeta detail. The code itself is spelled identically.
func TestWireShapeError(t *testing.T) {
	logger := slog.New(test.NewLogHandler(t, slog.LevelWarn))

	tc := testingAPIServer(t, logger)

	client := tc.AuthenticatedClient(t, "doc_read", "search")

	for _, stack := range []struct {
		name string
		path string
	}{
		{name: "twirp", path: twirpPath("SearchV1", "GetFlatDocument")},
		{name: "connect", path: connectPath("SearchV1", "GetFlatDocument")},
	} {
		t.Run(stack.name, func(t *testing.T) {
			// A request with no UUID, which the handler refuses as
			// an invalid argument.
			status, body := postJSON(
				t, client, tc.IndexEndpoint, stack.path, "{}")

			test.Equalf(t, http.StatusBadRequest, status,
				"status for %s", stack.path)

			test.AgainstGolden(t, regenerateTestFixtures(), body,
				filepath.Join("..", "testdata", t.Name()+".json"))
		})
	}
}

// TestWireShapeMissingScope records what a caller without the scope gets on
// each stack, which is the error path every method shares.
func TestWireShapeMissingScope(t *testing.T) {
	logger := slog.New(test.NewLogHandler(t, slog.LevelWarn))

	tc := testingAPIServer(t, logger)

	client := tc.AuthenticatedClient(t, "doc_read")

	for _, stack := range []struct {
		name string
		path string
	}{
		{name: "twirp", path: twirpPath("Management", "ListIndexSets")},
		{name: "connect", path: connectPath("Management", "ListIndexSets")},
	} {
		t.Run(stack.name, func(t *testing.T) {
			status, body := postJSON(
				t, client, tc.IndexEndpoint, stack.path, "{}")

			test.Equalf(t, http.StatusForbidden, status,
				"status for %s", stack.path)

			test.AgainstGolden(t, regenerateTestFixtures(), body,
				filepath.Join("..", "testdata", t.Name()+".json"))
		})
	}
}
