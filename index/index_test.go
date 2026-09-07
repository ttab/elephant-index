package index_test

import (
	"bytes"
	"log/slog"
	"testing"

	"github.com/ttab/elephant-index/index"
	"github.com/ttab/elephantine/test"
)

var successBulk = []byte(`{
  "took": 17,
  "errors": false,
  "items": [
    {
      "index": {
        "_index": "documents-casual-greymalkin-core_article-sv-unspecified",
        "_id": "7d5d718f-4e36-46d5-9cd4-49b5aa4e4cbd",
        "_version": 2,
        "result": "updated",
        "_shards": {
          "total": 2,
          "successful": 2,
          "failed": 0
        },
        "_seq_no": 59,
        "_primary_term": 1,
        "status": 200
      }
    }
  ]
}`)

var failBulk = []byte(`{
  "took": 1,
  "errors": true,
  "items": [
    {
      "index": {
        "_index": "documents-casual-greymalkin-core_article-sv-unspecified",
        "_id": "b2a43c53-d6e4-4b79-a44f-b0d143da7823",
        "status": 400,
        "error": {
          "type": "mapper_parsing_exception",
          "reason": "failed to parse",
          "caused_by": {
            "type": "illegal_argument_exception",
            "reason": "Cannot write to a field alias [slug]."
          }
        }
      }
    }
  ]
}`)

// unavailableBulk reproduces the per-item 503 unavailable_shards_exception
// seen when a primary shard is reallocating (see the 2026-06-02 ele000
// incident). This must be reported as an error so the consumer retries from
// the same position rather than advancing past the dropped document.
var unavailableBulk = []byte(`{
  "took": 60000,
  "errors": true,
  "items": [
    {
      "index": {
        "_index": "documents-fair-doctor-doom-tt_wire-es-es",
        "_id": "b7974da0-e224-547c-ab3b-b54b72f0de33",
        "status": 503,
        "error": {
          "type": "unavailable_shards_exception",
          "reason": "[documents-fair-doctor-doom-tt_wire-es-es][0] primary shard is not active Timeout: [1m]"
        }
      }
    }
  ]
}`)

func TestInterpretBulkResponse(t *testing.T) {
	type testCase struct {
		Input   []byte
		Result  map[string]int
		WantErr bool
	}

	cases := map[string]testCase{
		"success": {
			Input: successBulk,
			Result: map[string]int{
				"index": 1,
			},
		},
		"fail": {
			Input: failBulk,
			Result: map[string]int{
				"index_err": 1,
			},
		},
		"unavailable_shards": {
			Input: unavailableBulk,
			Result: map[string]int{
				"index_err": 1,
			},
			WantErr: true,
		},
	}

	for name, tCase := range cases {
		t.Run(name, func(t *testing.T) {
			ctx := t.Context()
			log := slog.New(test.NewLogHandler(t, slog.LevelDebug))

			got, err := index.InterpretBulkResponse(
				ctx, log, bytes.NewReader(tCase.Input))

			if tCase.WantErr {
				if err == nil {
					t.Fatal("expected a retryable error, got nil")
				}
			} else {
				test.Mustf(t, err, "interpret response")
			}

			test.EqualDiff(t, tCase.Result, got,
				"get the expected result")
		})
	}
}
