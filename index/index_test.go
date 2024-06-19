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

func TestInterpretBulkResponse(t *testing.T) {
	type testCase struct {
		Input  []byte
		Result map[string]int
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
	}

	for name, tCase := range cases {
		t.Run(name, func(t *testing.T) {
			ctx := test.Context(t)
			log := slog.New(test.NewLogHandler(t, slog.LevelDebug))

			got, err := index.InterpretBulkResponse(
				ctx, log, bytes.NewReader(tCase.Input))

			test.Must(t, err, "interpret response")
			test.EqualDiff(t, tCase.Result, got,
				"get the expected result")
		})
	}
}
