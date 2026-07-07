package index_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"

	"github.com/golang-jwt/jwt/v5"
	opensearch "github.com/opensearch-project/opensearch-go/v2"
	"github.com/ttab/elephant-api/index"
	indeximpl "github.com/ttab/elephant-index/index"
	"github.com/ttab/elephant-index/internal"
	"github.com/ttab/elephantine"
	"github.com/ttab/elephantine/test"
	"github.com/twitchtv/twirp"
)

type staticTransport struct {
	statusCode int
	body       string
}

func (t *staticTransport) RoundTrip(_ *http.Request) (*http.Response, error) {
	return &http.Response{
		StatusCode: t.statusCode,
		Body:       io.NopCloser(strings.NewReader(t.body)),
		Header:     make(http.Header),
	}, nil
}

type fakeActiveIndex struct {
	client   *opensearch.Client
	indexSet string
}

func (f *fakeActiveIndex) GetActiveIndex() (*opensearch.Client, string) {
	return f.client, f.indexSet
}

func newTestSearchService(t *testing.T, statusCode int, body string) *indeximpl.SearchServiceV1 {
	t.Helper()

	client, err := opensearch.NewClient(opensearch.Config{
		Addresses: []string{"http://localhost:9200"},
		Transport: &staticTransport{statusCode: statusCode, body: body},
	})
	test.Must(t, err, "create opensearch client")

	return indeximpl.NewSearchServiceV1(
		slog.New(test.NewLogHandler(t, slog.LevelDebug)),
		nil, nil,
		&fakeActiveIndex{client: client, indexSet: "test"},
		nil, nil, nil, nil,
	)
}

func searchAuthContext(t *testing.T) context.Context {
	t.Helper()
	return elephantine.SetAuthInfo(t.Context(), &elephantine.AuthInfo{
		Claims: elephantine.JWTClaims{
			RegisteredClaims: jwt.RegisteredClaims{Subject: "core://user/1"},
			Scope:            "search",
		},
	})
}

var badRequestBody = `{"error":{"type":"search_phase_execution_exception","reason":"all shards failed"},"status":400}`

var simpleQuery = &index.QueryRequestV1{
	Query: &index.QueryV1{
		Conditions: &index.QueryV1_Term{
			Term: &index.TermQueryV1{Field: "id", Value: "foo"},
		},
	},
}

func TestIndexPattern(t *testing.T) {
	test.Equal(t, "documents-foo-*-*",
		internal.IndexPattern("foo", &index.QueryRequestV1{}),
		"index pattern")
	test.Equal(t, "documents-foo-text-*",
		internal.IndexPattern("foo", &index.QueryRequestV1{
			DocumentType: "text",
		}),
		"index pattern with text")
	test.Equal(t, "documents-foo-text-sv-*",
		internal.IndexPattern("foo", &index.QueryRequestV1{
			DocumentType: "text",
			Language:     "sv",
		}),
		"index pattern with text and language")
	test.Equal(t, "documents-foo-text-sv-se",
		internal.IndexPattern("foo", &index.QueryRequestV1{
			DocumentType: "text",
			Language:     "sv-se",
		}),
		"index pattern with text and language and region")
}

func TestLoadDocumentHasSizeCap(t *testing.T) {
	_, err := internal.NewSearchRequest(
		&elephantine.AuthInfo{},
		&index.QueryRequestV1{
			LoadDocument: true,
			Size:         400,
			Query: &index.QueryV1{
				Conditions: &index.QueryV1_Term{
					Term: &index.TermQueryV1{},
				},
			},
		},
	)
	test.MustNot(t, err, "loading requires size <= 200")
}

func TestSubscriptionsCannotBePaginated(t *testing.T) {
	_, err := internal.NewSearchRequest(
		&elephantine.AuthInfo{},
		&index.QueryRequestV1{
			Subscribe:    true,
			From:         10,
			DocumentType: "foo",
			Query: &index.QueryV1{
				Conditions: &index.QueryV1_Term{
					Term: &index.TermQueryV1{},
				},
			},
		},
	)
	test.MustNot(t, err, "subscriptions cannot be paginated")
}

func TestRequireDocumentTypeForSubscription(t *testing.T) {
	_, err := internal.NewSearchRequest(
		&elephantine.AuthInfo{},
		&index.QueryRequestV1{
			Subscribe:    true,
			DocumentType: "",
			Query: &index.QueryV1{
				Conditions: &index.QueryV1_Term{
					Term: &index.TermQueryV1{},
				},
			},
		},
	)
	test.MustNot(t, err, "require document type for subscription")
}

func TestNewSearchRequest(t *testing.T) {
	req, err := internal.NewSearchRequest(
		&elephantine.AuthInfo{
			Claims: elephantine.JWTClaims{
				RegisteredClaims: jwt.RegisteredClaims{
					Subject: "core://user/1",
				},
				Scope: "doc_read",
				Units: []string{"org://tt"},
			},
		},
		&index.QueryRequestV1{
			Query: &index.QueryV1{
				Conditions: &index.QueryV1_Term{
					Term: &index.TermQueryV1{
						Field: "id",
						Value: "foo",
					},
				},
			},
			DocumentType: "",
			Language:     "sv-se",
			Fields: []string{
				"id",
			},
			Sort: []*index.SortingV1{
				{
					Field: "id",
					Desc:  false,
				},
			},
			Source:       false,
			From:         0,
			Size:         0,
			SearchAfter:  []string{},
			LoadDocument: false,
			Subscribe:    false,
			Shared:       false,
		},
	)
	test.Must(t, err, "new search request")
	test.Equal(t,
		&internal.SearchRequestV1{
			Size: internal.DefaultSearchSize,
			Query: map[string]any{
				"bool": internal.BoolConditionsV1{
					Must: []map[string]any{{"term": map[string]any{
						"id": map[string]string{"value": "foo"},
					}}},
					Filter: []map[string]any{{"terms": map[string]any{
						"readers": []string{
							"core://user/1",
							"org://tt",
						},
					}}},
				},
			},
			Fields:      []string{"id"},
			Sort:        []map[string]string{{"id": "asc"}},
			Source:      false,
			From:        0,
			SearchAfter: []string{},
		},
		req,
		"new search request",
	)
}

func TestQueryBadRequest(t *testing.T) {
	svc := newTestSearchService(t, http.StatusBadRequest, badRequestBody)
	ctx := searchAuthContext(t)

	_, err := svc.Query(ctx, simpleQuery)
	test.MustNot(t, err, "query with bad request")

	var twerr twirp.Error
	if !errors.As(err, &twerr) {
		t.Fatalf("expected twirp.Error, got %T: %v", err, err)
	}

	test.Equal(t, twirp.InvalidArgument, twerr.Code(), "twirp error code")
}

func TestMultiSearchBadRequest(t *testing.T) {
	svc := newTestSearchService(t, http.StatusBadRequest, badRequestBody)
	ctx := searchAuthContext(t)

	_, err := svc.MultiSearch(ctx, &index.MultiSearchRequest{
		Queries: []*index.QueryRequestV1{simpleQuery},
	})
	test.MustNot(t, err, "multisearch with bad request")

	var twerr twirp.Error
	if !errors.As(err, &twerr) {
		t.Fatalf("expected twirp.Error, got %T: %v", err, err)
	}

	test.Equal(t, twirp.InvalidArgument, twerr.Code(), "twirp error code")
}

func TestNewSearchRequestAsDocAdmin(t *testing.T) {
	req, err := internal.NewSearchRequest(
		&elephantine.AuthInfo{
			Claims: elephantine.JWTClaims{
				RegisteredClaims: jwt.RegisteredClaims{
					Subject: "core://user/1",
				},
				Scope: "doc_admin",
				Units: []string{"org://tt"},
			},
		},
		&index.QueryRequestV1{
			Query: &index.QueryV1{
				Conditions: &index.QueryV1_Term{
					Term: &index.TermQueryV1{
						Field: "id",
						Value: "foo",
					},
				},
			},
			DocumentType: "",
			Language:     "sv-se",
			Fields: []string{
				"id",
			},
			Sort: []*index.SortingV1{
				{
					Field: "id",
					Desc:  false,
				},
			},
			Source:       false,
			From:         0,
			Size:         0,
			SearchAfter:  []string{},
			LoadDocument: false,
			Subscribe:    false,
			Shared:       false,
		},
	)
	test.Must(t, err, "new search request")
	test.Equal(t,
		&internal.SearchRequestV1{
			Size: internal.DefaultSearchSize,
			Query: map[string]any{
				"bool": internal.BoolConditionsV1{
					Must: []map[string]any{{"term": map[string]any{
						"id": map[string]string{"value": "foo"},
					}}},
				},
			},
			Fields:      []string{"id"},
			Sort:        []map[string]string{{"id": "asc"}},
			Source:      false,
			From:        0,
			SearchAfter: []string{},
		},
		req,
		"new search request",
	)
}
