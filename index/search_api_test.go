package index_test

import (
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/golang-jwt/jwt/v5"
	"github.com/ttab/elephant-api/index"
	"github.com/ttab/elephant-api/repository"
	"github.com/ttab/elephant-index/internal"
	"github.com/ttab/elephantine"
	"github.com/ttab/elephantine/test"
)

func TestGetFlatDocument(t *testing.T) {
	ctx := t.Context()
	logger := slog.New(test.NewLogHandler(t, slog.LevelWarn))

	tc := testingAPIServer(t, logger)

	documents := repository.NewDocumentsProtobufClient(
		tc.Env.Repository.GetAPIEndpoint(),
		tc.AuthenticatedClient(t, "doc_read", "doc_write", "eventlog_read"))

	search := index.NewSearchV1ProtobufClient(tc.IndexEndpoint,
		tc.AuthenticatedClient(t, "doc_read", "search"))

	docDataDir := filepath.Join("..", "testdata", "documents")

	loadDocuments(t, documents, docDataDir, "russia_v1.json")

	const russiaUUID = "f5d2e4c5-01ba-4dae-9f09-a86701e06ecd"

	// The flattened document is fetched directly from the repository, so it's
	// available without waiting for OpenSearch to catch up.
	res, err := search.GetFlatDocument(ctx, &index.GetFlatDocumentRequest{
		Uuid: russiaUUID,
	})
	test.Must(t, err, "get flattened document")

	test.Equal(t, russiaUUID, res.Document.Uuid, "returned document UUID")

	test.EqualDiff(t,
		[]string{"Rysslands ambassadör kallas upp"},
		flatFieldValues(res, "document.title"),
		"flattened document title")

	test.EqualDiff(t,
		[]string{"core://newscoverage/" + russiaUUID},
		flatFieldValues(res, "document.uri"),
		"flattened document URI")

	test.EqualDiff(t, []string{"1"},
		flatFieldValues(res, "current_version"),
		"flattened current version")
}

func TestGetFlatDocumentRequiresUUID(t *testing.T) {
	ctx := t.Context()
	logger := slog.New(test.NewLogHandler(t, slog.LevelWarn))

	tc := testingAPIServer(t, logger)

	search := index.NewSearchV1ProtobufClient(tc.IndexEndpoint,
		tc.AuthenticatedClient(t, "doc_read", "search"))

	_, err := search.GetFlatDocument(ctx, &index.GetFlatDocumentRequest{})
	test.MustNot(t, err, "reject request without a UUID")
}

func flatFieldValues(
	res *index.GetFlatDocumentResponse, name string,
) []string {
	f := res.Fields[name]
	if f == nil {
		return nil
	}

	return f.Values
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
	test.Equal(t, "documents-foo-core_article--template-*",
		internal.IndexPattern("foo", &index.QueryRequestV1{
			DocumentType: "core/article#template",
		}),
		"index pattern with variant type")
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
