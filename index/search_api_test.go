package index_test

import (
	"fmt"
	"log/slog"
	"path/filepath"
	"slices"
	"testing"
	"time"

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

	search := tc.SearchClient(t, "doc_read", "search")

	docDataDir := filepath.Join("..", "testdata", "documents")

	loadDocuments(t, documents, docDataDir, "russia_v1.json")

	const russiaUUID = "f5d2e4c5-01ba-4dae-9f09-a86701e06ecd"

	// The converted document is fetched directly from the repository, so it's
	// available without waiting for OpenSearch to catch up.
	live, err := search.GetFlatDocument(ctx, &index.GetFlatDocumentRequest{
		Uuid: russiaUUID,
	})
	test.Mustf(t, err, "get converted flattened document")

	test.Equalf(t, russiaUUID, live.Document.Uuid, "returned document UUID")

	test.EqualDiff(t,
		[]string{"Rysslands ambassadör kallas upp"},
		flatFieldValues(live, "document.title"),
		"flattened document title")

	test.EqualDiff(t,
		[]string{"core://newscoverage/" + russiaUUID},
		flatFieldValues(live, "document.uri"),
		"flattened document URI")

	test.EqualDiff(t, []string{"1"},
		flatFieldValues(live, "current_version"),
		"flattened current version")

	// The stored document becomes available once indexing has caught up. It
	// should be identical to the document converted directly from the repo.
	var stored *index.GetFlatDocumentResponse

	deadline := time.After(10 * time.Second)

	for stored == nil {
		select {
		case <-ctx.Done():
			t.Fatal("cancelled while waiting for the document to be indexed")
		case <-deadline:
			t.Fatalf("timed out waiting for the document to be indexed,"+
				" last error: %v", err)
		case <-time.After(200 * time.Millisecond):
		}

		var res *index.GetFlatDocumentResponse

		res, err = search.GetFlatDocument(ctx, &index.GetFlatDocumentRequest{
			Uuid:   russiaUUID,
			Stored: true,
		})
		if err == nil {
			stored = res
		}
	}

	if stored.Document != nil {
		t.Error("the stored response should not include the source document")
	}

	test.EqualDiff(t, allFlatFields(live), allFlatFields(stored),
		"stored fields match the converted fields")
}

func TestGetFlatDocumentRequiresUUID(t *testing.T) {
	ctx := t.Context()
	logger := slog.New(test.NewLogHandler(t, slog.LevelWarn))

	tc := testingAPIServer(t, logger)

	search := tc.SearchClient(t, "doc_read", "search")

	_, err := search.GetFlatDocument(ctx, &index.GetFlatDocumentRequest{})
	test.MustNotf(t, err, "reject request without a UUID")
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

func allFlatFields(res *index.GetFlatDocumentResponse) map[string][]string {
	out := make(map[string][]string, len(res.Fields))

	for name, values := range res.Fields {
		out[name] = values.Values
	}

	return out
}

func TestIndexPattern(t *testing.T) {
	test.Equalf(t, "documents-foo-*-*",
		internal.IndexPattern("foo", &index.QueryRequestV1{}),
		"index pattern")
	test.Equalf(t, "documents-foo-text-*",
		internal.IndexPattern("foo", &index.QueryRequestV1{
			DocumentType: "text",
		}),
		"index pattern with text")
	test.Equalf(t, "documents-foo-text-sv-*",
		internal.IndexPattern("foo", &index.QueryRequestV1{
			DocumentType: "text",
			Language:     "sv",
		}),
		"index pattern with text and language")
	test.Equalf(t, "documents-foo-text-sv-se",
		internal.IndexPattern("foo", &index.QueryRequestV1{
			DocumentType: "text",
			Language:     "sv-se",
		}),
		"index pattern with text and language and region")
	test.Equalf(t, "documents-foo-core_article--template-*",
		internal.IndexPattern("foo", &index.QueryRequestV1{
			DocumentType: "core/article#template",
		}),
		"index pattern with variant type")
	test.Equalf(t, "documents-foo-text-*,documents-foo-image-*",
		internal.IndexPattern("foo", &index.QueryRequestV1{
			DocumentTypes: []string{"text", "image"},
		}),
		"index pattern with several document types")
	test.Equalf(t, "documents-foo-text-sv-se,documents-foo-image-sv-se",
		internal.IndexPattern("foo", &index.QueryRequestV1{
			DocumentTypes: []string{"text", "image"},
			Language:      "sv-se",
		}),
		"index pattern with several document types and a language")
	test.Equalf(t, "documents-foo-text-*,documents-foo-image-*",
		internal.IndexPattern("foo", &index.QueryRequestV1{
			DocumentType:  "text",
			DocumentTypes: []string{"image", "text"},
		}),
		"index pattern unions the singular and plural fields")
	test.Equalf(t, "documents-foo-core_article-*",
		internal.IndexPattern("foo", &index.QueryRequestV1{
			DocumentTypes: []string{"core/article", "core+article"},
		}),
		"index pattern deduplicates types that sanitize alike")
	test.Equalf(t, "documents-foo-*-*",
		internal.IndexPattern("foo", &index.QueryRequestV1{
			DocumentTypes: []string{""},
		}),
		"index pattern ignores empty document types")
}

func TestSubscriptionsCannotSpanDocumentTypes(t *testing.T) {
	_, err := internal.NewSearchRequest(
		&elephantine.AuthInfo{},
		&index.QueryRequestV1{
			Subscribe:     true,
			DocumentTypes: []string{"text", "image"},
			Query: &index.QueryV1{
				Conditions: &index.QueryV1_Term{
					Term: &index.TermQueryV1{},
				},
			},
		},
	)
	test.MustNotf(t, err, "subscriptions cannot span document types")

	_, err = internal.NewSearchRequest(
		&elephantine.AuthInfo{},
		&index.QueryRequestV1{
			Subscribe:     true,
			DocumentTypes: []string{"text"},
			Query: &index.QueryV1{
				Conditions: &index.QueryV1_Term{
					Term: &index.TermQueryV1{},
				},
			},
		},
	)
	test.Mustf(t, err, "subscribe using only the plural document type field")
}

func TestQueryDocumentTypesAreCapped(t *testing.T) {
	types := make([]string, internal.MaxQueryDocumentTypes+1)
	for i := range types {
		types[i] = fmt.Sprintf("type%d", i)
	}

	_, err := internal.NewSearchRequest(
		&elephantine.AuthInfo{},
		&index.QueryRequestV1{
			DocumentTypes: types,
			Query: &index.QueryV1{
				Conditions: &index.QueryV1_Term{
					Term: &index.TermQueryV1{},
				},
			},
		},
	)
	test.MustNotf(t, err, "a query cannot span unbounded document types")
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
	test.MustNotf(t, err, "loading requires size <= 200")
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
	test.MustNotf(t, err, "subscriptions cannot be paginated")
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
	test.MustNotf(t, err, "require document type for subscription")
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
	test.Mustf(t, err, "new search request")
	test.Equalf(t,
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
	test.Mustf(t, err, "new search request")
	test.Equalf(t,
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

func TestQueryAcrossDocumentTypes(t *testing.T) {
	ctx := t.Context()
	logger := slog.New(test.NewLogHandler(t, slog.LevelWarn))

	tc := testingAPIServer(t, logger)

	documents := repository.NewDocumentsProtobufClient(
		tc.Env.Repository.GetAPIEndpoint(),
		tc.AuthenticatedClient(t, "doc_read", "doc_write", "eventlog_read"))

	search := tc.SearchClient(t, "doc_read", "search")

	docDataDir := filepath.Join("..", "testdata", "documents")

	// An article and a planning item, so the query has to reach two
	// indices to see both.
	loadDocuments(t, documents, docDataDir,
		"cyber_v1.json", "russia_v1.json")

	const (
		cyberUUID  = "b3a96437-4f79-4cc4-84c2-5c659a00e428"
		russiaUUID = "f5d2e4c5-01ba-4dae-9f09-a86701e06ecd"
	)

	want := []string{cyberUUID, russiaUUID}
	slices.Sort(want)

	hitIDs := func(req *index.QueryRequestV1) ([]string, error) {
		res, err := search.Query(ctx, req)
		if err != nil {
			return nil, fmt.Errorf("query the index: %w", err)
		}

		ids := make([]string, len(res.Hits.Hits))
		for i, hit := range res.Hits.Hits {
			ids[i] = hit.Id
		}

		slices.Sort(ids)

		return ids, nil
	}

	query := func(t *testing.T, req *index.QueryRequestV1) []string {
		t.Helper()

		ids, err := hitIDs(req)
		test.Mustf(t, err, "perform search")

		return ids
	}

	deadline := time.After(20 * time.Second)

	for {
		select {
		case <-ctx.Done():
			t.Fatal("cancelled while waiting for documents to become searchable")
		case <-deadline:
			t.Fatal("timed out waiting for documents to become searchable")
		case <-time.After(200 * time.Millisecond):
		}

		// The indices are still being created here, so a search can come
		// back 503 until the shards are assigned. That is what we're
		// waiting out, so it isn't a failure yet.
		got, err := hitIDs(&index.QueryRequestV1{
			DocumentTypes: []string{"core/article", "core/planning-item"},
			Language:      "sv-se",
			Query:         index.MatchAllQuery(),
		})
		if err != nil || len(got) < len(want) {
			continue
		}

		test.EqualDiff(t, want, got,
			"get both documents from a multi-type query")

		break
	}

	// The singular and the plural field are unioned rather than
	// exclusive.
	test.EqualDiff(t, want, query(t, &index.QueryRequestV1{
		DocumentType:  "core/article",
		DocumentTypes: []string{"core/planning-item"},
		Language:      "sv-se",
		Query:         index.MatchAllQuery(),
	}), "union the singular and plural document type fields")

	// Each hit says which document type it is, which is the only way to
	// tell a mixed result set apart.
	res, err := search.Query(ctx, &index.QueryRequestV1{
		DocumentTypes: []string{"core/article", "core/planning-item"},
		Language:      "sv-se",
		Query:         index.MatchAllQuery(),
	})
	test.Mustf(t, err, "query for the document types of the hits")

	byID := make(map[string]string, len(res.Hits.Hits))
	for _, hit := range res.Hits.Hits {
		byID[hit.Id] = hit.DocumentType
	}

	test.EqualDiff(t, map[string]string{
		cyberUUID:  "core/article",
		russiaUUID: "core/planning-item",
	}, byID, "each hit carries its own document type")

	// A type with nothing indexed has no index, and naming it must not
	// take the rest of the query with it.
	test.EqualDiff(t, want, query(t, &index.QueryRequestV1{
		DocumentTypes: []string{
			"core/article", "core/planning-item", "core/nonexistent",
		},
		Language: "sv-se",
		Query:    index.MatchAllQuery(),
	}), "ignore a document type that has no index")

	test.EqualDiff(t, []string{}, query(t, &index.QueryRequestV1{
		DocumentTypes: []string{"core/nonexistent"},
		Language:      "sv-se",
		Query:         index.MatchAllQuery(),
	}), "an unknown document type alone gives no hits")
}
