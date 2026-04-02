package index_test

import (
	"testing"

	"github.com/golang-jwt/jwt/v5"
	"github.com/ttab/elephant-api/index"
	"github.com/ttab/elephant-index/internal"
	"github.com/ttab/elephantine"
	"github.com/ttab/elephantine/test"
)

func TestMultiMatchFuzziness(t *testing.T) {
	req, err := internal.NewSearchRequest(
		&elephantine.AuthInfo{
			Claims: elephantine.JWTClaims{
				RegisteredClaims: jwt.RegisteredClaims{
					Subject: "core://user/1",
				},
				Scope: "doc_admin",
			},
		},
		&index.QueryRequestV1{
			Query: &index.QueryV1{
				Conditions: &index.QueryV1_MultiMatch{
					MultiMatch: &index.MultiMatchQueryV1{
						Fields: []string{"document.title"},
						Query:  "ukrane",
						Type:   "best_fields",
						Fuzziness: &index.Fuzziness{
							Edits: 2,
						},
					},
				},
			},
		},
	)
	test.Must(t, err, "multi_match with fuzziness")
	test.Equal(t,
		&internal.SearchRequestV1{
			Size: internal.DefaultSearchSize,
			Query: map[string]any{
				"bool": internal.BoolConditionsV1{
					Must: []map[string]any{{
						"multi_match": map[string]any{
							"fields":    []string{"document.title"},
							"query":     "ukrane",
							"type":      "best_fields",
							"fuzziness": int64(2),
						},
					}},
				},
			},
		},
		req,
		"multi_match with fuzziness",
	)
}

func TestMultiMatchFuzzinessWithPrefixLength(t *testing.T) {
	req, err := internal.NewSearchRequest(
		&elephantine.AuthInfo{
			Claims: elephantine.JWTClaims{
				RegisteredClaims: jwt.RegisteredClaims{
					Subject: "core://user/1",
				},
				Scope: "doc_admin",
			},
		},
		&index.QueryRequestV1{
			Query: &index.QueryV1{
				Conditions: &index.QueryV1_MultiMatch{
					MultiMatch: &index.MultiMatchQueryV1{
						Fields: []string{"document.title", "document.content"},
						Query:  "ukrane",
						Type:   "best_fields",
						Fuzziness: &index.Fuzziness{
							Edits: 1,
						},
						PrefixLength: 2,
					},
				},
			},
		},
	)
	test.Must(t, err, "multi_match with fuzziness and prefix_length")
	test.Equal(t,
		&internal.SearchRequestV1{
			Size: internal.DefaultSearchSize,
			Query: map[string]any{
				"bool": internal.BoolConditionsV1{
					Must: []map[string]any{{
						"multi_match": map[string]any{
							"fields":        []string{"document.title", "document.content"},
							"query":         "ukrane",
							"type":          "best_fields",
							"fuzziness":     int64(1),
							"prefix_length": int64(2),
						},
					}},
				},
			},
		},
		req,
		"multi_match with fuzziness and prefix_length",
	)
}

func TestMultiMatchWithoutFuzziness(t *testing.T) {
	req, err := internal.NewSearchRequest(
		&elephantine.AuthInfo{
			Claims: elephantine.JWTClaims{
				RegisteredClaims: jwt.RegisteredClaims{
					Subject: "core://user/1",
				},
				Scope: "doc_admin",
			},
		},
		&index.QueryRequestV1{
			Query: &index.QueryV1{
				Conditions: &index.QueryV1_MultiMatch{
					MultiMatch: &index.MultiMatchQueryV1{
						Fields: []string{"document.title"},
						Query:  "ukraine",
						Type:   "best_fields",
					},
				},
			},
		},
	)
	test.Must(t, err, "multi_match without fuzziness")
	test.Equal(t,
		&internal.SearchRequestV1{
			Size: internal.DefaultSearchSize,
			Query: map[string]any{
				"bool": internal.BoolConditionsV1{
					Must: []map[string]any{{
						"multi_match": map[string]any{
							"fields": []string{"document.title"},
							"query":  "ukraine",
							"type":   "best_fields",
						},
					}},
				},
			},
		},
		req,
		"multi_match without fuzziness",
	)
}
