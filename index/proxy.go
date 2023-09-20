package index

import (
	"bytes"
	"crypto/ecdsa"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"github.com/opensearch-project/opensearch-go/v2"
	"github.com/ttab/elephantine"
)

type ElasticProxy struct {
	logger       *slog.Logger
	client       *opensearch.Client
	publicJWTKey *ecdsa.PublicKey
}

func NewElasticProxy(
	logger *slog.Logger,
	client *opensearch.Client,
	publicJWTKey *ecdsa.PublicKey,
) *ElasticProxy {
	return &ElasticProxy{
		logger:       logger,
		client:       client,
		publicJWTKey: publicJWTKey,
	}
}

func (ep *ElasticProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	logger := ep.logger.With(elephantine.LogKeyRoute, r.URL.Path)

	parts := splitPath(r.URL.Path)

	switch {
	case len(parts) <= 2 && parts[len(parts)-1] == "_search":
		ElasticHandler(logger, w, r,
			ep.searchHandler)
	default:
		ElasticHandler(logger, w, r,
			func(w http.ResponseWriter, r *http.Request) error {
				return ElasticErrorf(ErrorTypeNotFound, "no such route")
			})
	}
}

func splitPath(path string) []string {
	return strings.Split(strings.Trim(path, "/"), "/")
}

func (ep *ElasticProxy) searchHandler(
	w http.ResponseWriter, r *http.Request,
) error {
	ctx := r.Context()

	authorization := r.Header.Get("Authorization")
	if authorization == "" {
		return ElasticErrorf(
			ErrorTypeUnauthorized,
			"no authorization header")
	}

	indexBase := "documents-v1-"
	parts := splitPath(r.URL.Path)

	indicesParam := "_all"

	if len(parts) == 2 {
		indicesParam = parts[0]
	}

	var indices []string

	switch indicesParam {
	case "", "*", "_all":
		indices = []string{indexBase + "*"}
	default:
		indices = strings.Split(indicesParam, ",")

		for i := range indices {
			indices[i] = indexBase + indices[i]
		}
	}

	auth, err := elephantine.AuthInfoFromHeader(
		ep.publicJWTKey, authorization)
	if err != nil {
		return ElasticErrorf(
			ErrorTypeUnauthorized,
			"invalid token: %v", err)
	}

	if !auth.Claims.HasScope("search") {
		return ElasticErrorf(
			ErrorTypeAccessDenied,
			"missing 'search' permission")
	}

	var rawQuery RawSearchRequest

	dec := json.NewDecoder(r.Body)

	err = dec.Decode(&rawQuery)
	if err != nil {
		return ElasticErrorf(
			ErrorTypeBadRequest,
			"failed to parse request body: %v", err)
	}

	var filter []ElasticQuery

	filter = append(filter,
		ElasticQuery{
			Term: map[string]string{
				"readers": auth.Claims.Subject,
			},
		},
	)

	for _, unit := range auth.Claims.Units {
		filter = append(filter, ElasticQuery{
			Term: map[string]string{
				"readers": unit,
			},
		})
	}

	rootQuery := ElasticQuery{
		Bool: &BooleanQuery{
			Filter: []ElasticQuery{
				{Bool: &BooleanQuery{
					Should: filter,
				}},
			},
			Must: []json.RawMessage{rawQuery.Query},
		},
	}

	var searchBody bytes.Buffer

	enc := json.NewEncoder(&searchBody)

	err = enc.Encode(ElasticSearchRequest{
		Query:  rootQuery,
		Fields: rawQuery.Fields,
		Source: rawQuery.Source,
		Sort:   rawQuery.Sort,
	})
	if err != nil {
		return ElasticErrorf(
			ErrorTypeInternal,
			"failed to marshal upstream search request: %v", err)
	}

	res, err := ep.client.Search(
		ep.client.Search.WithIndex(indices...),
		ep.client.Search.WithContext(ctx),
		ep.client.Search.WithBody(&searchBody))
	if err != nil {
		return ElasticErrorf(
			ErrorTypeInternal,
			"failed to perform request: %v", err)
	}

	header := w.Header()

	for k, v := range res.Header {
		header[k] = v
	}

	w.WriteHeader(res.StatusCode)

	_, err = io.Copy(w, res.Body)
	if err != nil {
		ep.logger.ErrorContext(ctx,
			"failed to proxy search response to client",
			elephantine.LogKeyError, err)
	}

	return nil
}
