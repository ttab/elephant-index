package index

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"github.com/opensearch-project/opensearch-go/v2"
	"github.com/ttab/elephantine"
)

type ActiveIndexGetter interface {
	GetActiveIndex() (*opensearch.Client, string)
}

type ElasticProxy struct {
	logger         *slog.Logger
	authInfoParser *elephantine.AuthInfoParser
	active         ActiveIndexGetter
}

func NewElasticProxy(
	logger *slog.Logger,
	active ActiveIndexGetter,
	authInfoParser *elephantine.AuthInfoParser,
) *ElasticProxy {
	return &ElasticProxy{
		logger:         logger,
		active:         active,
		authInfoParser: authInfoParser,
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
			func(_ http.ResponseWriter, _ *http.Request) error {
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

	client, indexSet := ep.active.GetActiveIndex()
	if client == nil {
		return ElasticErrorf(
			ErrorTypeClusterUnavailable,
			"the active index is not set")
	}

	indexBase := "documents-" + indexSet + "-"
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

	auth, err := ep.authInfoParser.AuthInfoFromHeader(authorization)
	if err != nil {
		return ElasticErrorf(
			ErrorTypeUnauthorized,
			"invalid token: %v", err)
	}

	if !auth.Claims.HasScope(ScopeSearch) {
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

	rootQuery := ElasticQuery{
		Bool: &BooleanQuery{
			Must: []json.RawMessage{rawQuery.Query},
		},
	}

	if !auth.Claims.HasScope("doc_admin") {
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

		rootQuery.Bool.Filter = []ElasticQuery{
			{Bool: &BooleanQuery{
				Should: filter,
			}},
		}
	}

	var searchBody bytes.Buffer

	enc := json.NewEncoder(&searchBody)

	err = enc.Encode(ElasticSearchRequest{
		Query:       rootQuery,
		Fields:      rawQuery.Fields,
		Source:      rawQuery.Source,
		Sort:        rawQuery.Sort,
		From:        rawQuery.From,
		Size:        rawQuery.Size,
		SearchAfter: rawQuery.SearchAfter,
	})
	if err != nil {
		return ElasticErrorf(
			ErrorTypeInternal,
			"failed to marshal upstream search request: %v", err)
	}

	res, err := client.Search(
		client.Search.WithIndex(indices...),
		client.Search.WithContext(ctx),
		client.Search.WithBody(&searchBody))
	if err != nil {
		return ElasticErrorf(
			ErrorTypeInternal,
			"failed to perform request: %v", err)
	}

	defer elephantine.SafeClose(ep.logger, "search response", res.Body)

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
