package index

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/ttab/elephant-api/index"
	"github.com/twitchtv/twirp"
)

var _ index.SearchV1 = &SearchServiceV1{}

func NewSearchServiceV1(
	active ActiveIndexGetter,
) *SearchServiceV1 {
	return &SearchServiceV1{
		active: active,
	}
}

type SearchServiceV1 struct {
	active ActiveIndexGetter
}

// Query implements index.SearchV1.
func (s *SearchServiceV1) Query(
	ctx context.Context, req *index.QueryRequestV1,
) (_ *index.QueryResponseV1, outErr error) {
	auth, err := RequireAnyScope(ctx, ScopeSeach)
	if err != nil {
		return nil, err
	}

	client, indexSet := s.active.GetActiveIndex()

	indexPattern := "documents-" + indexSet

	if req.DocumentType != "" {
		indexPattern += "-" + nonAlphaNum.ReplaceAllString(req.DocumentType, "_")
	} else {
		indexPattern += "-*"
	}

	if req.Language != "" {
		indexPattern += "-" + req.Language

		// Add a tailing wildcard if no language region has been specified.
		if !strings.ContainsRune(req.Language, '-') {
			indexPattern += "-*"
		}
	} else {
		indexPattern += "-*"
	}

	var boolQuery boolConditionsV1

	userQuery, err := protoToQuery(req.Query)
	if err != nil {
		return nil, twirp.InternalErrorf("translate query: %w", err)
	}

	boolQuery.Must = append(boolQuery.Must, userQuery)

	if !auth.Claims.HasScope("doc_admin") {
		readers := []string{
			auth.Claims.Subject,
		}

		readers = append(readers, auth.Claims.Units...)

		boolQuery.Filter = append(
			boolQuery.Filter,
			termsQueryV1("readers", readers))
	}

	osReq := searchRequestV1{
		Query:       boolQueryV1(boolQuery),
		Source:      req.Source,
		Fields:      req.Fields,
		From:        req.From,
		Size:        req.Size,
		SearchAfter: req.SearchAfter,
	}

	for _, s := range req.Sort {
		order := "asc"
		if s.Desc {
			order = "desc"
		}

		osReq.Sort = append(osReq.Sort, map[string]string{
			s.Field: order,
		})
	}

	queryPayload, err := json.Marshal(osReq)
	if err != nil {
		return nil, twirp.InternalErrorf(
			"marshal opensearch query: %w", err)
	}

	res, err := client.Search(
		client.Search.WithContext(ctx),
		client.Search.WithIndex(indexPattern),
		client.Search.WithBody(bytes.NewReader(queryPayload)))
	if err != nil {
		return nil, twirp.InternalErrorf(
			"perform opensearch search request: %w", err)
	}

	defer func() {
		err := res.Body.Close()
		if err != nil {
			outErr = errors.Join(outErr, fmt.Errorf(
				"close opensearch response body: %w", err))
		}
	}()

	dec := json.NewDecoder(res.Body)

	if res.IsError() {
		var elasticErr ElasticErrorResponse

		err := dec.Decode(&elasticErr)
		if err != nil {
			return nil, errors.Join(
				fmt.Errorf("opensearch responded with: %s", res.Status()),
				fmt.Errorf("decode error response: %w", err),
			)
		}

		return nil, fmt.Errorf(
			"error response from opensearch: %w", elasticErr)
	}

	var response searchResponse

	err = dec.Decode(&response)
	if err != nil {
		return nil, twirp.InternalErrorf(
			"unmarshal opensearch response: %w", err)
	}

	pRes := index.QueryResponseV1{
		Took:     response.Took,
		TimedOut: response.TimedOut,
		Shards: &index.ShardsV1{
			Total:      response.Shards.Total,
			Successful: response.Shards.Successful,
			Skipped:    response.Shards.Skipped,
			Failed:     response.Shards.Failed,
		},
		Hits: &index.HitsV1{
			Total: &index.HitsTotalV1{
				Value:    response.Hits.Total.Value,
				Relation: response.Hits.Total.Relation,
			},
			Hits: make([]*index.HitV1, len(response.Hits.Hits)),
		},
	}

	for i, hit := range response.Hits.Hits {
		ph := index.HitV1{
			Id:     hit.ID,
			Fields: make(map[string]*index.FieldValuesV1, len(hit.Fields)),
		}

		if hit.Score != nil {
			ph.Score = *hit.Score
		}

		for field, values := range hit.Fields {
			ph.Fields[field] = &index.FieldValuesV1{
				Values: anySliceToStrings(values),
			}
		}

		if hit.Source != nil {
			ph.Source = make(
				map[string]*index.FieldValuesV1,
				len(hit.Source))

			for field, values := range hit.Source {
				ph.Source[field] = &index.FieldValuesV1{
					Values: anySliceToStrings(values),
				}
			}
		}

		ph.Sort = anySliceToStrings(hit.Sort)

		pRes.Hits.Hits[i] = &ph
	}

	return &pRes, nil
}

func anySliceToStrings(s []any) []string {
	if len(s) == 0 {
		return nil
	}

	r := make([]string, len(s))

	for i, v := range s {
		switch c := v.(type) {
		case string:
			r[i] = c
		case float64:
			r[i] = strconv.FormatFloat(c, 'f', -1, 64)
		case bool:
			r[i] = strconv.FormatBool(c)
		}
	}

	return r
}

type searchResponse struct {
	Took     int64          `json:"took"`
	TimedOut bool           `json:"timed_out"`
	Shards   responseShards `json:"_shards"`
	Hits     responseHits   `json:"hits"`
}

type responseShards struct {
	Total      int32 `json:"total"`
	Successful int32 `json:"successful"`
	Skipped    int32 `json:"skipped"`
	Failed     int32 `json:"failed"`
}

type responseHits struct {
	Total    responseHitsTotal `json:"total"`
	MaxScore float32           `json:"max_score"`
	Hits     []responseHit     `json:"hits"`
}

type responseHitsTotal struct {
	Value    int64  `json:"value"`
	Relation string `json:"relation"`
}

type responseHit struct {
	Index  string           `json:"_index"`
	ID     string           `json:"_id"`
	Score  *float32         `json:"_score"`
	Source map[string][]any `json:"_source"`
	Fields map[string][]any `json:"fields"`
	Sort   []any            `json:"sort"`
}

type searchRequestV1 struct {
	Query       map[string]any      `json:"query"`
	Fields      []string            `json:"fields,omitempty"`
	Sort        []map[string]string `json:"sort,omitempty"`
	Source      bool                `json:"_source"`
	From        int64               `json:"from,omitempty"`
	Size        int64               `json:"size,omitempty"`
	SearchAfter []string            `json:"search_after,omitempty"`
}

func protoToQuery(p *index.QueryV1) (map[string]any, error) {
	v := p.GetConditions()

	switch q := v.(type) {
	case *index.QueryV1_Bool:
		must, err := protosToQueries(q.Bool.Must)
		if err != nil {
			return nil, fmt.Errorf("bool must queries: %w", err)
		}

		mustNot, err := protosToQueries(q.Bool.MustNot)
		if err != nil {
			return nil, fmt.Errorf("bool must not queries: %w", err)
		}

		should, err := protosToQueries(q.Bool.Should)
		if err != nil {
			return nil, fmt.Errorf("bool should queries: %w", err)
		}

		filter, err := protosToQueries(q.Bool.Filter)
		if err != nil {
			return nil, fmt.Errorf("bool filter queries: %w", err)
		}

		return boolQueryV1(boolConditionsV1{
			Must:    must,
			MustNot: mustNot,
			Should:  should,
			Filter:  filter,
		}), nil
	case *index.QueryV1_Range:
		return rangeQueryV1(q.Range.Field, rangeConditionsV1{
			GT:  q.Range.Gt,
			GTE: q.Range.Gte,
			LT:  q.Range.Lt,
			LTE: q.Range.Lte,
		}), nil
	case *index.QueryV1_Exists:
		return existsQueryV1(q.Exists), nil
	case *index.QueryV1_MatchAll:
		return matchAllQueryV1(), nil
	case *index.QueryV1_Term:
		return termQueryV1(q.Term.Field, q.Term.Value), nil
	case *index.QueryV1_Terms:
		return termsQueryV1(q.Terms.Field, q.Terms.Values), nil
	case *index.QueryV1_Match:
		return matchQueryV1(q.Match.Field, q.Match.Value), nil
	case *index.QueryV1_MatchPhrase:
		return matchPhraseQueryV1(q.MatchPhrase.Field, q.MatchPhrase.Value), nil
	case *index.QueryV1_QueryString:
		return queryStringQueryV1(q.QueryString), nil
	case *index.QueryV1_Prefix:
		return prefixQueryV1(q.Prefix.Field, q.Prefix.Value), nil
	default:
		return nil, fmt.Errorf("unknown query type %T", v)
	}
}

func protosToQueries(p []*index.QueryV1) ([]map[string]any, error) {
	if len(p) == 0 {
		return nil, nil
	}

	res := make([]map[string]any, len(p))

	for i := range p {
		q, err := protoToQuery(p[i])
		if err != nil {
			return nil, fmt.Errorf("query %d: %w", i+1, err)
		}

		res[i] = q
	}

	return res, nil
}

func qWrap(query string, cond any) map[string]any {
	return map[string]any{
		query: cond,
	}
}

func boolQueryV1(cond boolConditionsV1) map[string]any {
	return qWrap("bool", cond)
}

type boolConditionsV1 struct {
	Must    []map[string]any `json:"must,omitempty"`
	MustNot []map[string]any `json:"must_not,omitempty"`
	Should  []map[string]any `json:"should,omitempty"`
	Filter  []map[string]any `json:"filter,omitempty"`
}

func rangeQueryV1(field string, cond rangeConditionsV1) map[string]any {
	return qWrap("range", map[string]rangeConditionsV1{
		field: cond,
	})
}

type rangeConditionsV1 struct {
	GT  string `json:"gt,omitempty"`
	GTE string `json:"gte,omitempty"`
	LT  string `json:"lt,omitempty"`
	LTE string `json:"lte,omitempty"`
}

func existsQueryV1(field string) map[string]any {
	return qWrap("exists", map[string]string{
		"field": field,
	})
}

func matchAllQueryV1() map[string]any {
	return qWrap("match_all", struct{}{})
}

func termQueryV1(field string, term string) map[string]any {
	return qWrap("term", map[string]string{
		field: term,
	})
}

func termsQueryV1(field string, terms []string) map[string]any {
	return qWrap("terms", map[string][]string{
		field: terms,
	})
}

func matchQueryV1(field string, match string) map[string]any {
	return qWrap("match", map[string]string{
		field: match,
	})
}

func matchPhraseQueryV1(field string, phrase string) map[string]any {
	return qWrap("match_phrase", map[string]string{
		field: phrase,
	})
}

func queryStringQueryV1(query string) map[string]any {
	return qWrap("query_string", map[string]string{
		"query": query,
	})
}

func prefixQueryV1(field string, prefix string) map[string]any {
	return qWrap("prefix", map[string]string{
		field: prefix,
	})
}