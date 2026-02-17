package internal

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/ttab/elephant-api/index"
	"github.com/ttab/elephantine"
	"github.com/twitchtv/twirp"
)

const (
	DefaultSearchSize = 50
)

var NonAlphaNum = regexp.MustCompile(`[^a-zA-Z0-9 ]+`)

// SanitizeDocType converts a document type name into a string safe for use in
// index names. The "+" variant separator is replaced with "--" to avoid
// collisions with the "_" used for other non-alphanumeric characters.
func SanitizeDocType(docType string) string {
	base, variant, hasVariant := strings.Cut(docType, "+")
	sanitized := NonAlphaNum.ReplaceAllString(base, "_")

	if hasVariant {
		sanitized += "--" + NonAlphaNum.ReplaceAllString(variant, "_")
	}

	return sanitized
}

func IndexPattern(
	indexSet string, req *index.QueryRequestV1,
) (indexPattern string) {
	indexPattern = "documents-" + indexSet

	if req.DocumentType != "" {
		indexPattern += "-" + SanitizeDocType(req.DocumentType)
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

	return
}

func NewSearchRequest(
	auth *elephantine.AuthInfo, req *index.QueryRequestV1,
) (*SearchRequestV1, error) {
	if req.Size == 0 {
		req.Size = DefaultSearchSize
	}

	if req.LoadDocument && req.Size > 200 {
		return nil, twirp.InvalidArgumentError("documents",
			"document loading is not allowed for result sets over 200 items")
	}

	paginated := req.From != 0 || len(req.SearchAfter) > 0

	if req.Subscribe && paginated {
		return nil, twirp.InvalidArgumentError("subscribe",
			"pagination cannot be used with subscriptions")
	}

	if req.Subscribe && req.DocumentType == "" {
		return nil, twirp.InvalidArgumentError("subscribe",
			"document type is required for subscriptions")
	}

	var boolQuery BoolConditionsV1

	userQuery, err := protoToQuery(req.Query)
	if err != nil {
		return nil, twirp.InternalErrorf("translate query: %w", err)
	}

	boolQuery.Must = append(boolQuery.Must, userQuery)

	// Whether a query is shared primarily affects subscriptions.
	shared := req.Shared
	// Whether the client has the necessary scopes to bypass ACL access
	// checks.
	readAll := auth.Claims.HasAnyScope("doc_admin", "doc_read_all")

	// Add read restrictions if the client isn't allowed to read all
	// documents, or if shared was requested, as shared queries do not allow
	// for ACL bypass.
	if !readAll || shared {
		var readers []string

		// Only add the subject to readers if
		if !shared {
			readers = append(readers, auth.Claims.Subject)
		}

		readers = append(readers, auth.Claims.Units...)

		boolQuery.Filter = append(
			boolQuery.Filter,
			termsQueryV1("readers", readers, 0, false))
	}

	osReq := SearchRequestV1{
		Query:       boolQueryV1(boolQuery),
		Source:      req.Source,
		Fields:      req.Fields,
		From:        req.From,
		Size:        req.Size,
		SearchAfter: req.SearchAfter,
	}

	for _, o := range req.Sort {
		order := "asc"
		if o.Desc {
			order = "desc"
		}

		osReq.Sort = append(osReq.Sort, map[string]string{
			o.Field: order,
		})
	}

	return &osReq, nil
}

type SearchRequestV1 struct {
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

		return boolQueryV1(BoolConditionsV1{
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
		return termQueryV1(
			q.Term.Field, q.Term.Value,
			q.Term.Boost, false,
		), nil
	case *index.QueryV1_Terms:
		return termsQueryV1(
			q.Terms.Field, q.Terms.Values,
			q.Terms.Boost, false,
		), nil
	case *index.QueryV1_Match:
		return matchQueryV1(
			q.Match.Field, q.Match.Value,
			q.Match.Boost, false,
		), nil
	case *index.QueryV1_MultiMatch:
		return multiMatchQueryV1(q.MultiMatch), nil
	case *index.QueryV1_MatchPhrase:
		return matchPhraseQueryV1(q.MatchPhrase.Field, q.MatchPhrase.Value), nil
	case *index.QueryV1_QueryString:
		return queryStringQueryV1(q.QueryString), nil
	case *index.QueryV1_Prefix:
		return prefixQueryV1(
			q.Prefix.Field, q.Prefix.Value,
			q.Prefix.Boost, q.Prefix.CaseInsensitive,
		), nil
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

func QWrap(query string, cond any) map[string]any {
	return map[string]any{
		query: cond,
	}
}

func boolQueryV1(cond BoolConditionsV1) map[string]any {
	return QWrap("bool", cond)
}

type BoolConditionsV1 struct {
	Must    []map[string]any `json:"must,omitempty"`
	MustNot []map[string]any `json:"must_not,omitempty"`
	Should  []map[string]any `json:"should,omitempty"`
	Filter  []map[string]any `json:"filter,omitempty"`
}

func rangeQueryV1(field string, cond rangeConditionsV1) map[string]any {
	return QWrap("range", map[string]rangeConditionsV1{
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
	return QWrap("exists", map[string]string{
		"field": field,
	})
}

func matchAllQueryV1() map[string]any {
	return QWrap("match_all", struct{}{})
}

func termQueryV1(
	field string, term string,
	boost float64, caseInsensitive bool,
) map[string]any {
	spec := map[string]string{
		"value": term,
	}

	spec = addBoostCase(spec, boost, caseInsensitive)

	return QWrap("term", map[string]any{
		field: spec,
	})
}

func termsQueryV1(
	field string, terms []string,
	boost float64, caseInsensitive bool,
) map[string]any {
	spec := map[string]any{
		field: terms,
	}

	if boost != 0 {
		spec["boost"] = boost
	}

	if caseInsensitive {
		spec["case_insensitive"] = "true"
	}

	return QWrap("terms", spec)
}

func multiMatchQueryV1(q *index.MultiMatchQueryV1) map[string]any {
	spec := map[string]any{
		"fields": q.Fields,
		"query":  q.Query,
	}

	if q.Boost != 0 {
		spec["boost"] = q.Boost
	}

	if q.Type != "" {
		spec["type"] = q.Type
	}

	if q.BooleanAnd {
		spec["operator"] = "AND"
	}

	if q.MinimumShouldMatch != "" {
		spec["minimum_should_match"] = q.MinimumShouldMatch
	}

	if q.TieBreaker != 0 {
		spec["tie_breaker"] = q.TieBreaker
	}

	return QWrap("multi_match", spec)
}

func matchQueryV1(
	field string, match string,
	boost float64, caseInsensitive bool,
) map[string]any {
	spec := map[string]string{
		"query": match,
	}

	spec = addBoostCase(spec, boost, caseInsensitive)

	return QWrap("match", map[string]any{
		field: spec,
	})
}

func matchPhraseQueryV1(field string, phrase string) map[string]any {
	return QWrap("match_phrase", map[string]string{
		field: phrase,
	})
}

func queryStringQueryV1(query string) map[string]any {
	return QWrap("query_string", map[string]string{
		"query": query,
	})
}

func prefixQueryV1(
	field string, term string,
	boost float64, caseInsensitive bool,
) map[string]any {
	spec := map[string]string{
		"value": term,
	}

	spec = addBoostCase(spec, boost, caseInsensitive)

	return QWrap("prefix", map[string]any{
		field: spec,
	})
}

func addBoostCase(
	values map[string]string, boost float64, caseInsensitive bool,
) map[string]string {
	if boost != 0 {
		values["boost"] = fmt.Sprintf("%f", boost)
	}

	if caseInsensitive {
		values["case_insensitive"] = "true"
	}

	return values
}
