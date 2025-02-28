package index

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/opensearch-project/opensearch-go/v2/opensearchapi"
	"github.com/ttab/elephantine"
)

type RawSearchRequest struct {
	Query       json.RawMessage `json:"query"`
	Fields      json.RawMessage `json:"fields"`
	Sort        json.RawMessage `json:"sort"`
	Source      *bool           `json:"_source,omitempty"`
	From        *int            `json:"from,omitempty"`
	Size        *int            `json:"size,omitempty"`
	SearchAfter json.RawMessage `json:"search_after,omitempty"`
}

type ElasticSearchRequest struct {
	Query       ElasticQuery    `json:"query"`
	Fields      json.RawMessage `json:"fields,omitempty"`
	Sort        json.RawMessage `json:"sort,omitempty"`
	Source      *bool           `json:"_source,omitempty"`
	From        *int            `json:"from,omitempty"`
	Size        *int            `json:"size,omitempty"`
	SearchAfter json.RawMessage `json:"search_after,omitempty"`
}

type ElasticQuery struct {
	Bool *BooleanQuery     `json:"bool,omitempty"`
	Term map[string]string `json:"term,omitempty"`
	IDs  *IDsQuery         `json:"ids,omitempty"`
}

type BooleanQuery struct {
	Must    []json.RawMessage `json:"must,omitempty"`
	MustNot []ElasticQuery    `json:"must_not,omitempty"`
	Should  []ElasticQuery    `json:"should,omitempty"`
	Filter  []ElasticQuery    `json:"filter,omitempty"`
}

type IDsQuery struct {
	Values []string `json:"values,omitempty"`
}

type SearchResponseBody struct {
	Hits struct {
		Hits []struct {
			ID     string         `json:"_id"`
			Index  string         `json:"_index"`
			Source DocumentSource `json:"_source"`
		} `json:"hits"`
	} `json:"hits"`
}

// Some well-known field names used by the indexer.
type DocumentSource struct {
	DocumentLanguage []string `json:"document.language"`
}

// ElasticErrors are used to emulate the error response structure of OpenSearch.
//
//      {
//        "error": {
//          "reason": "Invalid SQL query",
//          "details": "Field [unknown] cannot be found or used here.",
//          "type": "SemanticAnalysisException"
//        },
//        "status": 400
//      }

type ElasticErrorType string

const (
	ErrorTypeUnauthorized       ElasticErrorType = "elephant.Unauthorized"
	ErrorTypeAccessDenied       ElasticErrorType = "elephant.AccessDenied"
	ErrorTypeInternal           ElasticErrorType = "elephant.InternalError"
	ErrorTypeBadRequest         ElasticErrorType = "elephant.BadRequest"
	ErrorTypeNotFound           ElasticErrorType = "elephant.NotFound"
	ErrorTypeClusterUnavailable ElasticErrorType = "elephant.ClusterUnavailable"
)

func (et ElasticErrorType) StatusCode() int {
	switch et {
	case ErrorTypeUnauthorized:
		return http.StatusUnauthorized
	case ErrorTypeAccessDenied:
		return http.StatusForbidden
	case ErrorTypeInternal:
		return http.StatusInternalServerError
	case ErrorTypeBadRequest:
		return http.StatusBadRequest
	case ErrorTypeNotFound:
		return http.StatusNotFound
	case ErrorTypeClusterUnavailable:
		return http.StatusBadGateway
	}

	return http.StatusInternalServerError
}

func (et ElasticErrorType) Reason() string {
	switch et {
	case ErrorTypeUnauthorized:
		return "Missing or invalid authorization"
	case ErrorTypeAccessDenied:
		return "Access denied"
	case ErrorTypeInternal:
		return "Internal error"
	case ErrorTypeBadRequest:
		return "Bad request"
	case ErrorTypeNotFound:
		return "Not found"
	case ErrorTypeClusterUnavailable:
		return "Cluster unavailable"
	}

	return ErrorTypeInternal.Reason()
}

func ElasticErrorFromResponse(res *opensearchapi.Response) error {
	if !res.IsError() {
		return nil
	}

	dec := json.NewDecoder(res.Body)

	var elasticErr ElasticErrorResponse

	err := dec.Decode(&elasticErr)
	if err != nil {
		return errors.Join(
			fmt.Errorf("opensearch responded with: %s", res.Status()),
			fmt.Errorf("decoded error response: %w", err),
		)
	}

	return fmt.Errorf(
		"error response from opensearch: %s", res.Status())
}

type ElasticErrorResponse struct {
	ErrorInfo ElasticError `json:"error"`
	Status    int          `json:"status"`
}

func (er ElasticErrorResponse) Error() string {
	return er.ErrorInfo.Details
}

type ElasticError struct {
	Reason  string           `json:"reason"`
	Details string           `json:"details"`
	Type    ElasticErrorType `json:"type"`
}

func ElasticErrorf(t ElasticErrorType, format string, a ...any) ElasticErrorResponse {
	return ElasticErrorResponse{
		ErrorInfo: ElasticError{
			Reason:  t.Reason(),
			Details: fmt.Sprintf(format, a...),
			Type:    t,
		},
		Status: t.StatusCode(),
	}
}

func ElasticHandler(
	logger *slog.Logger,
	w http.ResponseWriter, r *http.Request,
	fn func(w http.ResponseWriter, r *http.Request) error,
) {
	err := fn(w, r)
	if err == nil {
		return
	}

	var ee ElasticErrorResponse

	if !errors.As(err, &ee) {
		e := ElasticErrorf(ErrorTypeInternal,
			"internal error: %v", err.Error())

		ee = e
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(ee.Status)

	enc := json.NewEncoder(w)

	enc.SetIndent("", "  ")

	encErr := enc.Encode(ee)
	if encErr != nil {
		logger.ErrorContext(r.Context(),
			"failed to write error to client",
			elephantine.LogKeyError, encErr)
	}
}
