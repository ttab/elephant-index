package index

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/ttab/elephantine"
	"golang.org/x/exp/slog"
)

type RawSearchRequest struct {
	Query  json.RawMessage `json:"query"`
	Fields json.RawMessage `json:"fields"`
	Source *bool           `json:"_source,omitempty"`
}

type ElasticSearchRequest struct {
	Query  ElasticQuery    `json:"query"`
	Fields json.RawMessage `json:"fields"`
	Source *bool           `json:"_source,omitempty"`
}

type ElasticQuery struct {
	Bool *BooleanQuery     `json:"bool,omitempty"`
	Term map[string]string `json:"term,omitempty"`
}

type BooleanQuery struct {
	Must   []json.RawMessage `json:"must,omitempty"`
	Should []ElasticQuery    `json:"should,omitempty"`
	Filter []ElasticQuery    `json:"filter,omitempty"`
}

// {
//   "error": {
//     "reason": "Invalid SQL query",
//     "details": "Field [unknown] cannot be found or used here.",
//     "type": "SemanticAnalysisException"
//   },
//   "status": 400
// }

type ElasticErrorType string

const (
	ErrorTypeUnauthorized ElasticErrorType = "elephant.Unauthorized"
	ErrorTypeAccessDenied ElasticErrorType = "elephant.AccessDenied"
	ErrorTypeInternal     ElasticErrorType = "elephant.InternalError"
	ErrorTypeBadRequest   ElasticErrorType = "elephant.BadRequest"
	ErrorTypeNotFound     ElasticErrorType = "elephant.NotFound"
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
	}

	return ErrorTypeInternal.Reason()
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
		logger.ErrorCtx(r.Context(),
			"failed to write error to client",
			elephantine.LogKeyError, encErr)
	}
}
