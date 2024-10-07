package response

import (
	"net/http"
)

type TransformerError struct {
	message string
	code    int
}

func (e *TransformerError) Error() string   { return e.message }
func (e *TransformerError) StatusCode() int { return e.code }

var _ error = (*TransformerError)(nil)

var (
	ErrInternalServer               = &TransformerError{message: "Internal Server Error", code: http.StatusInternalServerError}
	ErrMergePropertiesMissing       = &TransformerError{message: "either or both identifiers missing in mergeProperties", code: http.StatusBadRequest}
	ErrMergePropertiesNotSufficient = &TransformerError{message: "either or both identifiers missing in mergeProperties", code: http.StatusBadRequest}
	ErrMergePropertyOneInvalid      = &TransformerError{message: "mergeProperties contains null values for expected inputs", code: http.StatusBadRequest}
	ErrMergePropertyTwoInvalid      = &TransformerError{message: "mergeProperties contains null values for expected inputs", code: http.StatusBadRequest}
	ErrMergePropertyEmpty           = &TransformerError{message: "mergeProperties contains null values for expected inputs", code: http.StatusBadRequest}
	ErrMergePropertiesNotArray      = &TransformerError{message: "mergeProperties contains null values for expected inputs", code: http.StatusBadRequest}
	ErrEmptyTableName               = &TransformerError{message: "Table name cannot be empty.", code: http.StatusBadRequest}
	ErrEmptyColumnName              = &TransformerError{message: "Column name cannot be empty.", code: http.StatusBadRequest}
	ErrRecordIDEmpty                = &TransformerError{message: "recordId cannot be empty for cloud sources events", code: http.StatusBadRequest}
	ErrRecordIDObject               = &TransformerError{message: "recordId cannot be empty for cloud sources events", code: http.StatusBadRequest}
)

func New(message string, statusCode int) *TransformerError {
	return &TransformerError{
		message: message,
		code:    statusCode,
	}
}
