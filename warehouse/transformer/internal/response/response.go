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
	ErrInternalServer               = NewTransformerError("Internal Server Error", http.StatusInternalServerError)
	ErrMergePropertiesMissing       = NewTransformerError("either or both identifiers missing in mergeProperties", http.StatusBadRequest)
	ErrMergePropertiesNotSufficient = ErrMergePropertiesMissing
	ErrMergePropertyOneInvalid      = NewTransformerError("mergeProperties contains null values for expected inputs", http.StatusBadRequest)
	ErrMergePropertyTwoInvalid      = ErrMergePropertyOneInvalid
	ErrMergePropertyEmpty           = ErrMergePropertyOneInvalid
	ErrMergePropertiesNotArray      = ErrMergePropertyOneInvalid
	ErrEmptyTableName               = NewTransformerError("Table name cannot be empty.", http.StatusBadRequest)
	ErrEmptyColumnName              = NewTransformerError("Column name cannot be empty.", http.StatusBadRequest)
	ErrRecordIDEmpty                = NewTransformerError("recordId cannot be empty for cloud sources events", http.StatusBadRequest)
	ErrContextNotMap                = NewTransformerError("context is not a map", http.StatusInternalServerError)
	ErrExtractEventNameEmpty        = NewTransformerError("cannot create event table with empty event name, event name is missing in the payload", http.StatusInternalServerError)
	ErrRecordIDObject               = ErrRecordIDEmpty
)

func NewTransformerError(message string, statusCode int) *TransformerError {
	return &TransformerError{
		message: message,
		code:    statusCode,
	}
}
