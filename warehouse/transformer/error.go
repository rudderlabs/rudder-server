package transformer

import (
	"net/http"
)

type transErr struct {
	message string
	code    int
}

func (e *transErr) Error() string {
	return e.message
}
func (e *transErr) StatusCode() int { return e.code }

var _ error = (*transErr)(nil)

var (
	errInternalServer               = &transErr{message: "Internal Server Error", code: http.StatusInternalServerError}
	errMergePropertiesMissing       = &transErr{message: "either or both identifiers missing in mergeProperties", code: http.StatusBadRequest}
	errMergePropertiesNotSufficient = &transErr{message: "either or both identifiers missing in mergeProperties", code: http.StatusBadRequest}
	errMergePropertyOneInvalid      = &transErr{message: "mergeProperties contains null values for expected inputs", code: http.StatusBadRequest}
	errMergePropertyTwoInvalid      = &transErr{message: "mergeProperties contains null values for expected inputs", code: http.StatusBadRequest}
	errMergePropertyNull            = &transErr{message: "mergeProperties contains null values for expected inputs", code: http.StatusBadRequest}
	errMergePropertiesNotArray      = &transErr{message: "mergeProperties contains null values for expected inputs", code: http.StatusBadRequest}
)

func newTransErr(message string, statusCode int) *transErr {
	return &transErr{
		message: message,
		code:    statusCode,
	}
}
