package router

import (
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
)

type errorMapper interface {
	ErrorMappings() []model.JobError
}

type ErrorHandler struct {
	Mapper errorMapper
}

// MatchUploadJobErrorType matches the error with the error mappings defined in the integrations
// and returns the corresponding matched error type else returns UncategorizedError
func (e *ErrorHandler) MatchUploadJobErrorType(err error) model.JobErrorType {
	if e.Mapper == nil || err == nil {
		return model.UncategorizedError
	}

	errString := err.Error()

	for _, em := range e.Mapper.ErrorMappings() {
		if em.Format.MatchString(errString) {
			return em.Type
		}
	}

	return model.UncategorizedError
}
