package warehouse

import (
	"github.com/rudderlabs/rudder-server/warehouse/integrations/manager"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
)

type ErrorHandler struct {
	Manager manager.Manager
}

// MatchUploadJobErrorType matches the error with the error mappings defined in the integrations
// and returns the corresponding matched error type else returns UncategorizedError
func (e *ErrorHandler) MatchUploadJobErrorType(err error) model.JobErrorType {
	if e.Manager == nil || err == nil {
		return model.UncategorizedError
	}

	errString := err.Error()

	for _, em := range e.Manager.ErrorMappings() {
		if em.Format.MatchString(errString) {
			return em.Type
		}
	}

	return model.UncategorizedError
}
