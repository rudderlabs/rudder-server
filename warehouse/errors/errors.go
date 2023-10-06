package errors

import (
	"strings"

	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"

	"github.com/rudderlabs/rudder-server/warehouse/integrations/manager"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
)

type ErrorHandler struct {
	Manager manager.Manager
}

// MatchErrorMappings matches the error with the error mappings defined in the integrations
// and returns the corresponding joins of the matched error type
// else returns UnknownError
func (e *ErrorHandler) MatchErrorMappings(err error) warehouseutils.Tag {
	if e.Manager == nil || err == nil {
		return warehouseutils.Tag{Name: "error_mapping", Value: model.Noop}
	}

	var (
		errMappings []string
		errString   = err.Error()
	)

	for _, em := range e.Manager.ErrorMappings() {
		if em.Format.MatchString(errString) {
			errMappings = append(errMappings, em.Type)
		}
	}

	if len(errMappings) > 0 {
		return warehouseutils.Tag{Name: "error_mapping", Value: strings.Join(errMappings, ",")}
	}
	return warehouseutils.Tag{Name: "error_mapping", Value: model.UnknownError}
}
