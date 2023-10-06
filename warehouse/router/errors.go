package router

import (
	"strings"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type errorMapper interface {
	ErrorMappings() []model.JobError
}

type ErrorHandler struct {
	Mapper errorMapper
}

// MatchErrorMappings matches the error with the error mappings defined in the integrations
// and returns the corresponding joins of the matched error type
// else returns UnknownError
func (e *ErrorHandler) MatchErrorMappings(err error) whutils.Tag {
	if e.Mapper == nil || err == nil {
		return whutils.Tag{Name: "error_mapping", Value: model.Noop}
	}

	var (
		errMappings []string
		errString   = err.Error()
	)

	for _, em := range e.Mapper.ErrorMappings() {
		if em.Format.MatchString(errString) {
			errMappings = append(errMappings, em.Type)
		}
	}

	if len(errMappings) > 0 {
		return whutils.Tag{Name: "error_mapping", Value: strings.Join(errMappings, ",")}
	}
	return whutils.Tag{Name: "error_mapping", Value: model.UnknownError}
}
