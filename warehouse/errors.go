package warehouse

import (
	"errors"
	"fmt"
	"strings"

	"github.com/rudderlabs/rudder-server/warehouse/integrations/manager"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
)

var (
	ErrIncompatibleSchemaConversion = errors.New("incompatible schema conversion")
	ErrSchemaConversionNotSupported = errors.New("schema conversion not supported")
)

type InvalidDestinationCredErr struct {
	Base      error
	Operation string
}

func (err InvalidDestinationCredErr) Error() string {
	return fmt.Sprintf("Invalid destination creds, failed for operation: %s with err: \n%s", err.Operation, err.Base.Error())
}

type ErrorHandler struct {
	Manager manager.Manager
}

func (e *ErrorHandler) MatchErrorMappings(err error) Tag {
	var (
		errMappings []string
		errString   = err.Error()
	)

	for _, em := range e.Manager.ErrorMappings() {
		if em.Format.Match([]byte(errString)) {
			errMappings = append(errMappings, string(em.Type))
		}
	}

	if len(errMappings) > 0 {
		return Tag{name: "errors_mapping", value: strings.Join(errMappings, ",")}
	}
	return Tag{name: "errors_mapping", value: string(model.UnknownError)}
}
