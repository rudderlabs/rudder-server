package validations

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	proto "github.com/rudderlabs/rudder-server/proto/warehouse"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

var (
	connectionTestingFolder string
	pkgLogger               logger.Logger
	fileManagerFactory      filemanager.FileManagerFactory
	fileManagerTimeout      time.Duration
)

var (
	TestTableSchemaMap = map[string]string{
		"id":  "int",
		"val": "string",
	}
	TestPayloadMap = map[string]interface{}{
		"id":  1,
		"val": "RudderStack",
	}
	TestNamespace  = "rudderstack_setup_test"
	AlterColumnMap = map[string]string{
		"val_alter": "string",
	}
)

const (
	verifyingObjectStorage       = "Verifying Object Storage"
	verifyingConnections         = "Verifying Connections"
	verifyingCreateSchema        = "Verifying Create Schema"
	verifyingCreateAndAlterTable = "Verifying Create and Alter Table"
	verifyingFetchSchema         = "Verifying Fetch Schema"
	verifyingLoadTable           = "Verifying Load Table"
)

func Init() {
	connectionTestingFolder = config.GetString("RUDDER_CONNECTION_TESTING_BUCKET_FOLDER_NAME", misc.RudderTestPayload)
	pkgLogger = logger.NewLogger().Child("warehouse").Child("validations")
	fileManagerFactory = filemanager.DefaultFileManagerFactory
	fileManagerTimeout = 15 * time.Second
}

// Validating Facade for Global invoking validation
func (ct *CTHandleT) Validating(req *proto.WHValidationRequest) (response *proto.WHValidationResponse, err error) {
	f, ok := ct.validationFunctions()[req.Path]
	if !ok {
		err = errors.New("path not found")
		return
	}

	result, requestError := f.Func(json.RawMessage(req.Body), req.Step)
	response = &proto.WHValidationResponse{
		Data: string(result),
	}

	if requestError != nil {
		response.Error = requestError.Error()
	}
	return
}

// validationFunctions returns validating functions for validation
func (ct *CTHandleT) validationFunctions() map[string]*validationFunc {
	return map[string]*validationFunc{
		"validate": {
			Path: "/validate",
			Func: ct.validateDestinationFunc,
		},
		"steps": {
			Path: "/steps",
			Func: ct.validationStepsFunc,
		},
	}
}
