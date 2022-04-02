package configuration_testing

import (
	"encoding/json"
	"errors"
	"github.com/rudderlabs/rudder-server/config"
	proto "github.com/rudderlabs/rudder-server/proto/warehouse"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

var (
	connectionTestingFolder string
	pkgLogger               logger.LoggerI
	fileManagerFactory      filemanager.FileManagerFactory
)

func Init() {
	connectionTestingFolder = config.GetEnv("RUDDER_CONNECTION_TESTING_BUCKET_FOLDER_NAME", misc.RudderTestPayload)
	pkgLogger = logger.NewLogger().Child("warehouse").Child("configuration_testing")
	fileManagerFactory = filemanager.DefaultFileManagerFactory
}

/*
	Validation Facade: Global invoking function for validation
*/
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
