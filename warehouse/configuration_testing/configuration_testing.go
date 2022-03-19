package configuration_testing

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/rudderlabs/rudder-server/config"
	proto "github.com/rudderlabs/rudder-server/proto/warehouse"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

var (
	connectionTestingFolder string
	pkgLogger               logger.LoggerI
)

func Init() {
	connectionTestingFolder = config.GetEnv("RUDDER_CONNECTION_TESTING_BUCKET_FOLDER_NAME", misc.RudderTestPayload)
	pkgLogger = logger.NewLogger().Child("warehouse").Child("connection_testing")
}

/*
	Validation Facade: Global invoking function for validation
*/
func (ct *CTHandleT) Validating(ctx context.Context, req *proto.WHValidationRequest) (*proto.WHValidationResponse, error) {
	validationFunctions := ct.validationFunctions()
	f, ok := validationFunctions[req.Path]
	if !ok {
		return nil, errors.New("path not found")
	}

	step := req.Step
	result, err := f.Func(ctx, json.RawMessage(req.Body), step)
	errorMessage := ""
	if err != nil {
		errorMessage = err.Error()
	}
	return &proto.WHValidationResponse{
		Error: errorMessage,
		Data:  string(result),
	}, nil
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
