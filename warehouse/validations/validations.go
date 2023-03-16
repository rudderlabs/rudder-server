package validations

import (
	"encoding/json"
	"fmt"
	"time"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

var (
	connectionTestingFolder        string
	pkgLogger                      logger.Logger
	fileManagerFactory             filemanager.FileManagerFactory
	objectStorageValidationTimeout time.Duration
)

var (
	TableSchemaMap = model.TableSchema{
		"id":  "int",
		"val": "string",
	}
	PayloadMap = map[string]interface{}{
		"id":  1,
		"val": "RudderStack",
	}
	AlterColumnMap = model.TableSchema{
		"val_alter": "string",
	}
	Namespace = "rudderstack_setup_test"
	Table     = "setup_test_staging"
)

type validationFunc struct {
	Func func(*backendconfig.DestinationT, string) (json.RawMessage, error)
}

func Init() {
	connectionTestingFolder = config.GetString("RUDDER_CONNECTION_TESTING_BUCKET_FOLDER_NAME", misc.RudderTestPayload)
	pkgLogger = logger.NewLogger().Child("warehouse").Child("validations")
	fileManagerFactory = filemanager.DefaultFileManagerFactory
	objectStorageValidationTimeout = 15 * time.Second
}

// Validate the destination by running all the validation steps
func Validate(req *model.ValidationRequest) (*model.ValidationResponse, error) {
	res := &model.ValidationResponse{}

	f, ok := validationFunctions()[req.Path]
	if !ok {
		return res, fmt.Errorf("invalid path: %s", req.Path)
	}

	result, requestError := f.Func(req.Destination, req.Step)
	res.Data = string(result)

	if requestError != nil {
		res.Error = requestError.Error()
	}
	return res, nil
}

func validationFunctions() map[string]*validationFunc {
	return map[string]*validationFunc{
		"validate": {
			Func: validateDestinationFunc,
		},
		"steps": {
			Func: validateStepFunc,
		},
	}
}
