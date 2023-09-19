package validations

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

const (
	namespace = "rudderstack_setup_test"
	table     = "setup_test_staging"
)

var (
	connectionTestingFolder string
	pkgLogger               logger.Logger
	fileManagerFactory      filemanager.Factory
	objectStorageTimeout    time.Duration
	queryTimeout            time.Duration
)

var (
	tableSchemaMap = model.TableSchema{
		"id":  "int",
		"val": "string",
	}
	payloadMap = map[string]interface{}{
		"id":  1,
		"val": "RudderStack",
	}
	alterColumnMap = model.TableSchema{
		"val_alter": "string",
	}
)

type validationFunc struct {
	Func func(context.Context, *backendconfig.DestinationT, string) (json.RawMessage, error)
}

func Init() {
	connectionTestingFolder = config.GetString("RUDDER_CONNECTION_TESTING_BUCKET_FOLDER_NAME", misc.RudderTestPayload)
	pkgLogger = logger.NewLogger().Child("warehouse").Child("validations")
	fileManagerFactory = filemanager.New
	objectStorageTimeout = config.GetDuration("Warehouse.Validations.ObjectStorageTimeout", 15, time.Second)

	// Since we have a cp-router default timeout of 30 seconds, keeping the query timeout to 25 seconds
	queryTimeout = config.GetDuration("Warehouse.Validations.QueryTimeout", 25, time.Second)
}

// Validate the destination by running all the validation steps
func Validate(ctx context.Context, req *model.ValidationRequest) (*model.ValidationResponse, error) {
	res := &model.ValidationResponse{}

	f, ok := validationFunctions()[req.Path]
	if !ok {
		return res, fmt.Errorf("invalid path: %s", req.Path)
	}

	result, requestError := f.Func(ctx, req.Destination, req.Step)
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
