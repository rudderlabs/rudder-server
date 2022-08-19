package configuration_testing

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/manager"
	"github.com/rudderlabs/rudder-server/warehouse/utils"
)

type validationFunc struct {
	Path string
	Func func(json.RawMessage, string) (json.RawMessage, error)
}

type DestinationValidationRequest struct {
	Destination backendconfig.DestinationT `json:"destination"`
}

type validationStep struct {
	ID        int       `json:"id"`
	Name      string    `json:"name"`
	Success   bool      `json:"success"`
	Error     string    `json:"error"`
	Validator validator `json:"-"`
}

type validator func() error

type validationStepsResponse struct {
	Steps []*validationStep `json:"steps"`
}

type DestinationValidator interface {
	ValidateCredentials(req *DestinationValidationRequest) (*DestinationValidationResponse, error)
}

// NewDestinationValidator encapsulates the process
// to generate the destination validator.
func NewDestinationValidator() DestinationValidator {
	handler := &CTHandleT{}
	return &DestinationValidatorImpl{
		validateFunc: handler.validateDestinationFunc,
	}
}

type DestinationValidatorImpl struct {
	// validateFunc takes the `creds` as raw-message for backward
	// compatibility and then results the response in raw-message
	// which can decoded later into proper struct.
	validateFunc func(json.RawMessage, string) (json.RawMessage, error)
}

// ValidateCredentials for now offloads the request to destination validation
// to the validationFunc. This function runs through all the steps in the validation check
// and then generate a valid response.
func (dv *DestinationValidatorImpl) ValidateCredentials(req *DestinationValidationRequest) (*DestinationValidationResponse, error) {
	byt, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("unable to get marshal validation request: %v", err)
	}

	bytResponse, err := dv.validateFunc(byt, "")
	if err != nil {
		return nil, fmt.Errorf("unable to perform validation on destination: %s credentials, error: %v",
			req.Destination.ID,
			err)
	}

	validationResponse := &DestinationValidationResponse{}
	err = json.Unmarshal(bytResponse, validationResponse)

	return validationResponse, err
}

type DestinationValidationResponse struct {
	Success bool              `json:"success"`
	Error   string            `json:"error"`
	Steps   []*validationStep `json:"steps"`
}

type CTHandleT struct {
	infoRequest *DestinationValidationRequest
	warehouse   warehouseutils.WarehouseT
	manager     manager.WarehouseOperations
}

type CTUploadJob struct {
	infoRequest *DestinationValidationRequest
}

func (job *CTUploadJob) GetSchemaInWarehouse() warehouseutils.SchemaT {
	return warehouseutils.SchemaT{}
}

func (job *CTUploadJob) GetLocalSchema() warehouseutils.SchemaT {
	return warehouseutils.SchemaT{}
}

func (job *CTUploadJob) UpdateLocalSchema(schema warehouseutils.SchemaT) error {
	return nil
}

func (job *CTUploadJob) GetTableSchemaInWarehouse(tableName string) warehouseutils.TableSchemaT {
	return warehouseutils.TableSchemaT{}
}

func (job *CTUploadJob) GetTableSchemaInUpload(tableName string) warehouseutils.TableSchemaT {
	return warehouseutils.TableSchemaT{}
}

func (job *CTUploadJob) GetLoadFilesMetadata(options warehouseutils.GetLoadFilesOptionsT) []warehouseutils.LoadFileT {
	return []warehouseutils.LoadFileT{}
}

func (job *CTUploadJob) GetSampleLoadFileLocation(tableName string) (string, error) {
	return "", nil
}

func (job *CTUploadJob) GetSingleLoadFile(tableName string) (warehouseutils.LoadFileT, error) {
	return warehouseutils.LoadFileT{}, nil
}

func (job *CTUploadJob) ShouldOnDedupUseNewRecord() bool {
	return false
}

func (job *CTUploadJob) UseRudderStorage() bool {
	return misc.IsConfiguredToUseRudderObjectStorage(job.infoRequest.Destination.Config)
}

func (job *CTUploadJob) GetLoadFileGenStartTIme() time.Time {
	return time.Time{}
}

func (job *CTUploadJob) GetLoadFileType() string {
	return warehouseutils.GetLoadFileType(job.infoRequest.Destination.DestinationDefinition.Name)
}

func (job *CTUploadJob) GetFirstLastEvent() (time.Time, time.Time) {
	return time.Time{}, time.Time{}
}
