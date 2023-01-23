package validations

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/rudderlabs/rudder-server/warehouse/integrations/manager"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/client/controlplane"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
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
	infoRequest      *DestinationValidationRequest
	warehouse        warehouseutils.Warehouse
	manager          manager.WarehouseOperations
	CPClient         controlplane.InternalControlPlane
	EnableTunnelling bool
}

type CTUploadJob struct {
	infoRequest *DestinationValidationRequest
}

func (*CTUploadJob) GetSchemaInWarehouse() warehouseutils.SchemaT {
	return warehouseutils.SchemaT{}
}

func (*CTUploadJob) GetLocalSchema() warehouseutils.SchemaT {
	return warehouseutils.SchemaT{}
}

func (*CTUploadJob) UpdateLocalSchema(_ warehouseutils.SchemaT) error {
	return nil
}

func (*CTUploadJob) GetTableSchemaInWarehouse(_ string) warehouseutils.TableSchemaT {
	return warehouseutils.TableSchemaT{}
}

func (*CTUploadJob) GetTableSchemaInUpload(_ string) warehouseutils.TableSchemaT {
	return warehouseutils.TableSchemaT{}
}

func (*CTUploadJob) GetLoadFilesMetadata(_ warehouseutils.GetLoadFilesOptionsT) []warehouseutils.LoadFileT {
	return []warehouseutils.LoadFileT{}
}

func (*CTUploadJob) GetSampleLoadFileLocation(_ string) (string, error) {
	return "", nil
}

func (*CTUploadJob) GetSingleLoadFile(_ string) (warehouseutils.LoadFileT, error) {
	return warehouseutils.LoadFileT{}, nil
}

func (*CTUploadJob) ShouldOnDedupUseNewRecord() bool {
	return false
}

func (job *CTUploadJob) UseRudderStorage() bool {
	return misc.IsConfiguredToUseRudderObjectStorage(job.infoRequest.Destination.Config)
}

func (*CTUploadJob) GetLoadFileGenStartTIme() time.Time {
	return time.Time{}
}

func (job *CTUploadJob) GetLoadFileType() string {
	return warehouseutils.GetLoadFileType(job.infoRequest.Destination.DestinationDefinition.Name)
}

func (*CTUploadJob) GetFirstLastEvent() (time.Time, time.Time) {
	return time.Time{}, time.Time{}
}
