package configuration_testing

import (
	"encoding/json"
	"github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/utils"
)

type validationFunc struct {
	Path string
	Func func(json.RawMessage, string, string) (json.RawMessage, error)
}

type infoRequest struct {
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

type validationResponse struct {
	Success bool              `json:"success"`
	Error   string            `json:"error"`
	Steps   []*validationStep `json:"steps"`
}

type CTHandleT struct {
	infoRequest      *infoRequest
	client           client.Client
	warehouse        warehouseutils.WarehouseT
	stagingTableName string
}
