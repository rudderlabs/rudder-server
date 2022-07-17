package warehouse

import (
	"errors"
	"fmt"
	"strings"

	"github.com/rudderlabs/rudder-server/warehouse/configuration_testing"

	client2 "github.com/rudderlabs/rudder-server/warehouse/client"

	"github.com/rudderlabs/rudder-server/admin"
	"github.com/rudderlabs/rudder-server/warehouse/manager"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type WarehouseAdmin struct{}

type QueryInput struct {
	DestID       string
	SourceID     string
	SQLStatement string
}

type TestInput struct {
	DestID string
}

type TestResponse struct {
	Status bool
	Error  string
}

func Init5() {
	admin.RegisterAdminHandler("Warehouse", &WarehouseAdmin{})
}

// TriggerUpload sets uploads to start without delay
func (wh *WarehouseAdmin) TriggerUpload(off bool, reply *string) error {
	startUploadAlways = !off
	if off {
		*reply = "Turned off explicit warehouse upload triggers.\nWarehouse uploads will continue to be done as per schedule in control plane."
	} else {
		*reply = "Successfully set uploads to start always without delay.\nRun same command with -o flag to turn off explicit triggers."
	}
	return nil
}

// Query the underlying warehouse
func (wh *WarehouseAdmin) Query(s QueryInput, reply *warehouseutils.QueryResult) error {
	if strings.TrimSpace(s.DestID) == "" {
		return errors.New("Please specify the destination ID to query the warehouse")
	}

	var warehouse warehouseutils.WarehouseT
	srcMap, ok := connectionsMap[s.DestID]
	if !ok {
		return errors.New("Please specify a valid and existing destination ID")
	}

	// use the sourceID-destID connection if sourceID is not empty
	if s.SourceID != "" {
		w, ok := srcMap[s.SourceID]
		if !ok {
			return errors.New("Please specify a valid (sourceID, destination ID) pair")
		}
		warehouse = w
	} else {
		// use any source connected to the given destination otherwise
		for _, v := range srcMap {
			warehouse = v
			break
		}
	}

	whManager, err := manager.New(warehouse.Type)
	if err != nil {
		return err
	}
	client, err := whManager.Connect(warehouse)
	if err != nil {
		return err
	}
	defer client.Close()

	pkgLogger.Infof(`[WH Admin]: Querying warehouse: %s:%s`, warehouse.Type, warehouse.Destination.ID)
	*reply, err = client.Query(s.SQLStatement, client2.Read)
	return err
}

// Test the underlying warehouse
func (wh *WarehouseAdmin) Test(s TestInput, reply *TestResponse) (err error) {
	if strings.TrimSpace(s.DestID) == "" {
		return errors.New("please specify the destination ID to query the warehouse")
	}

	var warehouse warehouseutils.WarehouseT
	srcMap, ok := connectionsMap[s.DestID]
	if !ok {
		return errors.New("please specify a valid and existing destination ID")
	}

	for _, v := range srcMap {
		warehouse = v
		break
	}

	pkgLogger.Infof(`[WH Admin]: Test warehouse: %s:%s`, warehouse.Type, warehouse.Destination.ID)

	destinationValidator := configuration_testing.NewDestinationValidator()
	req := &configuration_testing.DestinationValidationRequest{Destination: warehouse.Destination}
	res, err := destinationValidator.ValidateCredentials(req)
	if err != nil {
		err = fmt.Errorf("unable to successfully validate destination: %s credentials, err: %v", warehouse.Destination.ID, err)
		pkgLogger.Error(err)
		return
	}

	*reply = TestResponse{Status: res.Success, Error: res.Error}
	return
}
