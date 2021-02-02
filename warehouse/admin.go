package warehouse

import (
	"encoding/json"
	"errors"
	"strings"

	"github.com/rudderlabs/rudder-server/admin"
	"github.com/rudderlabs/rudder-server/warehouse/manager"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type WarehouseAdmin struct{}

func init() {
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

type QueryInput struct {
	DestID       string
	SQLStatement string
}

// Query the underlying warehouse
func (wh *WarehouseAdmin) Query(s QueryInput, reply *warehouseutils.QueryResult) error {
	if strings.TrimSpace(s.DestID) == "" {
		return errors.New("Please specify the destination ID to query the warehouse")
	}

	var warehouse warehouseutils.WarehouseT
	var ok bool
	if warehouse, ok = destinationsMap[s.DestID]; !ok {
		return errors.New("Please specify a valid and existing destination ID")
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
	*reply, err = client.Query(s.SQLStatement)
	return err
}

func (wh *WarehouseAdmin) QueryWhUploads(uploadsReq UploadsReqT, reply *[]byte) error {
	uploadsReq.API = UploadAPI
	res, err := uploadsReq.GetWhUploads()
	if err != nil {
		return err
	}
	bytes, err := json.Marshal(res)
	if err != nil {
		return err
	}
	*reply = bytes
	return nil
}

func (wh *WarehouseAdmin) QueryWhTables(tableUploadReq TableUploadReqT, reply *[]byte) error {
	tableUploadReq.API = UploadAPI
	res, err := tableUploadReq.GetWhTableUploads()
	if err != nil {
		return err
	}
	bytes, err := json.Marshal(res)
	if err != nil {
		return err
	}
	*reply = bytes
	return nil
}
