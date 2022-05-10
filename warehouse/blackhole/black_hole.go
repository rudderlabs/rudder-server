package blackhole

import (
	"fmt"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"math/rand"
	"time"
)

// BlackHole is a special warehouse destination which
// absorbs all the data events being sent to it much like the /dev/null
type BlackHole struct {
	warehouse   warehouseutils.WarehouseT
	uploader    warehouseutils.UploaderI
	variability timeBound
}
type timeBound struct {
	lower int
	upper int
}

// Setup simply initialized the variability aspect of the black-hole
// destination.
func (bh *BlackHole) Setup(warehouse warehouseutils.WarehouseT, uploader warehouseutils.UploaderI) error {

	// Pickup the variability from the destination config.
	lowerBound := warehouse.Destination.Config["lowerBound"].(int)
	upperBound := warehouse.Destination.Config["upperBound"].(int)

	if upperBound < lowerBound {
		return fmt.Errorf("unable to setup destination as the upperbound: %d should be less than lowerbound: %d",
			upperBound,
			lowerBound)
	}
	bh.variability = timeBound{
		lower: lowerBound,
		upper: upperBound,
	}
	bh.warehouse, bh.uploader = warehouse, uploader

	return nil
}

func (bh *BlackHole) CrashRecover(warehouse warehouseutils.WarehouseT) (err error) {
	return nil
}

func (bh *BlackHole) FetchSchema(warehouse warehouseutils.WarehouseT) (warehouseutils.SchemaT, error) {
	schema := bh.uploader.GetLocalSchema()
	if schema == nil {
		schema = warehouseutils.SchemaT{}
	}
	return schema, nil
}

func (bh *BlackHole) CreateSchema() (err error) {
	return nil
}

func (bh *BlackHole) CreateTable(tableName string, columnMap map[string]string) (err error) {
	schema, err := bh.FetchSchema(bh.warehouse)
	if err != nil {
		return err
	}

	if _, ok := schema[tableName]; ok {
		return fmt.Errorf("Failed to create table: table %s already exists", tableName)
	}

	// add table to schema
	schema[tableName] = columnMap

	// update schema
	return bh.uploader.UpdateLocalSchema(schema)
}

func (bh *BlackHole) AddColumn(tableName string, columnName string, columnType string) (err error) {
	// fetch schema from local db
	schema, err := bh.FetchSchema(bh.warehouse)
	if err != nil {
		return err
	}

	// check if table exists
	if _, ok := schema[tableName]; !ok {
		return fmt.Errorf("Failed to add column: table %s does not exist", tableName)
	}

	schema[tableName][columnName] = columnType

	// update schema
	return bh.uploader.UpdateLocalSchema(schema)
}

func (bh *BlackHole) AlterColumn(tableName string, columnName string, columnType string) (err error) {
	// fetch schema from local db
	schema, err := bh.FetchSchema(bh.warehouse)
	if err != nil {
		return err
	}

	// check if table exists
	if _, ok := schema[tableName]; !ok {
		return fmt.Errorf("Failed to add column: table %s does not exist", tableName)
	}

	// check if column exists
	if _, ok := schema[tableName][columnName]; !ok {
		return fmt.Errorf("Failed to alter column: column %s does not exist in table %s", columnName, tableName)
	}

	schema[tableName][columnName] = columnType

	// update schema
	return bh.uploader.UpdateLocalSchema(schema)
}

func (bh *BlackHole) LoadTable(tableName string) error {
	bh.delay()
	return nil
}

func (bh *BlackHole) LoadUserTables() map[string]error {
	// Only complete the function to loading of user tables after a delay.
	bh.delay()
	return nil
}

func (bh *BlackHole) LoadIdentityMergeRulesTable() error {
	return nil
}

func (bh *BlackHole) LoadIdentityMappingsTable() error {
	return nil
}

func (bh *BlackHole) Cleanup() {
}

func (bh *BlackHole) IsEmpty(warehouse warehouseutils.WarehouseT) (bool, error) {
	return true, nil
}

func (bh *BlackHole) TestConnection(warehouse warehouseutils.WarehouseT) error {
	return nil
}

func (bh *BlackHole) DownloadIdentityRules(*misc.GZipWriter) error {
	return nil
}
func (bh *BlackHole) GetTotalCountInTable(tableName string) (int64, error) {
	return 0, nil
}
func (bh *BlackHole) Connect(warehouse warehouseutils.WarehouseT) (client.Client, error) {
	return client.Client{}, fmt.Errorf("BlackHole: not implemented yet")
}

func (bh *BlackHole) LoadTestTable(client *client.Client, location string, warehouse warehouseutils.WarehouseT, stagingTableName string, payloadMap map[string]interface{}, format string) error {
	return fmt.Errorf("BlackHole: not implemented yet")
	return nil
}

func (bh *BlackHole) delay() {
	timeToDelay := bh.variability.lower + rand.Intn(bh.variability.upper-bh.variability.lower)
	time.Sleep(time.Duration(timeToDelay) * time.Millisecond)
}
