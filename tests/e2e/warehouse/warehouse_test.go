package warehouse_test

import (
	"database/sql"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/tests/helpers"
	"github.com/tidwall/gjson"
	"reflect"
	"strings"
)

var dbHandle *sql.DB
var gatewayDBPrefix string
var routerDBPrefix string
var dbPollFreqInS int = 1
var pollIntervalForLoadTables int = 10
var loadTablesTimeout int = 300
var warehouseLoadFolder string
var eventName string = "ginkgo"
var warehouseLoadFilesTable string
var warehouseSchemasTable string
var bucket string = "warehouse_ginkgo"
var destinationsIDs = []string{"1ZWfxCG7FEOwLiiicAkbLlhQ9oR", "1ZenKtbvnO60QA8HYanm3PZSoj6"}
var sourceIDs = []string{"1YyeVLcPH3rKK3ehTjTUfQyxiRS"}

var dbKeywords = []string{"alter", "select", "while", "limit", "is", "from", "dynamic", "catalog", "right"}

var _ = BeforeSuite(func() {
	var err error
	psqlInfo := jobsdb.GetConnectionString()
	dbHandle, err = sql.Open("postgres", psqlInfo)
	if err != nil {
		panic(err)
	}
	gatewayDBPrefix = config.GetString("Gateway.CustomVal", "GW")
	routerDBPrefix = config.GetString("Router.CustomVal", "RT")
	warehouseLoadFilesTable = config.GetString("Warehouse.loadFilesTable", "wh_load_files")
	warehouseLoadFolder = config.GetEnv("WAREHOUSE_BUCKET_LOAD_OBJECTS_FOLDER_NAME", "rudder-warehouse-load-objects")
	warehouseSchemasTable = config.GetString("Warehouse.schemasTable", "wh_schemas")
	helpers.DeleteRowsInTable(dbHandle, warehouseLoadFilesTable)
})

var _ = Describe("Warehouse", func() {
	Describe("By sending a generic track event, it should be able to create load files in gcs ", func() {
		batchJson := helpers.AddKeyToJSON(helpers.RemoveKeyFromJSON(helpers.BatchPayload, "batch.0.messageId", "batch.0.anonymousId"), "batch.0.event", eventName)
		It("should able to create a load file in database with event name", func() {
			helpers.SendBatchRequest("1YyeVOLDReXPNYORjDlvE7PM1Jw", batchJson)
			loadTablesFromAboveTrackJson := []string{"tracks", strings.Replace(strings.ToLower(eventName), " ", "_", -1)}
			Eventually(func() bool {
				loadedTables := helpers.GetLoadFileTableName(dbHandle, warehouseLoadFilesTable)
				return helpers.IsThisInThatSliceString(loadTablesFromAboveTrackJson, loadedTables)
			}, loadTablesTimeout, pollIntervalForLoadTables).Should(Equal(true))
		})
		It("should have same data for load file as the event payload sent", func() {
			loadedFileData := helpers.GetEventLoadFileData(dbHandle, warehouseLoadFilesTable, eventName, bucket)
			Expect(gjson.Get(batchJson, "batch.0.event").String()).Should(Equal(gjson.Get(loadedFileData, "event").String()))
		})
		It("should be able to create load files if source has two warehouse destinations", func() {
			loadedDestinationIDs := helpers.GetDestinationIDsFromLoadFileTable(dbHandle, warehouseLoadFilesTable, sourceIDs[0])
			Expect(helpers.IsThisInThatSliceString(destinationsIDs, loadedDestinationIDs)).Should(Equal(true))
		})
	})
	Describe("Compatible with segment warehouse schema", func() {
		BeforeEach(func() {
			helpers.DeleteRowsInTable(dbHandle, warehouseLoadFilesTable)
		})
		It("should be able to create tables ", func() {
			helpers.SendTrackRequest("1YyeVOLDReXPNYORjDlvE7PM1Jw", helpers.AddKeyToJSON(helpers.RemoveKeyFromJSON(helpers.TrackPayload, "messageId", "anonymousId"), "event", eventName))
			helpers.SendIdentifyRequest("1YyeVOLDReXPNYORjDlvE7PM1Jw", helpers.RemoveKeyFromJSON(helpers.IdentifyPayload, "messageId", "anonymousId"))
			helpers.SendPageRequest("1YyeVOLDReXPNYORjDlvE7PM1Jw", helpers.RemoveKeyFromJSON(helpers.PagePayload, "messageId", "anonymousId"))
			//helpers.SendAliasRequest("1YyeVOLDReXPNYORjDlvE7PM1Jw", helpers.RemoveKeyFromJSON(helpers.AliasPayload, "messageId", "anonymousId"))
			//helpers.SendGroupRequest("1YyeVOLDReXPNYORjDlvE7PM1Jw", helpers.RemoveKeyFromJSON(helpers.GroupPayload, "messageId", "anonymousId"))
			helpers.SendScreenRequest("1YyeVOLDReXPNYORjDlvE7PM1Jw", helpers.RemoveKeyFromJSON(helpers.ScreenPayload, "messageId", "anonymousId"))
			tables := []string{"identifies", "users", "pages", "tracks", "screens", strings.Replace(strings.ToLower(eventName), " ", "_", -1)} // group, alias are not supported.
			Eventually(func() bool {
				loadedTables := helpers.GetLoadFileTableName(dbHandle, warehouseLoadFilesTable)
				return helpers.IsThisInThatSliceString(tables, loadedTables)
			}, loadTablesTimeout, pollIntervalForLoadTables).Should(Equal(true))
		})
	})
	Describe("testing with different string formats", func() {
		BeforeEach(func() {
			helpers.DeleteRowsInTable(dbHandle, warehouseLoadFilesTable)
		})
		It("should be able to create load file", func() {
			helpers.SendBatchRequest("1YyeVOLDReXPNYORjDlvE7PM1Jw", helpers.AddKeyToJSON(helpers.RemoveKeyFromJSON(helpers.DiffStringFormatBatchPayload, "batch.0.messageId", "batch.0.anonymousId"), "batch.0.event", eventName))
			eventName := strings.Replace(strings.ToLower(eventName), " ", "_", -1)
			Eventually(func() bool {
				loadedTables := helpers.GetLoadFileTableName(dbHandle, warehouseLoadFilesTable)
				return helpers.IsThisInThatSliceString([]string{eventName}, loadedTables)
			}, loadTablesTimeout, pollIntervalForLoadTables).Should(Equal(true))
		})
	})
	Describe("sending different data types for a key consecutively", func() {
		BeforeEach(func() {
			helpers.DeleteRowsInTable(dbHandle, warehouseLoadFilesTable)
			helpers.DeleteRowsInTable(dbHandle, warehouseSchemasTable)
		})
		It("should be able to create load file and schema should have different data types", func() {
			helpers.SendTrackRequest("1YyeVOLDReXPNYORjDlvE7PM1Jw", helpers.AddKeyToJSON(helpers.RemoveKeyFromJSON(gjson.Get(helpers.DTBatchPayload, "batch.0").String(), "messageId", "anonymousId"), "event", eventName))
			helpers.SendTrackRequest("1YyeVOLDReXPNYORjDlvE7PM1Jw", helpers.AddKeyToJSON(helpers.RemoveKeyFromJSON(gjson.Get(helpers.DTBatchPayload, "batch.1").String(), "messageId", "anonymousId"), "event", eventName))
			helpers.SendTrackRequest("1YyeVOLDReXPNYORjDlvE7PM1Jw", helpers.AddKeyToJSON(helpers.RemoveKeyFromJSON(gjson.Get(helpers.DTBatchPayload, "batch.2").String(), "messageId", "anonymousId"), "event", eventName))
			eventName = strings.Replace(strings.ToLower(eventName), " ", "_", -1)
			Eventually(func() bool {
				loadedTables := helpers.GetLoadFileTableName(dbHandle, warehouseLoadFilesTable)
				return helpers.IsThisInThatSliceString([]string{eventName}, loadedTables)
			}, loadTablesTimeout, pollIntervalForLoadTables).Should(Equal(true))
			Eventually(func() bool {
				loadedSchema := helpers.GetWarehouseSchema(dbHandle, warehouseSchemasTable, sourceIDs[0], destinationsIDs[0])
				return reflect.DeepEqual(loadedSchema, helpers.DTSchema)
			}, loadTablesTimeout, pollIntervalForLoadTables).Should(Equal(true))
		})
	})
	Describe("Reserved Keywords as one of keys in an event should be replaced by _key", func() {
		BeforeEach(func() {
			helpers.DeleteRowsInTable(dbHandle, warehouseLoadFilesTable)
		})
		It("should be able to create load file, while sending reservered keywords", func() {
			helpers.SendBatchRequest("1YyeVOLDReXPNYORjDlvE7PM1Jw", helpers.RemoveKeyFromJSON(helpers.BQBatchPayload, "batch.0.messageId", "batch.0.anonymousId"))
			eventName := gjson.Get(helpers.BQBatchPayload, "batch.0.event").String()
			eventName = strings.Replace(strings.ToLower(eventName), " ", "_", -1)
			Eventually(func() bool {
				loadedTables := helpers.GetLoadFileTableName(dbHandle, warehouseLoadFilesTable)
				return helpers.IsThisInThatSliceString([]string{"_" + eventName}, loadedTables)
			}, loadTablesTimeout, pollIntervalForLoadTables).Should(Equal(true))
		})
	})
})
