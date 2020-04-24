package warehouse_test

import (
	"database/sql"
	"fmt"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/tests/helpers"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	uuid "github.com/satori/go.uuid"
	"github.com/tidwall/gjson"
	"strings"
)

var dbHandle *sql.DB
var gatewayDBPrefix string
var routerDBPrefix string
var dbPollFreqInS int = 1
var pollIntervalForLoadTables int = 10
var loadTablesTimeout int = 100
var eventName string = "ginkgo"
var warehouseTables []string
var bucket string = "warehouse_ginkgo"
var destinationsIDs = []string{"1arWHNQ33AOhDWd8DHOJFoCNHzQ"}
var sourceIDs = []string{"1arW7wSKSTpPyp0cscWOR37qvww"}

var dbKeywords = []string{"alter", "select", "while", "limit", "is", "from", "dynamic", "catalog", "right"}
var writeKey string = "1arW7vLmzvmwMkTzDFwmcKiAikX"
var sourceJSON backendconfig.SourcesT


var (
	warehouseStagingFilesTable string
	warehouseUploadsTable string
	warehouseSchemasTable string
	warehouseLoadFilesTable string
	warehouseLoadFolder string
	warehouseTableUploadsTable string
)
const (
	exportedDataState = "exported_data"
)
const (
	BQ = "BQ"
	RS = "RS"
	SNOWFLAKE = "SNOWFLAKE"
)
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
	warehouseStagingFilesTable = config.GetString("Warehouse.stagingFilesTable", "wh_staging_files")
	warehouseUploadsTable = config.GetString("Warehouse.uploadsTable", "wh_uploads")
	warehouseTableUploadsTable = config.GetString("Warehouse.tableUploadsTable", "wh_table_uploads")
	warehouseTables = []string{warehouseLoadFilesTable, warehouseSchemasTable, warehouseStagingFilesTable, warehouseUploadsTable, warehouseTableUploadsTable}
	sourceJSON = getWorkspaceConfig()
	initializeWarehouseConfig()
})
func getWorkspaceConfig() backendconfig.SourcesT{
	backendConfig := new(backendconfig.WorkspaceConfig)
	sourceJSON, _ := backendConfig.Get()
	return sourceJSON
}
func initializeWarehouseConfig(){
	for _, source := range sourceJSON.Sources {
		if source.Name == "warehouse-ginkgo" {
			if len(source.Destinations) > 0 {
				for _, destination := range source.Destinations {
					warehouses[destination.DestinationDefinition.Name] =append(warehouses[destination.DestinationDefinition.Name], warehouseutils.WarehouseT{Source: source, Destination: destination})
				}
			}
		}
	}
}

var warehouses  = make(map[string][]warehouseutils.WarehouseT)

var _ = Describe("Warehouse", func() {
	Describe("By sending a generic track event, it should be able to create load files in gcs and upload in warehouses ", func() {
		BeforeEach(func(){
			helpers.DeleteRowsInTables(dbHandle, warehouseTables)
		})
		//PDescribe("BigQuery",func() {
		//	It("should able to create a load file in database with event name", func() {
		//		destType := BQ
		//		WarehouseConfig := warehouses[destType]
		//		batchJson := helpers.AddKeyToJSON(helpers.WarehouseBatchPayload, "batch.0.event", eventName)
		//		anonymousId:= uuid.NewV4().String()
		//		batchJson =  helpers.AddKeyToJSON(batchJson, "batch.0.anonymousId", anonymousId)
		//		helpers.SendBatchRequest(writeKey, batchJson)
		//		loadTablesFromAboveTrackJson := []string{"tracks", strings.Replace(strings.ToLower(eventName), " ", "_", -1)}
		//		Eventually(func() bool {
		//			loadedTables := helpers.GetLoadFileTableName(dbHandle, warehouseLoadFilesTable, WarehouseConfig[0].Source.ID, WarehouseConfig[0].Destination.ID, destType)
		//			return helpers.IsThisInThatSliceString(loadTablesFromAboveTrackJson, loadedTables)
		//		}, loadTablesTimeout, pollIntervalForLoadTables).Should(Equal(true))
		//		By("should have same data for load file as the event payload sent")
		//		loadedFileData := helpers.GetEventLoadFileData(dbHandle, warehouseLoadFilesTable, eventName, bucket)
		//		Expect(gjson.Get(batchJson, "batch.0.event").String()).Should(Equal(gjson.Get(loadedFileData, "event").String()))
		//		By("should be able to create load files if source has two warehouse destinations")
		//		loadedDestinationIDs := helpers.GetDestinationIDsFromLoadFileTable(dbHandle, warehouseLoadFilesTable, sourceIDs[0])
		//		Expect(helpers.IsThisInThatSliceString(destinationsIDs, loadedDestinationIDs)).Should(Equal(true))
		//		By("should be able to upload to bq with state exported_data")
		//		var namespace string
		//		var state string
		//		Eventually(func() string {
		//			namespace,state=helpers.FetchUpdateState(dbHandle, warehouseUploadsTable, WarehouseConfig[0].Source.ID, WarehouseConfig[0].Destination.ID, destType)
		//			return state
		//		}, loadTablesTimeout, pollIntervalForLoadTables).Should(Equal(exportedDataState))
		//		By("should be able to query with anonymousId and compare properties and timestamps")
		//		Eventually(func() string{
		//			payload := helpers.QueryWarehouseWithAnonymusID(anonymousId, eventName, namespace)
		//			return payload.Label
		//		}, loadTablesTimeout, pollIntervalForLoadTables).Should(Equal(gjson.Get(batchJson, "batch.0.properties.label" ).String()))
		//	})
		//})
		Describe("REDSHIFT",func() {
			It("should able to create a load file in database with event name", func() {
				destType := RS
				WarehouseConfig := warehouses[destType]
				batchJson := helpers.AddKeyToJSON(helpers.WarehouseBatchPayload, "batch.0.event", eventName)
				anonymousId:= uuid.NewV4().String()
				batchJson =  helpers.AddKeyToJSON(batchJson, "batch.0.anonymousId", anonymousId)
				helpers.SendBatchRequest(writeKey, batchJson)
				loadTablesFromAboveTrackJson := []string{"tracks", strings.Replace(strings.ToLower(eventName), " ", "_", -1)}
				Eventually(func() bool {
					loadedTables := helpers.GetLoadFileTableName(dbHandle, warehouseLoadFilesTable, WarehouseConfig[0].Source.ID, WarehouseConfig[0].Destination.ID, destType)
					return helpers.IsThisInThatSliceString(loadTablesFromAboveTrackJson, loadedTables)
				}, loadTablesTimeout, pollIntervalForLoadTables).Should(Equal(true))
				//By("should have same data for load file as the event payload sent")
				//loadedFileData := helpers.GetEventLoadFileData(dbHandle, warehouseLoadFilesTable, eventName, bucket)
				//Expect(gjson.Get(batchJson, "batch.0.event").String()).Should(Equal(gjson.Get(loadedFileData, "event").String()))
				//By("should be able to create load files if source has two warehouse destinations")
				//loadedDestinationIDs := helpers.GetDestinationIDsFromLoadFileTable(dbHandle, warehouseLoadFilesTable, sourceIDs[0])
				//Expect(helpers.IsThisInThatSliceString(destinationsIDs, loadedDestinationIDs)).Should(Equal(true))
				By("should be able to upload to bq with state exported_data")
				var namespace string
				var state string
				Eventually(func() string {
					namespace,state=helpers.FetchUpdateState(dbHandle, warehouseUploadsTable, WarehouseConfig[0].Source.ID, WarehouseConfig[0].Destination.ID, destType)
					fmt.Println(namespace)
					return state
				}, loadTablesTimeout, pollIntervalForLoadTables).Should(Equal(exportedDataState))
				By("should be able to query with anonymousId and compare properties and timestamps")
				Eventually(func() string{
					payload:=helpers.QueryWarehouseWithAnonymusID(anonymousId, eventName, namespace, destType, WarehouseConfig[0].Destination.Config)
					return payload.Label
				}, loadTablesTimeout, pollIntervalForLoadTables).Should(Equal(gjson.Get(batchJson, "batch.0.properties.label" ).String()))

			})
		})
		PDescribe("SNOWFLAKE",func() {
			It("should able to create a load file in database with event name", func() {
				destType := SNOWFLAKE
				WarehouseConfig := warehouses[destType]
				batchJson := helpers.AddKeyToJSON(helpers.WarehouseBatchPayload, "batch.0.event", eventName)
				anonymousId:= uuid.NewV4().String()
				batchJson =  helpers.AddKeyToJSON(batchJson, "batch.0.anonymousId", anonymousId)
				helpers.SendBatchRequest(writeKey, batchJson)
				loadTablesFromAboveTrackJson := []string{"TRACKS", strings.Replace(strings.ToUpper(eventName), " ", "_", -1)}
				Eventually(func() bool {
					loadedTables := helpers.GetLoadFileTableName(dbHandle, warehouseLoadFilesTable, WarehouseConfig[0].Source.ID, WarehouseConfig[0].Destination.ID, destType)
					return helpers.IsThisInThatSliceString(loadTablesFromAboveTrackJson, loadedTables)
				}, loadTablesTimeout, pollIntervalForLoadTables).Should(Equal(true))
				//By("should have same data for load file as the event payload sent")
				//loadedFileData := helpers.GetEventLoadFileData(dbHandle, warehouseLoadFilesTable, eventName, bucket)
				//Expect(gjson.Get(batchJson, "batch.0.event").String()).Should(Equal(gjson.Get(loadedFileData, "event").String()))
				//By("should be able to create load files if source has two warehouse destinations")
				//loadedDestinationIDs := helpers.GetDestinationIDsFromLoadFileTable(dbHandle, warehouseLoadFilesTable, sourceIDs[0])
				//Expect(helpers.IsThisInThatSliceString(destinationsIDs, loadedDestinationIDs)).Should(Equal(true))
				By("should be able to upload to bq with state exported_data")
				var namespace string
				var state string
				Eventually(func() string {
					namespace,state=helpers.FetchUpdateState(dbHandle, warehouseUploadsTable, WarehouseConfig[0].Source.ID, WarehouseConfig[0].Destination.ID, destType)
					fmt.Println(namespace)
					return state
				}, loadTablesTimeout, pollIntervalForLoadTables).Should(Equal(exportedDataState))
				By("should be able to query with anonymousId and compare properties and timestamps")
				Eventually(func() string{
					payload:=helpers.QueryWarehouseWithAnonymusID(anonymousId, eventName, namespace, destType, WarehouseConfig[0].Destination.Config)
					return payload.Label
				}, loadTablesTimeout, pollIntervalForLoadTables).Should(Equal(gjson.Get(batchJson, "batch.0.properties.label" ).String()))

			})
		})

	})
	//Describe("Compatible with segment warehouse schema", func() {
	//	BeforeEach(func(){
	//		helpers.DeleteRowsInTables(dbHandle, warehouseTables)
	//	})
	//	It("should be able to create tables ", func() {
	//		helpers.SendTrackRequest(writeKey, helpers.AddKeyToJSON(helpers.RemoveKeyFromJSON(helpers.TrackPayload, "messageId", "anonymousId"), "event", eventName))
	//		helpers.SendIdentifyRequest(writeKey, helpers.RemoveKeyFromJSON(helpers.IdentifyPayload, "messageId", "anonymousId"))
	//		helpers.SendPageRequest(writeKey, helpers.RemoveKeyFromJSON(helpers.PagePayload, "messageId", "anonymousId"))
	//		helpers.SendAliasRequest(writeKey, helpers.RemoveKeyFromJSON(helpers.AliasPayload, "messageId", "anonymousId"))
	//		helpers.SendGroupRequest(writeKey, helpers.RemoveKeyFromJSON(helpers.GroupPayload, "messageId", "anonymousId"))
	//		helpers.SendScreenRequest(writeKey, helpers.RemoveKeyFromJSON(helpers.ScreenPayload, "messageId", "anonymousId"))
	//		tables := []string{"identifies", "users", "pages", "tracks", "screens","_groups","aliases", strings.Replace(strings.ToLower(eventName), " ", "_", -1)} // group, alias are not supported.
	//		Eventually(func() bool {
	//			loadedTables := helpers.GetLoadFileTableName(dbHandle, warehouseLoadFilesTable)
	//			return helpers.IsThisInThatSliceString(tables, loadedTables)
	//		}, loadTablesTimeout, pollIntervalForLoadTables).Should(Equal(true))
	//	})
	//})
	//Describe("testing with different string formats", func() {
	//	BeforeEach(func(){
	//		helpers.DeleteRowsInTables(dbHandle, warehouseTables)
	//	})
	//	It("should be able to create load file", func() {
	//		helpers.SendBatchRequest(writeKey, helpers.AddKeyToJSON(helpers.RemoveKeyFromJSON(helpers.DiffStringFormatBatchPayload, "batch.0.messageId", "batch.0.anonymousId"), "batch.0.event", eventName))
	//		eventName := strings.Replace(strings.ToLower(eventName), " ", "_", -1)
	//		Eventually(func() bool {
	//			loadedTables := helpers.GetLoadFileTableName(dbHandle, warehouseLoadFilesTable)
	//			return helpers.IsThisInThatSliceString([]string{eventName}, loadedTables)
	//		}, loadTablesTimeout, pollIntervalForLoadTables).Should(Equal(true))
	//	})
	//})
	//Describe("sending different data types for a key consecutively", func() {
	//	BeforeEach(func(){
	//		helpers.DeleteRowsInTables(dbHandle, warehouseTables)
	//	})
	//	It("should be able to create load file and schema should have different data types", func() {
	//		helpers.SendTrackRequest(writeKey, helpers.AddKeyToJSON(helpers.RemoveKeyFromJSON(gjson.Get(helpers.DTBatchPayload, "batch.0").String(), "messageId", "anonymousId"), "event", eventName))
	//		helpers.SendTrackRequest(writeKey, helpers.AddKeyToJSON(helpers.RemoveKeyFromJSON(gjson.Get(helpers.DTBatchPayload, "batch.1").String(), "messageId", "anonymousId"), "event", eventName))
	//		helpers.SendTrackRequest(writeKey, helpers.AddKeyToJSON(helpers.RemoveKeyFromJSON(gjson.Get(helpers.DTBatchPayload, "batch.2").String(), "messageId", "anonymousId"), "event", eventName))
	//		eventName = strings.Replace(strings.ToLower(eventName), " ", "_", -1)
	//		Eventually(func() bool {
	//			loadedTables := helpers.GetLoadFileTableName(dbHandle, warehouseLoadFilesTable)
	//			return helpers.IsThisInThatSliceString([]string{eventName}, loadedTables)
	//		}, loadTablesTimeout, pollIntervalForLoadTables).Should(Equal(true))
	//		Eventually(func() bool {
	//			loadedSchema := helpers.GetWarehouseSchema(dbHandle, warehouseSchemasTable, sourceIDs[0], destinationsIDs[0])
	//			return reflect.DeepEqual(loadedSchema, helpers.DTSchema)
	//		}, loadTablesTimeout, pollIntervalForLoadTables).Should(Equal(true))
	//	})
	//})
	//Describe("Reserved Keywords as one of keys in an event should be replaced by _key", func() {
	//	BeforeEach(func(){
	//		helpers.DeleteRowsInTables(dbHandle, warehouseTables)
	//	})
	//	It("should be able to create load file, while sending reservered keywords", func() {
	//		helpers.SendBatchRequest(writeKey, helpers.RemoveKeyFromJSON(helpers.BQBatchPayload, "batch.0.messageId", "batch.0.anonymousId"))
	//		eventName := gjson.Get(helpers.BQBatchPayload, "batch.0.event").String()
	//		eventName = strings.Replace(strings.ToLower(eventName), " ", "_", -1)
	//		Eventually(func() bool {
	//			loadedTables := helpers.GetLoadFileTableName(dbHandle, warehouseLoadFilesTable)
	//			return helpers.IsThisInThatSliceString([]string{"_" + eventName}, loadedTables)
	//		}, loadTablesTimeout, pollIntervalForLoadTables).Should(Equal(true))
	//	})
	//})
})
