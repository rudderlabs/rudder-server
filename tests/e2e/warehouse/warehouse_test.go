package warehouse_test

import (
	"database/sql"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/tests/helpers"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	uuid "github.com/satori/go.uuid"
	"github.com/tidwall/gjson"
	"reflect"
	"strings"
)

var dbHandle *sql.DB
var gatewayDBPrefix string
var routerDBPrefix string
var dbPollFreqInS int = 1
var pollIntervalForLoadTables int = 10
var loadTablesTimeout int = 200
var eventName string = "ginkgo"
var warehouseTables []string
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
	Describe("By sending a generic track event, it should be able to create load files in db and upload in warehouses ", func() {
		BeforeEach(func(){
			helpers.DeleteRowsInTables(dbHandle, warehouseTables)
		})

		It("should able to create a load file in database with event name", func() {
			batchJson := helpers.AddKeyToJSON(helpers.WarehouseBatchPayload, "batch.0.event", eventName)
			anonymousId:= uuid.NewV4().String()
			batchJson =  helpers.AddKeyToJSON(batchJson, "batch.0.anonymousId", anonymousId)
			helpers.SendBatchRequest(writeKey, batchJson)

			By("test to create a load file in database for BQ destination")
			Eventually(func() bool {
				destType := BQ
				WarehouseConfig := warehouses[destType]
				tables := []string{"tracks", strings.Replace(strings.ToLower(eventName), " ", "_", -1)}
				loadedTables := helpers.GetLoadFileTableName(dbHandle, warehouseLoadFilesTable, WarehouseConfig[0].Source.ID, WarehouseConfig[0].Destination.ID, destType)
				return helpers.IsThisInThatSliceString(tables, loadedTables)
			}, loadTablesTimeout, pollIntervalForLoadTables).Should(Equal(true))
			By("test to create a load file in database for RS destination")
			Eventually(func() bool {
				destType := RS
				WarehouseConfig := warehouses[destType]
				tables := []string{"tracks", strings.Replace(strings.ToLower(eventName), " ", "_", -1)}
				loadedTables := helpers.GetLoadFileTableName(dbHandle, warehouseLoadFilesTable, WarehouseConfig[0].Source.ID, WarehouseConfig[0].Destination.ID, destType)
				return helpers.IsThisInThatSliceString(tables, loadedTables)
			}, loadTablesTimeout, pollIntervalForLoadTables).Should(Equal(true))
			By("test to create a load file in database for SNOWFLAKE destination")
			Eventually(func() bool {
				destType := SNOWFLAKE
				WarehouseConfig := warehouses[destType]
				tables := []string{"TRACKS", strings.Replace(strings.ToUpper(eventName), " ", "_", -1)}
				loadedTables := helpers.GetLoadFileTableName(dbHandle, warehouseLoadFilesTable, WarehouseConfig[0].Source.ID, WarehouseConfig[0].Destination.ID, destType)
				return helpers.IsThisInThatSliceString(tables, loadedTables)
			}, loadTablesTimeout, pollIntervalForLoadTables).Should(Equal(true))
			By("test create load files if source has two warehouse destinations")
			Eventually(func() bool {
				destType := BQ
				WarehouseConfig := warehouses[destType]
				loadedDestinationIDs := helpers.GetDestinationIDsFromLoadFileTable(dbHandle, warehouseLoadFilesTable, WarehouseConfig[0].Source.ID)
				return helpers.IsThisInThatSliceString([]string{WarehouseConfig[0].Destination.ID, WarehouseConfig[1].Destination.ID}, loadedDestinationIDs)
			}, loadTablesTimeout, pollIntervalForLoadTables).Should(Equal(true))
			By("test to upload to BQ with state exported_data in db")
			Eventually(func() string {
				destType := BQ
				WarehouseConfig := warehouses[destType]
				_,_,state:=helpers.FetchUpdateState(dbHandle, warehouseUploadsTable, WarehouseConfig[0].Source.ID, WarehouseConfig[0].Destination.ID, destType)
				return state
			}, loadTablesTimeout, pollIntervalForLoadTables).Should(Equal(exportedDataState))
			By("test to upload to RS with state exported_data in db")
			Eventually(func() string {
				destType := BQ
				WarehouseConfig := warehouses[destType]
				_,_,state:=helpers.FetchUpdateState(dbHandle, warehouseUploadsTable, WarehouseConfig[0].Source.ID, WarehouseConfig[0].Destination.ID, destType)
				return state
			}, loadTablesTimeout, pollIntervalForLoadTables).Should(Equal(exportedDataState))
			By("test to upload to SNOWFLAKE with state exported_data in db")
			Eventually(func() string {
				destType := BQ
				WarehouseConfig := warehouses[destType]
				_,_,state:=helpers.FetchUpdateState(dbHandle, warehouseUploadsTable, WarehouseConfig[0].Source.ID, WarehouseConfig[0].Destination.ID, destType)
				return state
			}, loadTablesTimeout, pollIntervalForLoadTables).Should(Equal(exportedDataState))

			By("test to query with anonymousId and compare properties on table eventName in BQ")
			Eventually(func() string{
				destType := BQ
				WarehouseConfig := warehouses[destType]
				_,namespace,_:=helpers.FetchUpdateState(dbHandle, warehouseUploadsTable, WarehouseConfig[0].Source.ID, WarehouseConfig[0].Destination.ID, destType)
				payload := helpers.QueryWarehouseWithAnonymusID(anonymousId, eventName, namespace, destType, WarehouseConfig[0].Destination.Config)
				return payload.Label
			}, loadTablesTimeout, pollIntervalForLoadTables).Should(Equal(gjson.Get(batchJson, "batch.0.properties.label" ).String()))
			By("test to query with anonymousId and compare properties on table eventName in RS")
			Eventually(func() string{
				destType := RS
				WarehouseConfig := warehouses[destType]
				_,namespace,_:=helpers.FetchUpdateState(dbHandle, warehouseUploadsTable, WarehouseConfig[0].Source.ID, WarehouseConfig[0].Destination.ID, destType)
				payload := helpers.QueryWarehouseWithAnonymusID(anonymousId, eventName, namespace, destType, WarehouseConfig[0].Destination.Config)
				return payload.Label
			}, loadTablesTimeout, pollIntervalForLoadTables).Should(Equal(gjson.Get(batchJson, "batch.0.properties.label" ).String()))
			By("test to query with anonymousId and compare properties on table eventName in SNOWFLAKE")
			Eventually(func() string{
				destType := SNOWFLAKE
				WarehouseConfig := warehouses[destType]
				_,namespace,_:=helpers.FetchUpdateState(dbHandle, warehouseUploadsTable, WarehouseConfig[0].Source.ID, WarehouseConfig[0].Destination.ID, destType)
				payload := helpers.QueryWarehouseWithAnonymusID(anonymousId, eventName, namespace, destType, WarehouseConfig[0].Destination.Config)
				return payload.Label
			}, loadTablesTimeout, pollIntervalForLoadTables).Should(Equal(gjson.Get(batchJson, "batch.0.properties.label" ).String()))
		})
	})
	Describe("Compatible with segment warehouse schema, verifying different api calls like track, identity, etc", func() {
		BeforeEach(func(){
			helpers.DeleteRowsInTables(dbHandle, warehouseTables)
		})
		It("should be able to create tables with all api schema types ", func() {
			helpers.SendTrackRequest(writeKey, helpers.AddKeyToJSON(helpers.RemoveKeyFromJSON(helpers.TrackPayload, "messageId", "anonymousId"), "event", eventName))
			helpers.SendIdentifyRequest(writeKey, helpers.RemoveKeyFromJSON(helpers.IdentifyPayload, "messageId", "anonymousId"))
			helpers.SendPageRequest(writeKey, helpers.RemoveKeyFromJSON(helpers.PagePayload, "messageId", "anonymousId"))
			helpers.SendAliasRequest(writeKey, helpers.RemoveKeyFromJSON(helpers.AliasPayload, "messageId", "anonymousId"))
			helpers.SendGroupRequest(writeKey, helpers.RemoveKeyFromJSON(helpers.GroupPayload, "messageId", "anonymousId"))
			helpers.SendScreenRequest(writeKey, helpers.RemoveKeyFromJSON(helpers.ScreenPayload, "messageId", "anonymousId"))
			By("test to create a load file in database for BQ destination")
			Eventually(func() bool {
				destType := BQ
				WarehouseConfig := warehouses[destType]
				tables := []string{"identifies", "users", "pages", "tracks", "screens","_groups","aliases", strings.Replace(strings.ToLower(eventName), " ", "_", -1)}
				loadedTables := helpers.GetLoadFileTableName(dbHandle, warehouseLoadFilesTable, WarehouseConfig[0].Source.ID, WarehouseConfig[0].Destination.ID, destType)
				return helpers.IsThisInThatSliceString(tables, loadedTables)
			}, loadTablesTimeout, pollIntervalForLoadTables).Should(Equal(true))
			By("test to upload to BQ with state exported_data in db")
			Eventually(func() string {
				destType := BQ
				WarehouseConfig := warehouses[destType]
				_,_,state:=helpers.FetchUpdateState(dbHandle, warehouseUploadsTable, WarehouseConfig[0].Source.ID, WarehouseConfig[0].Destination.ID, destType)
				return state
			}, loadTablesTimeout, pollIntervalForLoadTables).Should(Equal(exportedDataState))
			By("test to upload to BQ with state exported_data in db and should have created tables in db which are updated")
			Eventually(func() bool {
				destType := BQ
				WarehouseConfig := warehouses[destType]
				uploadId,_,state:=helpers.FetchUpdateState(dbHandle, warehouseUploadsTable, WarehouseConfig[0].Source.ID, WarehouseConfig[0].Destination.ID, destType)
				updatedTables:=helpers.VerifyUpdatedTables(dbHandle, warehouseTableUploadsTable, uploadId ,state)
				tables := []string{"identifies", "users", "pages", "tracks", "screens","_groups","aliases", strings.Replace(strings.ToLower(eventName), " ", "_", -1)}
				return helpers.IsThisInThatSliceString(tables, updatedTables)
			}, loadTablesTimeout, pollIntervalForLoadTables).Should(Equal(true))
			By("test to create a load file in database for RS destination")
			Eventually(func() bool {
				destType := RS
				WarehouseConfig := warehouses[destType]
				tables := []string{"identifies", "users", "pages", "tracks", "screens","groups","aliases", strings.Replace(strings.ToLower(eventName), " ", "_", -1)}
				loadedTables := helpers.GetLoadFileTableName(dbHandle, warehouseLoadFilesTable, WarehouseConfig[0].Source.ID, WarehouseConfig[0].Destination.ID, destType)
				return helpers.IsThisInThatSliceString(tables, loadedTables)
			}, loadTablesTimeout, pollIntervalForLoadTables).Should(Equal(true))
			By("test to upload to RS with state exported_data in db")
			Eventually(func() string {
				destType := RS
				WarehouseConfig := warehouses[destType]
				_,_,state:=helpers.FetchUpdateState(dbHandle, warehouseUploadsTable, WarehouseConfig[0].Source.ID, WarehouseConfig[0].Destination.ID, destType)
				return state
			}, loadTablesTimeout, pollIntervalForLoadTables).Should(Equal(exportedDataState))
			By("test to upload to RS with state exported_data in db and should have created tables in db which are updated")
			Eventually(func() bool {
				destType := RS
				WarehouseConfig := warehouses[destType]
				uploadId,_,state:=helpers.FetchUpdateState(dbHandle, warehouseUploadsTable, WarehouseConfig[0].Source.ID, WarehouseConfig[0].Destination.ID, destType)
				updatedTables:=helpers.VerifyUpdatedTables(dbHandle, warehouseTableUploadsTable, uploadId ,state)
				tables := []string{"identifies", "users", "pages", "tracks", "screens","groups","aliases", strings.Replace(strings.ToLower(eventName), " ", "_", -1)}
				return helpers.IsThisInThatSliceString(tables, updatedTables)
			}, loadTablesTimeout, pollIntervalForLoadTables).Should(Equal(true))
			By("test to create a load file in database for SNOWFLAKE destination")
			Eventually(func() bool {
				destType := SNOWFLAKE
				WarehouseConfig := warehouses[destType]
				tables := []string{"IDENTIFIES", "USERS", "PAGES", "TRACKS", "SCREENS","GROUPS","ALIASES", strings.Replace(strings.ToUpper(eventName), " ", "_", -1)}
				loadedTables := helpers.GetLoadFileTableName(dbHandle, warehouseLoadFilesTable, WarehouseConfig[0].Source.ID, WarehouseConfig[0].Destination.ID, destType)
				return helpers.IsThisInThatSliceString(tables, loadedTables)
			}, loadTablesTimeout, pollIntervalForLoadTables).Should(Equal(true))
			By("test to upload to SNOWFLAKE with state exported_data in db")
			Eventually(func() string {
				destType := SNOWFLAKE
				WarehouseConfig := warehouses[destType]
				_,_,state:=helpers.FetchUpdateState(dbHandle, warehouseUploadsTable, WarehouseConfig[0].Source.ID, WarehouseConfig[0].Destination.ID, destType)
				return state
			}, loadTablesTimeout, pollIntervalForLoadTables).Should(Equal(exportedDataState))
			By("test to upload to SNOWFLAKE with state exported_data in db and should have created tables in db which are updated")
			Eventually(func() bool {
				destType := SNOWFLAKE
				WarehouseConfig := warehouses[destType]
				uploadId,_,state:=helpers.FetchUpdateState(dbHandle, warehouseUploadsTable, WarehouseConfig[0].Source.ID, WarehouseConfig[0].Destination.ID, destType)
				updatedTables:=helpers.VerifyUpdatedTables(dbHandle, warehouseTableUploadsTable, uploadId ,state)
				tables := []string{"IDENTIFIES", "USERS", "PAGES", "TRACKS", "SCREENS","GROUPS","ALIASES", strings.Replace(strings.ToUpper(eventName), " ", "_", -1)}
				return helpers.IsThisInThatSliceString(tables, updatedTables)
			}, loadTablesTimeout, pollIntervalForLoadTables).Should(Equal(true))
		})

	})
	Describe("testing with different string formats", func() {
		BeforeEach(func(){
			helpers.DeleteRowsInTables(dbHandle, warehouseTables)
		})
		It("should be able to create load file", func() {
			batchJson := helpers.AddKeyToJSON(helpers.WarehouseBatchPayload, "batch.0.event", eventName)
			anonymousId:= uuid.NewV4().String()
			batchJson =  helpers.AddKeyToJSON(batchJson, "batch.0.anonymousId", anonymousId)
			label := "Ken\"ny\"s iPh'o\"ne5\",6"
			batchJson =  helpers.AddKeyToJSON(batchJson, "batch.0.properties.label", label)
			eventName := strings.Replace(strings.ToLower(eventName), " ", "_", -1)
			helpers.SendBatchRequest(writeKey, batchJson)
			By("test to upload to BQ with state exported_data in db")
			Eventually(func() string {
				destType := BQ
				WarehouseConfig := warehouses[destType]
				_,_,state:=helpers.FetchUpdateState(dbHandle, warehouseUploadsTable, WarehouseConfig[0].Source.ID, WarehouseConfig[0].Destination.ID, destType)
				return state
			}, loadTablesTimeout, pollIntervalForLoadTables).Should(Equal(exportedDataState))
			By("test to query with anonymousId and compare properties and timestamps on table eventName on BQ")
			Eventually(func() string{
				destType := BQ
				WarehouseConfig := warehouses[destType]
				_,namespace,_:=helpers.FetchUpdateState(dbHandle, warehouseUploadsTable, WarehouseConfig[0].Source.ID, WarehouseConfig[0].Destination.ID, destType)
				payload := helpers.QueryWarehouseWithAnonymusID(anonymousId, eventName, namespace, destType, WarehouseConfig[0].Destination.Config)
				return payload.Label
			}, loadTablesTimeout, pollIntervalForLoadTables).Should(Equal(label ))
			By("test to upload to RS with state exported_data in db")
			Eventually(func() string {
				destType := RS
				WarehouseConfig := warehouses[destType]
				_,_,state:=helpers.FetchUpdateState(dbHandle, warehouseUploadsTable, WarehouseConfig[0].Source.ID, WarehouseConfig[0].Destination.ID, destType)
				return state
			}, loadTablesTimeout, pollIntervalForLoadTables).Should(Equal(exportedDataState))
			By("test to query with anonymousId and compare properties and timestamps on table eventName on RS")
			Eventually(func() string{
				destType := RS
				WarehouseConfig := warehouses[destType]
				_,namespace,_:=helpers.FetchUpdateState(dbHandle, warehouseUploadsTable, WarehouseConfig[0].Source.ID, WarehouseConfig[0].Destination.ID, destType)
				payload := helpers.QueryWarehouseWithAnonymusID(anonymousId, eventName, namespace, destType, WarehouseConfig[0].Destination.Config)
				return payload.Label
			}, loadTablesTimeout, pollIntervalForLoadTables).Should(Equal(label ))
			By("test to upload to SNOWFLAKE with state exported_data in db")
			Eventually(func() string {
				destType := SNOWFLAKE
				WarehouseConfig := warehouses[destType]
				_,_,state:=helpers.FetchUpdateState(dbHandle, warehouseUploadsTable, WarehouseConfig[0].Source.ID, WarehouseConfig[0].Destination.ID, destType)
				return state
			}, loadTablesTimeout, pollIntervalForLoadTables).Should(Equal(exportedDataState))
			By("test to query with anonymousId and compare properties and timestamps on table eventName on SNOWFLAKE")
			Eventually(func() string{
				destType := SNOWFLAKE
				WarehouseConfig := warehouses[destType]
				_,namespace,_:=helpers.FetchUpdateState(dbHandle, warehouseUploadsTable, WarehouseConfig[0].Source.ID, WarehouseConfig[0].Destination.ID, destType)
				payload := helpers.QueryWarehouseWithAnonymusID(anonymousId, eventName, namespace, destType, WarehouseConfig[0].Destination.Config)
				return payload.Label
			}, loadTablesTimeout, pollIntervalForLoadTables).Should(Equal(label ))

		})
	})
	Describe("sending different data types for a key consecutively", func() {
		BeforeEach(func(){
			helpers.DeleteRowsInTables(dbHandle, warehouseTables)
		})
		It("should be able to create load file and schema should have different data types", func() {
			batchJson := helpers.AddKeyToJSON(helpers.WarehouseBatchPayload, "batch.0.event", eventName)
			anonymousId:= uuid.NewV4().String()
			batchJson =  helpers.AddKeyToJSON(batchJson, "batch.0.anonymousId", anonymousId)
			batchJson = helpers.AddKeyToJSON(batchJson, "batch.0.properties.label","Demo")
			helpers.SendBatchRequest(writeKey, batchJson)
			batchJson = helpers.AddKeyToJSON(batchJson, "batch.0.properties.label",1)
			helpers.SendBatchRequest(writeKey, batchJson)
			batchJson = helpers.AddKeyToJSON(batchJson, "batch.0.properties.label",5.03)
			helpers.SendBatchRequest(writeKey, batchJson)
			batchJson = helpers.AddKeyToJSON(batchJson, "batch.0.properties.value",5)
			helpers.SendBatchRequest(writeKey, batchJson)
			batchJson = helpers.AddKeyToJSON(batchJson, "batch.0.properties.value",5.03)
			helpers.SendBatchRequest(writeKey, batchJson)
			batchJson = helpers.AddKeyToJSON(batchJson, "batch.0.properties.value","5.03")
			helpers.SendBatchRequest(writeKey, batchJson)
			batchJson = helpers.AddKeyToJSON(batchJson, "batch.0.properties.value","omega")
			helpers.SendBatchRequest(writeKey, batchJson)
			By("test to upload to BQ with state exported_data in db")
			Eventually(func() string {
				destType := BQ
				WarehouseConfig := warehouses[destType]
				_,_,state:=helpers.FetchUpdateState(dbHandle, warehouseUploadsTable, WarehouseConfig[0].Source.ID, WarehouseConfig[0].Destination.ID, destType)
				return state
			}, loadTablesTimeout, pollIntervalForLoadTables).Should(Equal(exportedDataState))
			By("test to upload to BQ with state exported_data in db and match with the schema")
			Eventually(func() bool {
				destType := BQ
				WarehouseConfig := warehouses[destType]
				uploadId,_,_:=helpers.FetchUpdateState(dbHandle, warehouseUploadsTable, WarehouseConfig[0].Source.ID, WarehouseConfig[0].Destination.ID, destType)
				uploadedSchema := helpers.GetWarehouseSchema(dbHandle,warehouseSchemasTable,uploadId,WarehouseConfig[0].Source.ID, WarehouseConfig[0].Destination.ID)
				return reflect.DeepEqual(uploadedSchema,helpers.BigQuerySchema)
			}, loadTablesTimeout, pollIntervalForLoadTables).Should(Equal(true))
			By("test to upload to RS with state exported_data in db")
			Eventually(func() string {
				destType := RS
				WarehouseConfig := warehouses[destType]
				_,_,state:=helpers.FetchUpdateState(dbHandle, warehouseUploadsTable, WarehouseConfig[0].Source.ID, WarehouseConfig[0].Destination.ID, destType)
				return state
			}, loadTablesTimeout, pollIntervalForLoadTables).Should(Equal(exportedDataState))
			By("test to upload to RS with state exported_data in db and match with the schema")
			Eventually(func() bool {
				destType := RS
				WarehouseConfig := warehouses[destType]
				uploadId,_,_:=helpers.FetchUpdateState(dbHandle, warehouseUploadsTable, WarehouseConfig[0].Source.ID, WarehouseConfig[0].Destination.ID, destType)
				uploadedSchema := helpers.GetWarehouseSchema(dbHandle,warehouseSchemasTable,uploadId,WarehouseConfig[0].Source.ID, WarehouseConfig[0].Destination.ID)
				return reflect.DeepEqual(uploadedSchema,helpers.RedshiftSchema)
			}, loadTablesTimeout, pollIntervalForLoadTables).Should(Equal(true))
			By("test to upload to SNOWFLAKE with state exported_data in db")
			Eventually(func() string {
				destType := SNOWFLAKE
				WarehouseConfig := warehouses[destType]
				_,_,state:=helpers.FetchUpdateState(dbHandle, warehouseUploadsTable, WarehouseConfig[0].Source.ID, WarehouseConfig[0].Destination.ID, destType)
				return state
			}, loadTablesTimeout, pollIntervalForLoadTables).Should(Equal(exportedDataState))
			By("test to upload to SNOWFLAKE with state exported_data in db and match with the schema")
			Eventually(func() bool {
				destType := SNOWFLAKE
				WarehouseConfig := warehouses[destType]
				uploadId,_,_:=helpers.FetchUpdateState(dbHandle, warehouseUploadsTable, WarehouseConfig[0].Source.ID, WarehouseConfig[0].Destination.ID, destType)
				uploadedSchema := helpers.GetWarehouseSchema(dbHandle,warehouseSchemasTable,uploadId,WarehouseConfig[0].Source.ID, WarehouseConfig[0].Destination.ID)
				return reflect.DeepEqual(uploadedSchema,helpers.SnowflakeSchema)
			}, loadTablesTimeout, pollIntervalForLoadTables).Should(Equal(true))
		})
	})
	Describe("Reserved Keywords as one of keys in an event should be replaced by _key", func() {
		BeforeEach(func(){
			helpers.DeleteRowsInTables(dbHandle, warehouseTables)
		})
		It("should be able to create load file, while sending reserved keywords", func() {
			batchJson := helpers.AddKeyToJSON(helpers.WarehouseBatchPayload, "batch.0.event", eventName)
			anonymousId:= uuid.NewV4().String()
			batchJson =  helpers.AddKeyToJSON(batchJson, "batch.0.anonymousId", anonymousId)
			eventName = strings.Replace(strings.ToLower(eventName), " ", "_", -1)
			property1 := "join"
			property2 := "select"
			property3 := "where"
			property4 := "order"
			property5 := "from"
			batchJson =  helpers.AddKeyToJSON(batchJson, "batch.0.properties." + property1, property1)
			batchJson =  helpers.AddKeyToJSON(batchJson, "batch.0.properties." + property2, property2)
			batchJson =  helpers.AddKeyToJSON(batchJson, "batch.0.properties." + property3, property3)
			batchJson =  helpers.AddKeyToJSON(batchJson, "batch.0.properties." + property4, property4)
			batchJson =  helpers.AddKeyToJSON(batchJson, "batch.0.properties." + property5, property5)
			helpers.SendBatchRequest(writeKey, batchJson)
			By("test to upload to BQ with state exported_data in db")
			Eventually(func() string {
				destType := BQ
				WarehouseConfig := warehouses[destType]
				_,_,state:=helpers.FetchUpdateState(dbHandle, warehouseUploadsTable, WarehouseConfig[0].Source.ID, WarehouseConfig[0].Destination.ID, destType)
				return state
			}, loadTablesTimeout, pollIntervalForLoadTables).Should(Equal(exportedDataState))
			By("test to upload to BQ with state exported_data in db and match with the schema")
			Eventually(func() bool {
				destType := BQ
				WarehouseConfig := warehouses[destType]
				uploadId,_,_:=helpers.FetchUpdateState(dbHandle, warehouseUploadsTable, WarehouseConfig[0].Source.ID, WarehouseConfig[0].Destination.ID, destType)
				uploadedSchema := helpers.GetWarehouseSchema(dbHandle,warehouseSchemasTable,uploadId,WarehouseConfig[0].Source.ID, WarehouseConfig[0].Destination.ID)
				return reflect.DeepEqual(uploadedSchema,helpers.ReserverKeyWordsBigQuerySchema)
			}, loadTablesTimeout, pollIntervalForLoadTables).Should(Equal(true))
			By("test to upload to RS with state exported_data in db")
			Eventually(func() string {
				destType := RS
				WarehouseConfig := warehouses[destType]
				_,_,state:=helpers.FetchUpdateState(dbHandle, warehouseUploadsTable, WarehouseConfig[0].Source.ID, WarehouseConfig[0].Destination.ID, destType)
				return state
			}, loadTablesTimeout, pollIntervalForLoadTables).Should(Equal(exportedDataState))
			By("test to upload to RS with state exported_data in db and match with the schema")
			Eventually(func() bool {
				destType := RS
				WarehouseConfig := warehouses[destType]
				uploadId,_,_:=helpers.FetchUpdateState(dbHandle, warehouseUploadsTable, WarehouseConfig[0].Source.ID, WarehouseConfig[0].Destination.ID, destType)
				uploadedSchema := helpers.GetWarehouseSchema(dbHandle,warehouseSchemasTable,uploadId,WarehouseConfig[0].Source.ID, WarehouseConfig[0].Destination.ID)
				return reflect.DeepEqual(uploadedSchema,helpers.ReservedKeywordsRedshiftSchema)
			}, loadTablesTimeout, pollIntervalForLoadTables).Should(Equal(true))
			By("test to upload to SNOWFLAKE with state exported_data in db")
			Eventually(func() string {
				destType := SNOWFLAKE
				WarehouseConfig := warehouses[destType]
				_,_,state:=helpers.FetchUpdateState(dbHandle, warehouseUploadsTable, WarehouseConfig[0].Source.ID, WarehouseConfig[0].Destination.ID, destType)
				return state
			}, loadTablesTimeout, pollIntervalForLoadTables).Should(Equal(exportedDataState))
			By("test to upload to SNOWFLAKE with state exported_data in db and match with the schema")
			Eventually(func() bool {
				destType := SNOWFLAKE
				WarehouseConfig := warehouses[destType]
				uploadId,_,_:=helpers.FetchUpdateState(dbHandle, warehouseUploadsTable, WarehouseConfig[0].Source.ID, WarehouseConfig[0].Destination.ID, destType)
				uploadedSchema := helpers.GetWarehouseSchema(dbHandle,warehouseSchemasTable,uploadId,WarehouseConfig[0].Source.ID, WarehouseConfig[0].Destination.ID)
				return reflect.DeepEqual(uploadedSchema,helpers.ReservedKeywordsSnowflakeSchema)
			}, loadTablesTimeout, pollIntervalForLoadTables).Should(Equal(true))
		})
	})
})
