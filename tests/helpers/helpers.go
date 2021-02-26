package helpers

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/rudderlabs/rudder-server/jobsdb"
	uuid "github.com/satori/go.uuid"
	"github.com/segmentio/ksuid"
	"github.com/tidwall/sjson"
)

const (
	serverIP = "http://localhost:8080"
)

// EventOptsT is the type specifying override options over sample event.json
type EventOptsT struct {
	Integrations map[string]bool
	WriteKey     string
	ID           string
	MessageID    string
	GaVal        int
}

type QueryTrackPayload struct{
	Label string `json:"label"`
	Category string `json:"category"`
	Property1 string `json:"property1"`
	Property2 string `json:"property2"`
	Property3 string `json:"property3"`
	Property4 string `json:"property4"`
	Property5 string `json:"property5"`
}

//RemoveKeyFromJSON returns the json with keys removed from the input json
func RemoveKeyFromJSON(json string, keys ...string) string {
	for _, key := range keys {
		var err error
		json, err = sjson.Delete(json, key)
		if err != nil {
			panic(err)
		}
	}
	return json
}

// AddKeyToJSON adds a key and its value to the
func AddKeyToJSON(json string, key string, value interface{}) string {
	json, _ = sjson.Set(json, key, value)
	return json
}

// SendEventRequest sends sample event.json with EventOptionsT overrides to gateway server
func SendEventRequest(options EventOptsT) int {
	if options.Integrations == nil {
		options.Integrations = map[string]bool{
			"All": true,
		}
	}

	//Source with WriteKey: 1Yc6YbOGg6U2E8rlj97ZdOawPyr has one S3 and one GA as destinations. Using this WriteKey as default.
	if options.WriteKey == "" {
		options.WriteKey = "1Yc6YbOGg6U2E8rlj97ZdOawPyr"
	}
	if options.ID == "" {
		options.ID = ksuid.New().String()
	}
	if options.MessageID == "" {
		options.MessageID = uuid.NewV4().String()
	}

	jsonPayload, _ := sjson.Set(BatchPayload, "batch.0.sentAt", time.Now())
	jsonPayload, _ = sjson.Set(jsonPayload, "batch.0.integrations", options.Integrations)
	jsonPayload, _ = sjson.Set(jsonPayload, "batch.0.anonymousId", options.ID)
	jsonPayload, _ = sjson.Set(jsonPayload, "batch.0.messageId", options.MessageID)
	jsonPayload, _ = sjson.Set(jsonPayload, "batch.0.properties.value", options.GaVal)

	return SendBatchRequest(options.WriteKey, jsonPayload)
}

// SendLoadRequests Send bunch of requests to server
func SendLoadRequests() {
	for i := 0; i < 100000; i++ {
		options := EventOptsT{GaVal: 3}
		fmt.Println("request making...")
		SendEventRequest(options)
	}
}

// SendBatchRequest sends request to /v1/batch
func SendBatchRequest(userNameForBasicAuth, jsonPayload string) int {
	return SendRequest("/v1/batch", userNameForBasicAuth, jsonPayload)
}

// SendIdentifyRequest sends request to /v1/identify
func SendIdentifyRequest(userNameForBasicAuth, jsonPayload string) int {
	return SendRequest("/v1/identify", userNameForBasicAuth, jsonPayload)
}

// SendTrackRequest sends request to /v1/track
func SendTrackRequest(userNameForBasicAuth, jsonPayload string) int {
	return SendRequest("/v1/track", userNameForBasicAuth, jsonPayload)
}

// SendPageRequest sends request to /v1/page
func SendPageRequest(userNameForBasicAuth, jsonPayload string) int {
	return SendRequest("/v1/page", userNameForBasicAuth, jsonPayload)
}

// SendScreenRequest sends request to /v1/screen
func SendScreenRequest(userNameForBasicAuth, jsonPayload string) int {
	return SendRequest("/v1/screen", userNameForBasicAuth, jsonPayload)
}

// SendAliasRequest sends request to /v1/alias
func SendAliasRequest(userNameForBasicAuth, jsonPayload string) int {
	return SendRequest("/v1/alias", userNameForBasicAuth, jsonPayload)
}

// SendGroupRequest sends request to /v1/group
func SendGroupRequest(userNameForBasicAuth, jsonPayload string) int {
	return SendRequest("/v1/group", userNameForBasicAuth, jsonPayload)
}

// SendRequest sends jsonPayload to gateway server with userNameForBasicAuth in basic auth
func SendRequest(endPoint, userNameForBasicAuth, jsonPayload string) int {
	req, _ := http.NewRequest("POST", serverIP+endPoint, bytes.NewBuffer([]byte(jsonPayload)))
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth(userNameForBasicAuth, "")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	return resp.StatusCode
}

// SendHealthRequest sends health request
func SendHealthRequest() []byte {
	resp, err := http.Get(serverIP + "/health")
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}
	return body
}

// SendVersionRequest sends version request
func SendVersionRequest() []byte {
	resp, err := http.Get(serverIP + "/version")
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}
	return body
}

// SameStringSlice checks if two slices have same strings in any order
func SameStringSlice(x, y []string) bool {
	// If x is nil, then y must also be nil.
	if (x == nil) != (y == nil) {
		return false
	}

	if len(x) != len(y) {
		return false
	}

	sort.Strings(x)
	sort.Strings(y)

	for i := range x {
		if x[i] != y[i] {
			return false
		}
	}

	return true
}

// GetTableNamesWithPrefix returns all table names with specified prefix
func GetTableNamesWithPrefix(dbHandle *sql.DB, prefix string) []string {
	//Read the table names from PG
	stmt, err := dbHandle.Prepare(`SELECT tablename
                                        FROM pg_catalog.pg_tables
                                        WHERE schemaname != 'pg_catalog' AND
                                        schemaname != 'information_schema'`)
	if err != nil {
		panic(err)
	}
	defer stmt.Close()

	rows, err := stmt.Query()
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	tableNames := []string{}
	for rows.Next() {
		var tbName string
		err = rows.Scan(&tbName)
		if err != nil {
			panic(err)
		}
		if strings.HasPrefix(tbName, prefix) {
			tableNames = append(tableNames, tbName)
		}
	}

	return tableNames
}

// GetJobsCount returns count of jobs across all tables with specified prefix
func GetJobsCount(dbHandle *sql.DB, prefix string) int {
	tableNames := GetTableNamesWithPrefix(dbHandle, strings.ToLower(prefix)+"_jobs_")
	count := 0
	for _, tableName := range tableNames {
		var jobsCount int
		dbHandle.QueryRow(fmt.Sprintf(`select count(*) as count from %s;`, tableName)).Scan(&jobsCount)
		count += jobsCount
	}
	return count
}

// GetJobStatusCount returns count of job status across all tables with specified prefix
func GetJobStatusCount(dbHandle *sql.DB, jobState string, prefix string) int {
	tableNames := GetTableNamesWithPrefix(dbHandle, strings.ToLower(prefix)+"_job_status_")
	count := 0
	for _, tableName := range tableNames {
		var jobsCount int
		dbHandle.QueryRow(fmt.Sprintf(`select count(*) as count from %s where job_state='%s';`, tableName, jobState)).Scan(&jobsCount)
		count += jobsCount
	}
	return count
}

// GetLatestJobs returns jobs (with a limit) across all tables with specified prefix
func GetLatestJobs(dbHandle *sql.DB, prefix string, limit int) []*jobsdb.JobT {
	tableNames := GetTableNamesWithPrefix(dbHandle, strings.ToLower(prefix)+"_jobs_")
	var jobList []*jobsdb.JobT
	for _, tableName := range tableNames {
		rows, err := dbHandle.Query(fmt.Sprintf(`select %[1]s.job_id, %[1]s.uuid, %[1]s.parameters, %[1]s.custom_val,
		%[1]s.event_payload, %[1]s.created_at, %[1]s.expire_at from %[1]s order by %[1]s.created_at desc, %[1]s.job_id desc limit %v;`, tableName, limit-len(jobList)))
		if err != nil {
			panic(err)
		}
		defer rows.Close()
		for rows.Next() {
			var job jobsdb.JobT
			rows.Scan(&job.JobID, &job.UUID, &job.Parameters, &job.CustomVal,
				&job.EventPayload, &job.CreatedAt, &job.ExpireAt)
			if len(jobList) < limit {
				jobList = append(jobList, &job)
			}
		}
		if len(jobList) >= limit {
			break
		}
	}
	return jobList
}

// GetJobStatus returns job statuses (with a limit) across all tables with specified prefix
func GetJobStatus(dbHandle *sql.DB, prefix string, limit int, jobState string) []*jobsdb.JobStatusT {
	tableNames := GetTableNamesWithPrefix(dbHandle, strings.ToLower(prefix)+"_job_status_")
	var jobStatusList []*jobsdb.JobStatusT
	for _, tableName := range tableNames {
		rows, err := dbHandle.Query(fmt.Sprintf(`select * from %[1]s where job_state='%s' order by %[1]s.created_at desc limit %v;`, tableName, jobState, limit-len(jobStatusList)))
		if err != nil {
			panic(err)
		}
		defer rows.Close()
		for rows.Next() {
			var jobStatus jobsdb.JobStatusT
			rows.Scan(&jobStatus)
			if len(jobStatusList) < limit {
				jobStatusList = append(jobStatusList, &jobStatus)
			}
		}
		if len(jobStatusList) >= limit {
			break
		}
	}
	return jobStatusList
}

// GetLoadFileTableName queries table column form the warehouseLoadFilesTable provided
func GetLoadFileTableName(dbHandle *sql.DB, warehouseLoadFilesTable string, sourceId string, destinationId string, destinationType string) []string {
	rows, err := dbHandle.Query(fmt.Sprintf(`SELECT table_name FROM %s where source_id='%s' and destination_id='%s' and destination_type='%s'`, warehouseLoadFilesTable, sourceId, destinationId, destinationType))
	if err != nil {
		panic(err)
	}
	defer rows.Close()
	var tableNames []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			panic(err)
		}
		tableNames = append(tableNames, tableName)
	}
	return tableNames
}

func GetWarehouseSchema(dbHandle *sql.DB, warehouseSchemaTable string, uploadId int64, sourceID string, destinationID string) map[string]map[string]string {
	var rawSchema json.RawMessage
	err := dbHandle.QueryRow(fmt.Sprintf(`SELECT schema FROM %s where wh_upload_id = %d and source_id='%s'and destination_id='%s'`, warehouseSchemaTable, uploadId, sourceID, destinationID)).Scan(&rawSchema)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil
		}
		if err != nil {
			panic(err)
		}
	}

	var schemaMapInterface map[string]interface{}
	err = json.Unmarshal(rawSchema, &schemaMapInterface)
	if err != nil {
		panic(err)
	}
	schema := make(map[string]map[string]string)
	for key, val := range schemaMapInterface {
		y := make(map[string]string)
		x := val.(map[string]interface{})
		for k, v := range x {
			y[k] = v.(string)
		}
		schema[key] = y
	}
	return schema
}

func DeleteRowsInTables(dbHandle *sql.DB, tables []string){
	for _,table := range tables{
		DeleteRowsInTable(dbHandle, table)
	}
}

func DeleteRowsInTable(dbHandle *sql.DB, table string) {
	_, err := dbHandle.Query(fmt.Sprintf(`delete from only %s`, table))
	if err != nil {
		panic(err)
	}
}
func GetDestinationIDsFromLoadFileTable(dbHandle *sql.DB, warehouseLoadFilesTable string, sourceID string) []string {
	rows, err := dbHandle.Query(fmt.Sprintf(`select destination_id from %s where source_id = '%s'`, warehouseLoadFilesTable, sourceID))
	if err != nil {
		panic(err)
	}
	var destinationIDs []string
	for rows.Next() {
		var destinationID string
		if err := rows.Scan(&destinationID); err != nil {
			panic(err)
		}
		destinationIDs = append(destinationIDs, destinationID)
	}
	return destinationIDs
}

func IsThisInThatSliceString(smallSlice []string, bigSlice []string) bool {
	if len(bigSlice) < len(smallSlice) {
		return false
	}
	for _, small := range smallSlice {
		for i, big := range bigSlice {
			if small == big {
				break
			}
			if small != big && i == len(bigSlice)-1 {
				return false
			}
		}
	}
	return true
}

func FetchUpdateState(dbHandle *sql.DB, warehouseUploadsTable string, sourceID string, destinationID string, destinationType string) ( int64, string, string) {
	row:=dbHandle.QueryRow(fmt.Sprintf(`select id, namespace, status from %s where source_id='%s' and destination_id='%s' and destination_type = '%s' order by updated_at desc limit 1`, warehouseUploadsTable, sourceID, destinationID, destinationType))
	var id int64
	var state string
	var namespace string
	err:=row.Scan(&id, &namespace, &state)
	if err != nil && err!= sql.ErrNoRows {
		panic(err)
	}
	return id, namespace, state
}

func VerifyUpdatedTables(dbHandle *sql.DB, warehouseTableUploadsTable string, uploadId int64, exportedDataState string ) []string {
	rows, err :=dbHandle.Query(fmt.Sprintf(`select table_name from %s where wh_upload_id=%d and status ='%s'`,warehouseTableUploadsTable, uploadId, exportedDataState))
	if err != nil {
		panic(err)
	}
	var tables []string
	for rows.Next() {
		var table string
		if err:=rows.Scan(&table); err != nil {
			panic(err)
		}
		tables = append(tables,table)
	}
	return tables
}



func QueryWarehouseWithAnonymusID(anonymousId string, eventName string, namespace string, destType string, destConfig interface{}) QueryTrackPayload {
	if destType == "BQ" {
		return queryBQ(anonymousId, eventName, namespace, destConfig)
	}
	if destType == "SNOWFLAKE" {
		 return querySnowflake(anonymousId, eventName, namespace, destConfig)
	}

	if destType == "RS" {
		return queryRS(anonymousId, eventName, namespace, destConfig)
	}
	return QueryTrackPayload{}
}
