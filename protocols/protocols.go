/*
 *
Table: event_models

| id  | uuid   | write_key | event_type  | event_model_identifier | created_at        |
| --- | ------ | --------- | -------- | ---------------- | ----------------- |
| 1   | uuid-1 | ksuid-1   | track    | logged_in        | 01, Jan 12: 00 PM |
| 2   | uuid-2 | ksuid-1   | track    | signed_up        | 01, Jan 12: 00 PM |
| 3   | uuid-3 | ksuid-1   | page     | Home Page        | 01, Jan 12: 00 PM |
| 4   | uuid-4 | ksuid-2   | identify |                  | 01, Jan 12: 00 PM |


Table: schema_versions

| id  | uuid   | event_model_id | schema_hash | schema                          | metadata | first_seen        | last_seen          |
| --- | ------ | -------- | ----------- | ------------------------------- | -------- | ----------------- | ------------------ |
| 1   | uuid-9 | uuid-1   | hash-1      | {"a": "string", "b": "float64"} | {}       | 01, Jan 12: 00 PM | 01, June 12: 00 PM |
| 2   | uuid-8 | uuid-2   | hash-2      | {"a": "string", "b": "string"}  | {}       | 01, Jan 12: 00 PM | 01, June 12: 00 PM |
| 3   | uuid-7 | uuid-3   | hash-3      | {"a": "string", "c": "float64"} | {}       | 01, Jan 12: 00 PM | 01, June 12: 00 PM |
| 4   | uuid-6 | uuid-2   | hash-1      | {"a": "string", "b": "float64"} | {}       | 01, Jan 12: 00 PM | 01, June 12: 00 PM |

*/

package protocols

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jeremywohl/flatten"
	"github.com/lib/pq"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	uuid "github.com/satori/go.uuid"
)

// EventModelT is a struct that represents EVENT_MODELS_TABLE
type EventModelT struct {
	ID              int
	UUID            string `json:"eventModelID"`
	WriteKey        string `json:"writeKey"`
	EventType       string `json:"eventType"`
	EventIdentifier string `json:"eventIdentifier"`
	CreatedAt       time.Time
}

// SchemaVersionT is a struct that represents SCHEMA_VERSIONS_TABLE
type SchemaVersionT struct {
	ID           int64
	UUID         string `json:"versionID"`
	SchemaHash   string `json:"-"`
	EventModelID string
	Schema       json.RawMessage
	Metadata     json.RawMessage
	FirstSeen    time.Time
	LastSeen     time.Time
}

//EventModelIDMapT :	event_model's uuid to EventModel Mapping
type EventModelIDMapT map[string]*EventModelT

//EventModelMapT : <writeKey, eventType, eventIdentifier> to EventModel Mapping
type EventModelMapT map[string]map[string]map[string]*EventModelT

//SchemaVersionMapT : <event_model_id, schema_hash> to SchemaVersion Mapping
type SchemaVersionMapT map[string]map[string]*SchemaVersionT

// ProtocolManagerT handles all protocols related features
type ProtocolManagerT struct {
	dbHandle          *sql.DB
	eventModelIDMap   EventModelIDMapT
	eventModelMap     EventModelMapT
	schemaVersionMap  SchemaVersionMapT
	eventModelLock    sync.RWMutex
	schemaVersionLock sync.RWMutex
}

var enableProtocols bool
var flushInterval time.Duration
var adminUser string
var adminPassword string

const EVENT_MODELS_TABLE = "event_models"
const SCHEMA_VERSIONS_TABLE = "schema_versions"

var eventSchemaChannel chan *GatewayEventBatchT

var newEventModels map[string]*EventModelT
var newSchemaVersions map[string]*SchemaVersionT
var dirtySchemaVersions map[string]*SchemaVersionT

//GatewayEventBatchT : Type sent from gateway
type GatewayEventBatchT struct {
	writeKey   string
	eventBatch string
}

//EventT : Generic type for singular event
type EventT map[string]interface{}

//EventPayloadT : Generic type for gateway event payload
type EventPayloadT struct {
	WriteKey   string
	ReceivedAt string
	Batch      []EventT
}

func loadConfig() {
	enableProtocols = config.GetBool("Protocols.enabled", true)
	flushInterval = config.GetDuration("Protocols.syncIntervalInS", 5) * time.Second
	adminUser = config.GetEnv("RUDDER_ADMIN_USER", "rudder")
	adminPassword = config.GetEnv("RUDDER_ADMIN_PASSWORD", "rudderstack")

	if adminPassword == "rudderstack" {
		fmt.Println("[Protocols] You are using default password. Please change it by setting env variable RUDDER_ADMIN_PASSWORD")
	}
}

func init() {
	loadConfig()
}

//RecordEventSchema : Records event schema for every event in the batch
func (manager *ProtocolManagerT) RecordEventSchema(writeKey string, eventBatch string) bool {
	if !enableProtocols {
		return false
	}

	eventSchemaChannel <- &GatewayEventBatchT{writeKey, eventBatch}
	return true
}

func (manager *ProtocolManagerT) updateEventModelCache(eventModel *EventModelT, isNew bool) {
	eventModelID := eventModel.UUID
	writeKey := eventModel.WriteKey
	eventType := eventModel.EventType
	eventIdentifier := eventModel.EventIdentifier

	_, ok := manager.eventModelMap[writeKey]
	if !ok {
		manager.eventModelMap[writeKey] = make(map[string]map[string]*EventModelT)
	}
	_, ok = manager.eventModelMap[writeKey][eventType]
	if !ok {
		manager.eventModelMap[writeKey][eventType] = make(map[string]*EventModelT)
	}
	manager.eventModelMap[writeKey][eventType][eventIdentifier] = eventModel
	manager.eventModelIDMap[eventModelID] = eventModel

	if isNew {
		newEventModels[eventModelID] = eventModel
	}
}

func (manager *ProtocolManagerT) updateSchemaVersionCache(schemaVersion *SchemaVersionT, isNew bool, isDirty bool) {
	eventModelID := schemaVersion.EventModelID
	schemaHash := schemaVersion.SchemaHash
	versionID := schemaVersion.UUID

	_, ok := manager.schemaVersionMap[eventModelID]
	if !ok {
		manager.schemaVersionMap[eventModelID] = make(map[string]*SchemaVersionT)
	}
	manager.schemaVersionMap[eventModelID][schemaHash] = schemaVersion

	if isNew {
		newSchemaVersions[versionID] = schemaVersion
	}
	if isDirty {
		_, ok := newSchemaVersions[versionID]
		if !ok {
			dirtySchemaVersions[versionID] = schemaVersion
		}
	}
}

/*
 *
| Event Type | event_type  | event_model_identfier |
| ---------- | -------- | --------------- |
| track      | track    | event["event"]  |
| page       | page     | event["name"]   |
| screen     | screen   | event["name"]   |
| identify   | identify | ""              |
| alias      | alias    | ""              |
| group      | group    | ""              |
*
* All event types and schema versions are generated by grouping according to the table above.
* Eg:
*    <track, login> will always be of same event_model. Different payloads will result in different schema_versions
*    <track, login> will always be of same event_model. Different payloads will result in different schema_versions
*    <page, home-page> will always be of same event_model. Different payloads will result in different schema_versions
*    <identify> There will be only identify event_model per source. Schema versions can change with different traits.
*
* This function is goroutine-safe. We can scale multiple go-routines calling this function,
* but since this method does mostly in-memory operations and has locks, there might not be much perfomance improvement.
*/
func (manager *ProtocolManagerT) handleEvent(writeKey string, event EventT) {
	eventType, ok := event["type"].(string)
	if !ok {
		logger.Debugf("[Protocols] Invalid or no eventType")
		return
	}
	eventIdentifier := ""
	if eventType == "track" {
		eventIdentifier, ok = event["event"].(string)
	} else if eventType == "page" {
		eventIdentifier, ok = event["name"].(string)
	} else if eventType == "screen" {
		eventIdentifier, ok = event["name"].(string)
	}
	if !ok {
		logger.Debugf("[Protocols] Invalid event idenitfier")
		return
	}

	//TODO: Create locks on every event_model to improve scaling this
	manager.eventModelLock.Lock()
	manager.schemaVersionLock.Lock()
	defer manager.eventModelLock.Unlock()
	defer manager.schemaVersionLock.Unlock()

	eventModel, ok := manager.eventModelMap[writeKey][eventType][eventIdentifier]
	if !ok {
		eventModelID := uuid.NewV4().String()
		eventModel = &EventModelT{
			UUID:            eventModelID,
			WriteKey:        writeKey,
			EventType:       eventType,
			EventIdentifier: eventIdentifier,
		}

		manager.updateEventModelCache(eventModel, true)
		newEventModels[eventModelID] = eventModel
	}

	schemaHash, schema, err := computeVersion(event)
	if err != nil {
		logger.Debug(err.Error())
		return
	}
	schemaVersion, ok := manager.schemaVersionMap[eventModel.UUID][schemaHash]

	if !ok {
		schemaJSON, err := json.Marshal(schema)
		assertError(err)
		versionID := uuid.NewV4().String()
		schemaVersion = &SchemaVersionT{
			UUID:         versionID,
			SchemaHash:   schemaHash,
			EventModelID: eventModel.UUID,
			Schema:       schemaJSON,
		}
		manager.updateSchemaVersionCache(schemaVersion, true, false)
	} else {
		manager.updateSchemaVersionCache(schemaVersion, false, true)
	}
}

func (manager *ProtocolManagerT) recordEvents() {
	for gatewayEventBatch := range eventSchemaChannel {
		var eventPayload EventPayloadT
		err := json.Unmarshal([]byte(gatewayEventBatch.eventBatch), &eventPayload)
		assertError(err)
		for _, event := range eventPayload.Batch {
			manager.handleEvent(eventPayload.WriteKey, event)
		}
	}
}

func (manager *ProtocolManagerT) flushEventSchemas() {
	// This will run forever. If you want to quit in between, change it to ticker and call stop()
	// Otherwise the ticker won't be GC'ed
	ticker := time.Tick(flushInterval)
	for range ticker {

		// If needed, copy the maps and release the lock immediately
		manager.eventModelLock.Lock()
		manager.schemaVersionLock.Lock()

		if len(newEventModels) == 0 && len(newSchemaVersions) == 0 && len(dirtySchemaVersions) == 0 {
			manager.eventModelLock.Unlock()
			manager.schemaVersionLock.Unlock()
			continue
		}

		txn, err := manager.dbHandle.Begin()
		assertError(err)

		if len(newEventModels) > 0 {
			stmt, err := txn.Prepare(pq.CopyIn(EVENT_MODELS_TABLE, "uuid", "write_key", "event_type", "event_model_identifier"))
			assertTxnError(err, txn)
			defer stmt.Close()
			for eventModelID, eventType := range newEventModels {
				_, err = stmt.Exec(eventModelID, eventType.WriteKey, eventType.EventType, eventType.EventIdentifier)
				assertTxnError(err, txn)
			}
			_, err = stmt.Exec()
			assertTxnError(err, txn)
			logger.Debugf("[Protocols][Flush] %d new event types", len(newEventModels))
		}

		if len(newSchemaVersions) > 0 {
			stmt, err := txn.Prepare(pq.CopyIn(SCHEMA_VERSIONS_TABLE, "uuid", "event_model_id", "schema_hash", "schema", "last_seen"))
			assertTxnError(err, txn)
			defer stmt.Close()
			for versionID, schemaVersion := range newSchemaVersions {
				_, err = stmt.Exec(versionID, schemaVersion.EventModelID, schemaVersion.SchemaHash, string(schemaVersion.Schema), "now()")
				assertTxnError(err, txn)
			}
			_, err = stmt.Exec()
			assertTxnError(err, txn)
			logger.Debugf("[Protocols][Flush] %d new schema versions", len(newSchemaVersions))
		}

		// To improve efficiency, making 1 query for all last_seen timestamps
		// Since the flush interval is short (i.e., 5 sec), this should not be a problem
		if len(dirtySchemaVersions) > 0 {
			versionIDs := make([]string, 0, len(dirtySchemaVersions))
			for versionID := range dirtySchemaVersions {
				versionIDs = append(versionIDs, versionID)
			}
			updateLastSeenSQL := fmt.Sprintf(`UPDATE %s SET last_seen = now() WHERE uuid IN ('%s')`, SCHEMA_VERSIONS_TABLE, strings.Join(versionIDs, "', '"))
			_, err := txn.Exec(updateLastSeenSQL)
			assertTxnError(err, txn)
			logger.Debugf("[Protocols][Flush] %d last_seen updates", len(dirtySchemaVersions))
		}

		err = txn.Commit()
		assertError(err)

		newEventModels = make(map[string]*EventModelT)
		newSchemaVersions = make(map[string]*SchemaVersionT)
		dirtySchemaVersions = make(map[string]*SchemaVersionT)

		manager.schemaVersionLock.Unlock()
		manager.eventModelLock.Unlock()
	}
}

// TODO: Move this into some DB manager
func createDBConnection() *sql.DB {
	psqlInfo := jobsdb.GetConnectionString()
	var err error
	dbHandle, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		panic(err)
	}

	err = dbHandle.Ping()
	if err != nil {
		panic(err)
	}
	return dbHandle
}

func assertError(err error) {
	if err != nil {
		panic(err)
	}
}

func assertTxnError(err error, txn *sql.Tx) {
	if err != nil {
		txn.Rollback()
		panic(err)
	}
}

func (manager *ProtocolManagerT) populateEventModels() {

	eventModelsSelectSQL := fmt.Sprintf(`SELECT * FROM %s`, EVENT_MODELS_TABLE)

	rows, err := manager.dbHandle.Query(eventModelsSelectSQL)
	assertError(err)
	defer rows.Close()

	for rows.Next() {
		var eventModel EventModelT
		err := rows.Scan(&eventModel.ID, &eventModel.UUID, &eventModel.WriteKey, &eventModel.EventType,
			&eventModel.EventIdentifier, &eventModel.CreatedAt)

		assertError(err)
		manager.updateEventModelCache(&eventModel, false)
	}
}

func (manager *ProtocolManagerT) populateSchemaVersions() {

	schemaVersionsSelectSQL := fmt.Sprintf(`SELECT * FROM %s`, SCHEMA_VERSIONS_TABLE)

	rows, err := manager.dbHandle.Query(schemaVersionsSelectSQL)
	assertError(err)
	defer rows.Close()

	for rows.Next() {
		var schemaVersion SchemaVersionT
		err := rows.Scan(&schemaVersion.ID, &schemaVersion.UUID, &schemaVersion.EventModelID, &schemaVersion.SchemaHash,
			&schemaVersion.Schema, &schemaVersion.Metadata, &schemaVersion.FirstSeen, &schemaVersion.LastSeen)
		assertError(err)

		manager.updateSchemaVersionCache(&schemaVersion, false, false)
	}
}

// This should be called during the Setup() to populate existing event Schemas
func (manager *ProtocolManagerT) populateEventSchemas() {
	manager.populateEventModels()
	manager.populateSchemaVersions()
}

//TODO: Support for prefix based
func computeVersion(event EventT) (schemaHash string, schema map[string]string, err error) {

	eventMap := map[string]interface{}(event)

	flattenedEvent, err := flatten.Flatten((eventMap), "", flatten.DotStyle)

	if err != nil {
		return "", nil, fmt.Errorf("[Protocols] Failed to flatten the event +%w", eventMap)
	} else {
		finalSchema := make(map[string]string)
		keys := make([]string, 0, len(flattenedEvent))
		for k, v := range flattenedEvent {
			keys = append(keys, k)
			reflectType := reflect.TypeOf(v)
			finalSchema[k] = reflectType.String()
		}
		sort.Strings(keys)

		var sb strings.Builder
		for _, k := range keys {
			sb.WriteString(k)
			sb.WriteString(":")
			sb.WriteString(finalSchema[k])
			sb.WriteString(",")
		}
		return misc.GetMD5Hash(sb.String()), finalSchema, err
	}
}

//TODO: Use Migrations library
func (manager *ProtocolManagerT) setupTables() {
	createEventModelsSQL := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		id SERIAL PRIMARY KEY,
		uuid VARCHAR(36) NOT NULL,
		write_key VARCHAR(32) NOT NULL,
		event_type TEXT NOT NULL,
		event_model_identifier TEXT NOT NULL DEFAULT '',
		created_at TIMESTAMP NOT NULL DEFAULT NOW()
	)
	`, EVENT_MODELS_TABLE)

	_, err := manager.dbHandle.Exec(createEventModelsSQL)
	assertError(err)

	createWriteKeyIndexSQL := fmt.Sprintf(`CREATE INDEX IF NOT EXISTS write_key_index ON %s (write_key)`, EVENT_MODELS_TABLE)
	_, err = manager.dbHandle.Exec(createWriteKeyIndexSQL)
	assertError(err)

	createSchemaVersionsSQL := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		id BIGSERIAL PRIMARY KEY,
		uuid VARCHAR(36) NOT NULL, 
		event_model_id VARCHAR(36) NOT NULL,
		schema_hash VARCHAR(32) NOT NULL,
		schema JSONB NOT NULL,
		metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
		first_seen TIMESTAMP NOT NULL DEFAULT NOW(),
		last_seen TIMESTAMP NOT NULL DEFAULT NOW()
	)
	`, SCHEMA_VERSIONS_TABLE)

	_, err = manager.dbHandle.Exec(createSchemaVersionsSQL)
	assertError(err)

	createEventModelIDIndexSQL := fmt.Sprintf(`CREATE INDEX IF NOT EXISTS event_model_id_index ON %s (event_model_id)`, SCHEMA_VERSIONS_TABLE)
	_, err = manager.dbHandle.Exec(createEventModelIDIndexSQL)
	assertError(err)

	createUniqueIndexSQL := fmt.Sprintf(`CREATE UNIQUE INDEX IF NOT EXISTS event_model_id_schema_hash_index ON %s (event_model_id, schema_hash)`, SCHEMA_VERSIONS_TABLE)
	_, err = manager.dbHandle.Exec(createUniqueIndexSQL)
	assertError(err)
}

func (manager *ProtocolManagerT) Setup() {

	if !enableProtocols {
		logger.Info("[Protocols] Feature is disabled.")
		return
	}

	logger.Info("[Protocols] Setting up protocols...")
	// Clean this up
	manager.dbHandle = createDBConnection()

	// Following data structures store events and schemas since last flush
	newEventModels = make(map[string]*EventModelT)
	newSchemaVersions = make(map[string]*SchemaVersionT)
	dirtySchemaVersions = make(map[string]*SchemaVersionT)

	manager.eventModelIDMap = make(EventModelIDMapT)
	manager.eventModelMap = make(EventModelMapT)
	manager.schemaVersionMap = make(SchemaVersionMapT)

	manager.setupTables()
	manager.populateEventSchemas()
	eventSchemaChannel = make(chan *GatewayEventBatchT, 1000)

	rruntime.Go(func() {
		manager.recordEvents()
	})

	rruntime.Go(func() {
		manager.flushEventSchemas()
	})

	logger.Info("[Protocols] Set up protocols successful.")
}
