/*
 *


//TODO: Find right name for e_type

Table: event_types
------------------------------------------------------------------
 uuid   | write_key | e_type     | event_identifier | created_at
------------------------------------------------------------------
 uuid-1 | ksuid-1   | track      | logged_in        | 01, Jan 12: 00 PM
 uuid-2 | ksuid-1   | track      | signed_up        | 01, Jan 12: 00 PM
 uuid-3 | ksuid-1   | page       | Home Page        | 01, Jan 12: 00 PM
 uuid-4 | ksuid-2   | identify   |                  | 01, Jan 12: 00 PM
------------------------------------------------------------------


Table: schema_versions
------------------------------------------------------------------------------------------------------
uuid   | schema_hash| event_id | schema                    | metadata | first_seen         | last_seen
------------------------------------------------------------------------------------------------------
uuid-9 | hash-1    | uuid-1   | {                         | {}       | 01, Jan 12: 00 PM  | 01, June 12: 00 PM
													"anonymousId": "string",
													"email": "string"
												}

uuid-8 | hash-2    | uuid-2   | {                         | {}       | 01, Jan 12: 00 PM  | 01, June 12: 00 PM
													"anonymousId": "string",
													"email": "string",
													"location": "string"
												}

uuid-7 | hash-3    | uuid-3   | {                         | {}       | 01, Jan 12: 00 PM  | 01, June 12: 00 PM
													"anonymousId": "string",
													"email": "string",
													"utm_source": "string"
													"location": "string"
												}
uuid-6 | hash-4    | uuid-4   | {                         | {}       | 01, Jan 12: 00 PM  | 01, June 12: 00 PM
													"path": "string",
													"referrer": "string",
													"location": "string"
												}
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
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	uuid "github.com/satori/go.uuid"
)

// ProtocolManagerT handles all protocols related features
type ProtocolManagerT struct {
	dbHandle *sql.DB

	// id to Event Mapping
	eventTypeIDMap map[string]*EventTypeT

	// <eType,eventIdentifier> to Event Mapping
	//TODO: Remove if not needed
	eventTypeMap map[string]map[string]*EventTypeT

	// <event_id, schema_hash> to SchemaVersion Mapping
	schemaVersionMap map[string]map[string]*SchemaVersionT

	eventTypeLock     sync.RWMutex
	schemaVersionLock sync.RWMutex
}

// EventTypeT is a struct that represents EVENT_TYPES_TABLE
type EventTypeT struct {
	ID              int
	UUID            string
	WriteKey        string
	eType           string
	EventIdentifier string
	CreatedAt       time.Time
}

// SchemaVersionT is a struct that represents SCHEMA_VERSIONS_TABLE
type SchemaVersionT struct {
	ID         int64
	UUID       string
	SchemaHash string
	EventID    string
	Schema     json.RawMessage
	Metadata   json.RawMessage
	FirstSeen  time.Time
	LastSeen   time.Time
	EventType  *EventTypeT `json:"-"`
}

//TODO: Add a config variable for this
const disableProtocols = false

const EVENT_TYPES_TABLE = "event_types"
const SCHEMA_VERSIONS_TABLE = "schema_versions"

var eventSchemaChannel chan *GatewayEventBatchT

var newEventTypes map[string]*EventTypeT
var newSchemaVersions map[string]*SchemaVersionT
var dirtySchemaVersions map[string]*SchemaVersionT

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

//RecordEventSchema : Records event schema for every event in the batch
func (manager *ProtocolManagerT) RecordEventSchema(writeKey string, eventBatch string) bool {
	//if disableProtocols is true, return;
	if disableProtocols {
		return false
	}

	eventSchemaChannel <- &GatewayEventBatchT{writeKey, eventBatch}
	return true
}

// TODO: Write doc here, how is this built
// TODO: Add goroutines for parallelization
func (manager *ProtocolManagerT) handleEvent(writeKey string, event EventT) {
	eType := event["type"].(string)
	eventIdentifier := ""
	if eType == "track" {
		eventIdentifier = event["event"].(string)
	} else if eType == "page" {
		eventIdentifier = event["name"].(string)
	} else if eType == "screen" {
		eventIdentifier = event["name"].(string)
	}

	//TODO: Review the concurrency by scaling goroutines
	manager.eventTypeLock.RLock()
	eventType, ok := manager.eventTypeMap[eType][eventIdentifier]
	manager.eventTypeLock.RUnlock()
	if !ok {
		eventID := uuid.NewV4().String()
		eventType = &EventTypeT{
			UUID:            eventID,
			WriteKey:        writeKey,
			eType:           eType,
			EventIdentifier: eventIdentifier,
		}

		manager.eventTypeLock.Lock()
		newEventTypes[eventID] = eventType
		_, ok := manager.eventTypeMap[eType][eventIdentifier]
		if !ok {
			manager.eventTypeMap[eType] = make(map[string]*EventTypeT)
		}
		manager.eventTypeMap[eType][eventIdentifier] = eventType
		manager.eventTypeIDMap[eventID] = eventType
		manager.eventTypeLock.Unlock()
	}

	schemaHash, schema := computeVersion(event)
	manager.schemaVersionLock.Lock()
	_, ok = manager.schemaVersionMap[eventType.UUID]
	if !ok {
		manager.schemaVersionMap[eventType.UUID] = make(map[string]*SchemaVersionT)
	}
	schemaVersion, ok := manager.schemaVersionMap[eventType.UUID][schemaHash]
	manager.schemaVersionLock.Unlock()

	if !ok {
		schemaJSON, err := json.Marshal(schema)
		assertError(err)
		versionID := uuid.NewV4().String()
		schemaVersion = &SchemaVersionT{
			UUID:       versionID,
			SchemaHash: schemaHash,
			EventID:    eventType.UUID,
			Schema:     schemaJSON,
			EventType:  eventType,
		}
		manager.schemaVersionLock.Lock()
		newSchemaVersions[versionID] = schemaVersion
		manager.schemaVersionMap[eventType.UUID][schemaHash] = schemaVersion
		manager.schemaVersionLock.Unlock()
	} else {
		manager.schemaVersionLock.Lock()
		_, ok := newSchemaVersions[schemaVersion.UUID]
		// If not present in newSchemaVersions, add it to dirty
		if !ok {
			dirtySchemaVersions[schemaVersion.UUID] = schemaVersion
		}
		manager.schemaVersionLock.Unlock()
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
	ticker := time.Tick(5 * time.Second)
	for range ticker {

		// If needed, copy the maps and release the lock immediately
		manager.eventTypeLock.Lock()
		manager.schemaVersionLock.Lock()

		if len(newEventTypes) == 0 && len(newSchemaVersions) == 0 && len(dirtySchemaVersions) == 0 {
			manager.eventTypeLock.Unlock()
			manager.schemaVersionLock.Unlock()
			continue
		}

		//TODO: Handle Rollback - Refer jobsdb
		txn, err := manager.dbHandle.Begin()
		assertError(err)

		if len(newEventTypes) > 0 {
			stmt, err := txn.Prepare(pq.CopyIn(EVENT_TYPES_TABLE, "uuid", "write_key", "e_type", "event_identifier"))
			assertError(err)
			defer stmt.Close()
			for eventID, eventType := range newEventTypes {
				_, err = stmt.Exec(eventID, eventType.WriteKey, eventType.eType, eventType.EventIdentifier)
				assertError(err)
			}
			_, err = stmt.Exec()
			assertError(err)
			logger.Debugf("[Protocols][Flush] %d new event types", len(newEventTypes))
		}

		if len(newSchemaVersions) > 0 {
			stmt, err := txn.Prepare(pq.CopyIn(SCHEMA_VERSIONS_TABLE, "uuid", "event_id", "schema_hash", "schema", "last_seen"))
			assertError(err)
			defer stmt.Close()
			for versionID, schemaVersion := range newSchemaVersions {
				_, err = stmt.Exec(versionID, schemaVersion.EventID, schemaVersion.SchemaHash, string(schemaVersion.Schema), "now()")
				assertError(err)
			}
			_, err = stmt.Exec()
			assertError(err)
			logger.Debugf("[Protocols][Flush] %d new schema versions", len(newSchemaVersions))
		}

		// To improve efficiency, making 1 query for all last_seen timestamps
		// Since the flush interval is short (i.e., 5 sec), this should not be a problem
		if len(dirtySchemaVersions) > 0 {
			versionIDs := make([]string, 0, len(dirtySchemaVersions))
			for versionID, _ := range dirtySchemaVersions {
				versionIDs = append(versionIDs, versionID)
			}
			updateLastSeenSQL := fmt.Sprintf(`UPDATE %s SET last_seen = now() WHERE uuid IN ('%s')`, SCHEMA_VERSIONS_TABLE, strings.Join(versionIDs, "', '"))
			_, err := txn.Exec(updateLastSeenSQL)
			assertError(err)
			logger.Debugf("[Protocols][Flush] %d last_seen updates", len(dirtySchemaVersions))
		}

		err = txn.Commit()
		assertError(err)

		newEventTypes = make(map[string]*EventTypeT)
		newSchemaVersions = make(map[string]*SchemaVersionT)
		dirtySchemaVersions = make(map[string]*SchemaVersionT)

		manager.schemaVersionLock.Unlock()
		manager.eventTypeLock.Unlock()
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

// This should be called during the Setup() to populate existing event Schemas
func (manager *ProtocolManagerT) populateEventSchemas() {
	eventTypesSelectSQL := fmt.Sprintf(`SELECT * FROM %s`, EVENT_TYPES_TABLE)

	rows, err := manager.dbHandle.Query(eventTypesSelectSQL)
	assertError(err)
	defer rows.Close()

	for rows.Next() {
		var eventType EventTypeT
		err := rows.Scan(&eventType.ID, &eventType.UUID, &eventType.WriteKey, &eventType.eType,
			&eventType.EventIdentifier, &eventType.CreatedAt)

		assertError(err)
		manager.eventTypeIDMap[eventType.UUID] = &eventType
		_, ok := manager.eventTypeMap[eventType.eType]
		if !ok {
			manager.eventTypeMap[eventType.eType] = make(map[string]*EventTypeT)
		}
		manager.eventTypeMap[eventType.eType][eventType.EventIdentifier] = &eventType

	}

	schemaVersionsSelectSQL := fmt.Sprintf(`SELECT * FROM %s`, SCHEMA_VERSIONS_TABLE)

	rows, err = manager.dbHandle.Query(schemaVersionsSelectSQL)
	assertError(err)
	defer rows.Close()

	manager.schemaVersionMap = make(map[string]map[string]*SchemaVersionT)

	for rows.Next() {
		var schemaVersion SchemaVersionT
		err := rows.Scan(&schemaVersion.ID, &schemaVersion.UUID, &schemaVersion.EventID, &schemaVersion.SchemaHash,
			&schemaVersion.Schema, &schemaVersion.Metadata, &schemaVersion.FirstSeen, &schemaVersion.LastSeen)
		assertError(err)

		schemaVersion.EventType = manager.eventTypeIDMap[schemaVersion.EventID]
		_, ok := manager.schemaVersionMap[schemaVersion.EventID]
		if !ok {
			manager.schemaVersionMap[schemaVersion.EventID] = make(map[string]*SchemaVersionT)
		}
		manager.schemaVersionMap[schemaVersion.EventID][schemaVersion.SchemaHash] = &schemaVersion
	}

}

//TODO: Support for prefix based
func computeVersion(event EventT) (schemaHash string, schema map[string]string) {

	eventMap := map[string]interface{}(event)

	flattenedEvent, err := flatten.Flatten((eventMap), "", flatten.DotStyle)

	if err != nil {
		fmt.Println(err)
		panic("Failed to flatten the event")
	} else {
		fmt.Println(flattenedEvent)
		finalSchema := make(map[string]string)
		keys := make([]string, 0, len(finalSchema))
		for k, v := range flattenedEvent {
			keys = append(keys, k)
			reflectType := reflect.TypeOf(v)
			finalSchema[k] = reflectType.String()
		}
		fmt.Println(finalSchema)
		sort.Strings(keys)

		var sb strings.Builder
		for _, k := range keys {
			sb.WriteString(k)
			sb.WriteString(":")
			sb.WriteString(finalSchema[k])
			sb.WriteString(",")
		}
		return misc.GetMD5Hash(sb.String()), finalSchema
	}
}

//TODO: Use Migrations library
//TODO: Create indices
func (manager *ProtocolManagerT) setupTables() {
	createEventTypesSQL := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		id SERIAL PRIMARY KEY,
		uuid VARCHAR(36) NOT NULL,
		write_key VARCHAR(32) NOT NULL,
		e_type TEXT NOT NULL,
		event_identifier TEXT NOT NULL DEFAULT '',
		created_at TIMESTAMP NOT NULL DEFAULT NOW()
	)
	`, EVENT_TYPES_TABLE)

	_, err := manager.dbHandle.Exec(createEventTypesSQL)
	assertError(err)

	createSchemaVersionsSQL := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		id BIGSERIAL PRIMARY KEY,
		uuid VARCHAR(36) NOT NULL, 
		event_id VARCHAR(36) NOT NULL,
		schema_hash VARCHAR(32) NOT NULL,
		schema JSONB NOT NULL,
		metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
		first_seen TIMESTAMP NOT NULL DEFAULT NOW(),
		last_seen TIMESTAMP NOT NULL DEFAULT NOW()
	)
	`, SCHEMA_VERSIONS_TABLE)

	_, err = manager.dbHandle.Exec(createSchemaVersionsSQL)
	assertError(err)
}

func (manager *ProtocolManagerT) Setup() {

	if disableProtocols {
		logger.Info("[Protocols] Feature is disabled.")
		return
	}

	logger.Info("[Protocols] Setting up protocols...")
	// Clean this up
	manager.dbHandle = createDBConnection()

	// Following data structures store all time events and schemas
	manager.eventTypeIDMap = make(map[string]*EventTypeT)
	manager.eventTypeMap = make(map[string]map[string]*EventTypeT)
	manager.schemaVersionMap = make(map[string]map[string]*SchemaVersionT)

	// Following data structures store events and schemas since last flush
	newEventTypes = make(map[string]*EventTypeT)
	newSchemaVersions = make(map[string]*SchemaVersionT)
	dirtySchemaVersions = make(map[string]*SchemaVersionT)

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
