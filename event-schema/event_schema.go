//Event schemas uses countish algorithm by https://github.com/shanemhansen/countish

/*
 *
Table: event_models

| id  | uuid   | write_key | event_type | event_model_identifier | created_at        |
| --- | ------ | --------- | ---------- | ---------------------- | ----------------- |
| 1   | uuid-1 | ksuid-1   | track      | logged_in              | 01, Jan 12: 00 PM |
| 2   | uuid-2 | ksuid-1   | track      | signed_up              | 01, Jan 12: 00 PM |
| 3   | uuid-3 | ksuid-1   | page       | Home Page              | 01, Jan 12: 00 PM |
| 4   | uuid-4 | ksuid-2   | identify   |                        | 01, Jan 12: 00 PM |


Table: schema_versions

| id  | uuid   | event_model_id | schema_hash | schema                          | metadata | first_seen        | last_seen          |
| --- | ------ | -------------- | ----------- | ------------------------------- | -------- | ----------------- | ------------------ |
| 1   | uuid-9 | uuid-1         | hash-1      | {"a": "string", "b": "float64"} | {}       | 01, Jan 12: 00 PM | 01, June 12: 00 PM |
| 2   | uuid-8 | uuid-2         | hash-2      | {"a": "string", "b": "string"}  | {}       | 01, Jan 12: 00 PM | 01, June 12: 00 PM |
| 3   | uuid-7 | uuid-3         | hash-3      | {"a": "string", "c": "float64"} | {}       | 01, Jan 12: 00 PM | 01, June 12: 00 PM |
| 4   | uuid-6 | uuid-2         | hash-1      | {"a": "string", "b": "float64"} | {}       | 01, Jan 12: 00 PM | 01, June 12: 00 PM |

*/

package event_schema

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	uuid "github.com/gofrs/uuid"
	"github.com/jeremywohl/flatten"
	"github.com/lib/pq"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
)

// EventModelT is a struct that represents EVENT_MODELS_TABLE
type EventModelT struct {
	ID              int
	UUID            string `json:"EventID"`
	WriteKey        string `json:"WriteKey"`
	EventType       string `json:"EventType"`
	EventIdentifier string `json:"EventIdentifier"`
	CreatedAt       time.Time
	Schema          json.RawMessage
	Metadata        json.RawMessage `json:"-"`
	PrivateData     json.RawMessage `json:"-"`
	LastSeen        time.Time
	reservoirSample *ReservoirSample
	TotalCount      int64
	Archived        bool
}

// SchemaVersionT is a struct that represents SCHEMA_VERSIONS_TABLE
type SchemaVersionT struct {
	ID              int64
	UUID            string `json:"VersionID"`
	SchemaHash      string `json:"-"`
	EventModelID    string
	Schema          json.RawMessage
	Metadata        json.RawMessage `json:"-"`
	PrivateData     json.RawMessage `json:"-"`
	FirstSeen       time.Time
	LastSeen        time.Time
	reservoirSample *ReservoirSample
	TotalCount      int64
	Archived        bool
}

type MetaDataT struct {
	SampledEvents []interface{}
	TotalCount    int64
	Counters      map[string][]*CounterItem `json:"FrequentValues"`
}

type PrivateDataT struct {
	FrequencyCounters []*FrequencyCounter
}

type WriteKey string
type EventType string
type EventIdentifier string

//EventModelMapT : <writeKey, eventType, eventIdentifier> to EventModel Mapping
type EventModelMapT map[WriteKey]map[EventType]map[EventIdentifier]*EventModelT

//SchemaVersionMapT : <event_model_id, schema_hash> to SchemaVersion Mapping
type SchemaVersionMapT map[string]map[string]*SchemaVersionT

// EventSchemaManagerT handles all event-schemas related features
type EventSchemaManagerT struct {
	dbHandle             *sql.DB
	eventModelMap        EventModelMapT
	schemaVersionMap     SchemaVersionMapT
	eventModelLock       sync.RWMutex
	schemaVersionLock    sync.RWMutex
	disableInMemoryCache bool
}

type OffloadedModelT struct {
	UUID            string
	LastSeen        time.Time
	WriteKey        string
	EventType       string
	EventIdentifier string
}

type OffloadedSchemaVersionT struct {
	UUID         string
	EventModelID string
	LastSeen     time.Time
	SchemaHash   string
}

var (
	flushInterval                   time.Duration
	adminUser                       string
	adminPassword                   string
	reservoirSampleSize             int
	eventSchemaChannel              chan *GatewayEventBatchT
	updatedEventModels              map[string]*EventModelT
	updatedSchemaVersions           map[string]*SchemaVersionT
	offloadedEventModels            map[string]map[string]*OffloadedModelT
	offloadedSchemaVersions         map[string]map[string]*OffloadedSchemaVersionT
	archivedEventModels             map[string]map[string]*OffloadedModelT
	archivedSchemaVersions          map[string]map[string]*OffloadedSchemaVersionT
	toDeleteEventModelIDs           []string
	toDeleteSchemaVersionIDs        []string
	pkgLogger                       logger.LoggerI
	noOfWorkers                     int
	shouldCaptureNilAsUnknowns      bool
	eventModelLimit                 int
	schemaVersionPerEventModelLimit int
	offloadLoopInterval             time.Duration
	offloadThreshold                time.Duration
	areEventSchemasPopulated        bool
)

const EVENT_MODELS_TABLE = "event_models"
const SCHEMA_VERSIONS_TABLE = "schema_versions"

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
	adminUser = config.GetEnv("RUDDER_ADMIN_USER", "rudder")
	adminPassword = config.GetEnv("RUDDER_ADMIN_PASSWORD", "rudderstack")
	noOfWorkers = config.GetInt("EventSchemas.noOfWorkers", 128)
	config.RegisterDurationConfigVariable(time.Duration(240), &flushInterval, true, time.Second, []string{"EventSchemas.syncInterval", "EventSchemas.syncIntervalInS"}...)

	config.RegisterIntConfigVariable(5, &reservoirSampleSize, true, 1, "EventSchemas.sampleEventsSize")
	config.RegisterIntConfigVariable(200, &eventModelLimit, true, 1, "EventSchemas.eventModelLimit")
	config.RegisterIntConfigVariable(20, &schemaVersionPerEventModelLimit, true, 1, "EventSchemas.schemaVersionPerEventModelLimit")
	config.RegisterBoolConfigVariable(false, &shouldCaptureNilAsUnknowns, true, "EventSchemas.captureUnknowns")
	config.RegisterDurationConfigVariable(time.Duration(60), &offloadLoopInterval, true, time.Second, []string{"EventSchemas.offloadLoopInterval"}...)
	config.RegisterDurationConfigVariable(time.Duration(1800), &offloadThreshold, true, time.Second, []string{"EventSchemas.offloadThreshold"}...)

	if adminPassword == "rudderstack" {
		fmt.Println("[EventSchemas] You are using default password. Please change it by setting env variable RUDDER_ADMIN_PASSWORD")
	}
}

func Init2() {
	loadConfig()
	pkgLogger = logger.NewLogger().Child("event-schema")
}

//RecordEventSchema : Records event schema for every event in the batch
func (manager *EventSchemaManagerT) RecordEventSchema(writeKey string, eventBatch string) bool {
	select {
	case eventSchemaChannel <- &GatewayEventBatchT{writeKey, eventBatch}:
	default:
		stats.NewTaggedStat("dropped_events_count", stats.CountType, stats.Tags{"module": "event_schemas", "writeKey": writeKey}).Increment()
	}
	return true
}

func (manager *EventSchemaManagerT) updateEventModelCache(eventModel *EventModelT, toCreateOrUpdate bool) {
	eventModelID := eventModel.UUID
	writeKey := eventModel.WriteKey
	eventType := eventModel.EventType
	eventIdentifier := eventModel.EventIdentifier

	_, ok := manager.eventModelMap[WriteKey(writeKey)]
	if !ok {
		manager.eventModelMap[WriteKey(writeKey)] = make(map[EventType]map[EventIdentifier]*EventModelT)
	}
	_, ok = manager.eventModelMap[WriteKey(writeKey)][EventType(eventType)]
	if !ok {
		manager.eventModelMap[WriteKey(writeKey)][EventType(eventType)] = make(map[EventIdentifier]*EventModelT)
	}
	manager.eventModelMap[WriteKey(writeKey)][EventType(eventType)][EventIdentifier(eventIdentifier)] = eventModel

	if toCreateOrUpdate {
		updatedEventModels[eventModelID] = eventModel
	}
}

func (manager *EventSchemaManagerT) deleteFromEventModelCache(eventModel *EventModelT) {
	writeKey := eventModel.WriteKey
	eventType := eventModel.EventType
	eventIdentifier := eventModel.EventIdentifier

	delete(updatedEventModels, eventModel.UUID)
	delete(offloadedEventModels[eventModel.WriteKey], eventTypeIdentifier(eventType, eventIdentifier))
	delete(manager.eventModelMap[WriteKey(writeKey)][EventType(eventType)], EventIdentifier(eventIdentifier))
	delete(countersCache, eventModel.UUID)
}

func (manager *EventSchemaManagerT) deleteFromSchemaVersionCache(schemaVersion *SchemaVersionT) {
	eventModelID := schemaVersion.EventModelID
	schemaHash := schemaVersion.SchemaHash

	delete(updatedSchemaVersions, schemaVersion.UUID)
	delete(offloadedSchemaVersions[eventModelID], schemaHash)
	delete(manager.schemaVersionMap[eventModelID], schemaHash)
	delete(countersCache, schemaHash)
}

func (manager *EventSchemaManagerT) deleteModelFromSchemaVersionCache(eventModel *EventModelT) {
	delete(manager.schemaVersionMap, eventModel.UUID)
}

func (manager *EventSchemaManagerT) updateSchemaVersionCache(schemaVersion *SchemaVersionT, toCreateOrUpdate bool) {
	eventModelID := schemaVersion.EventModelID
	schemaHash := schemaVersion.SchemaHash

	_, ok := manager.schemaVersionMap[eventModelID]
	if !ok {
		manager.schemaVersionMap[eventModelID] = make(map[string]*SchemaVersionT)
	}
	manager.schemaVersionMap[eventModelID][schemaHash] = schemaVersion

	if toCreateOrUpdate {
		updatedSchemaVersions[schemaVersion.UUID] = schemaVersion
	}
}

/*
 *
| Event Type | event_type | event_model_identfier |
| ---------- | ---------- | --------------------- |
| track      | track      | event["event"]        |
| page       | page       | event["name"]         |
| screen     | screen     | event["name"]         |
| identify   | identify   | ""                    |
| alias      | alias      | ""                    |
| group      | group      | ""                    |
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
func (manager *EventSchemaManagerT) handleEvent(writeKey string, event EventT) {
	eventType, ok := event["type"].(string)
	if !ok {
		pkgLogger.Debugf("[EventSchemas] Invalid or no eventType")
		return
	}
	eventIdentifier := ""
	if eventType == "track" {
		eventIdentifier, ok = event["event"].(string)
	}
	if !ok {
		pkgLogger.Debugf("[EventSchemas] Invalid event idenitfier")
		return
	}

	processingTimer := stats.NewTaggedStat("archive_event_model", stats.TimerType, stats.Tags{"module": "event_schemas", "writeKey": writeKey, "eventIdentifier": eventIdentifier})
	processingTimer.Start()
	defer processingTimer.End()

	//TODO: Create locks on every event_model to improve scaling this
	manager.eventModelLock.Lock()
	manager.schemaVersionLock.Lock()
	defer manager.eventModelLock.Unlock()
	defer manager.schemaVersionLock.Unlock()
	totalEventModels := 0
	for _, v := range manager.eventModelMap[WriteKey(writeKey)] {
		totalEventModels += len(v)
	}
	totalEventModels += len(offloadedEventModels[writeKey])
	eventModel, ok := manager.eventModelMap[WriteKey(writeKey)][EventType(eventType)][EventIdentifier(eventIdentifier)]
	if !ok {
		// check in offloaded models
		var wasOffloaded bool
		var offloadedModel *OffloadedModelT
		if byEventTypeIdentifier, ok := offloadedEventModels[writeKey]; ok {
			offloadedModel, wasOffloaded = byEventTypeIdentifier[eventTypeIdentifier(eventType, eventIdentifier)]
		}

		archiveOldestLastSeenModel := func() {
			oldestModel := manager.oldestSeenModel(writeKey)
			toDeleteEventModelIDs = append(toDeleteEventModelIDs, oldestModel.UUID)
			manager.deleteFromEventModelCache(oldestModel)
			if _, ok := archivedEventModels[oldestModel.WriteKey]; !ok {
				archivedEventModels[oldestModel.WriteKey] = make(map[string]*OffloadedModelT)
			}
			archivedEventModels[oldestModel.WriteKey][eventTypeIdentifier(oldestModel.EventType, oldestModel.EventIdentifier)] = &OffloadedModelT{UUID: oldestModel.UUID, LastSeen: oldestModel.LastSeen, WriteKey: oldestModel.WriteKey, EventType: oldestModel.EventType, EventIdentifier: oldestModel.EventIdentifier}
			stats.NewTaggedStat("archive_event_model", stats.CountType, stats.Tags{"module": "event_schemas", "writeKey": oldestModel.WriteKey, "eventIdentifier": oldestModel.EventIdentifier}).Increment()
		}

		// check in archived models
		var wasArchived bool
		var archivedModel *OffloadedModelT
		if byEventTypeIdentifier, ok := archivedEventModels[writeKey]; ok {
			archivedModel, wasArchived = byEventTypeIdentifier[eventTypeIdentifier(eventType, eventIdentifier)]
		}

		if wasOffloaded {
			manager.reloadModel(offloadedModel)
			eventModel, ok = manager.eventModelMap[WriteKey(writeKey)][EventType(eventType)][EventIdentifier(eventIdentifier)]
			if !ok {
				pkgLogger.Errorf(`[EventSchemas] Failed to reload event +%v, writeKey: %s, eventType: %s, eventIdentifier: %s`, offloadedModel.UUID, writeKey, eventType, eventIdentifier)
				return
			}
			stats.NewTaggedStat("reload_offloaded_event_model", stats.CountType, stats.Tags{"module": "event_schemas", "writeKey": eventModel.WriteKey, "eventIdentifier": eventModel.EventIdentifier}).Increment()
		} else if wasArchived {
			if totalEventModels >= eventModelLimit {
				archiveOldestLastSeenModel()
			}
			err := manager.reloadModel(archivedModel)
			if err != nil {
				eventModel = manager.createModel(writeKey, eventType, eventIdentifier, eventModel, totalEventModels, archiveOldestLastSeenModel)
			} else {
				eventModel, ok = manager.eventModelMap[WriteKey(writeKey)][EventType(eventType)][EventIdentifier(eventIdentifier)]
				if !ok {
					pkgLogger.Errorf(`[EventSchemas] Failed to reload event +%v, writeKey: %s, eventType: %s, eventIdentifier: %s`, archivedModel.UUID, writeKey, eventType, eventIdentifier)
					return
				}
				stats.NewTaggedStat("reload_archived_event_model", stats.CountType, stats.Tags{"module": "event_schemas", "writeKey": eventModel.WriteKey, "eventIdentifier": eventModel.EventIdentifier}).Increment()
			}
		} else {
			eventModel = manager.createModel(writeKey, eventType, eventIdentifier, eventModel, totalEventModels, archiveOldestLastSeenModel)
		}
	}
	eventModel.LastSeen = timeutil.Now()

	eventMap := map[string]interface{}(event)
	flattenedEvent, err := flatten.Flatten((eventMap), "", flatten.DotStyle)
	if err != nil {
		pkgLogger.Debug(fmt.Sprintf("[EventSchemas] Failed to flatten the event +%v with error: %s", eventMap, err.Error()))
		return
	}

	schema := getSchema(flattenedEvent)
	schemaHash := getSchemaHash(schema)
	computeFrequencies(flattenedEvent, schemaHash)
	computeFrequencies(flattenedEvent, eventModel.UUID)

	var schemaVersion *SchemaVersionT
	var schemaFoundInCache bool
	schemaVersion, schemaFoundInCache = manager.schemaVersionMap[eventModel.UUID][schemaHash]

	if !schemaFoundInCache {
		// check in offloaded schema versions
		var wasOffloaded bool
		var offloadedVersion *OffloadedSchemaVersionT
		if bySchemaHash, ok := offloadedSchemaVersions[eventModel.UUID]; ok {
			offloadedVersion, wasOffloaded = bySchemaHash[schemaHash]
		}

		// check in archived schema versions
		var wasArchived bool
		var archivedVersion *OffloadedSchemaVersionT
		if bySchemaHash, ok := archivedSchemaVersions[eventModel.UUID]; ok {
			archivedVersion, wasArchived = bySchemaHash[schemaHash]
		}

		archiveOldestLastSeenVersion := func() {
			oldestVersion := manager.oldestSeenVersion(eventModel.UUID)
			toDeleteSchemaVersionIDs = append(toDeleteSchemaVersionIDs, oldestVersion.UUID)
			manager.deleteFromSchemaVersionCache(oldestVersion)
			if _, ok := archivedSchemaVersions[oldestVersion.EventModelID]; !ok {
				archivedSchemaVersions[oldestVersion.EventModelID] = make(map[string]*OffloadedSchemaVersionT)
			}
			archivedSchemaVersions[oldestVersion.EventModelID][oldestVersion.SchemaHash] = &OffloadedSchemaVersionT{UUID: oldestVersion.UUID, LastSeen: oldestVersion.LastSeen, EventModelID: oldestVersion.EventModelID, SchemaHash: oldestVersion.SchemaHash}
			stats.NewTaggedStat("archive_schema_version", stats.CountType, stats.Tags{"module": "event_schemas", "writeKey": eventModel.WriteKey, "eventIdentifier": eventModel.EventIdentifier}).Increment()
		}

		totalSchemaVersions := len(manager.schemaVersionMap[eventModel.UUID])
		totalSchemaVersions += len(offloadedSchemaVersions[eventModel.UUID])

		if wasOffloaded {
			manager.reloadSchemaVersion(offloadedVersion)
			schemaVersion, ok = manager.schemaVersionMap[eventModel.UUID][schemaHash]
			if !ok {
				pkgLogger.Errorf(`[EventSchemas] Failed to reload event +%v, writeKey: %s, eventType: %s, eventIdentifier: %s`, offloadedVersion.UUID, writeKey, eventType, eventIdentifier)
				return
			}
			stats.NewTaggedStat("reload_offloaded_schema_version", stats.CountType, stats.Tags{"module": "event_schemas", "writeKey": eventModel.WriteKey, "eventIdentifier": eventModel.EventIdentifier}).Increment()
		} else if wasArchived {
			if totalSchemaVersions >= schemaVersionPerEventModelLimit {
				archiveOldestLastSeenVersion()
			}
			err := manager.reloadSchemaVersion(archivedVersion)
			if err != nil {
				schemaVersion = manager.createSchema(schema, schemaHash, eventModel, totalSchemaVersions, archiveOldestLastSeenVersion)
			} else {
				schemaVersion, ok = manager.schemaVersionMap[eventModel.UUID][schemaHash]
				if !ok {
					pkgLogger.Errorf(`[EventSchemas] Failed to reload event +%v, writeKey: %s, eventType: %s, eventIdentifier: %s`, archivedVersion.UUID, writeKey, eventType, eventIdentifier)
					return
				}
				stats.NewTaggedStat("reload_archived_schema_version", stats.CountType, stats.Tags{"module": "event_schemas", "writeKey": eventModel.WriteKey, "eventIdentifier": eventModel.EventIdentifier}).Increment()
			}
		} else {
			schemaVersion = manager.createSchema(schema, schemaHash, eventModel, totalSchemaVersions, archiveOldestLastSeenVersion)
		}
	}
	schemaVersion.LastSeen = timeutil.Now()
	manager.updateSchemaVersionCache(schemaVersion, true)

	eventModel.reservoirSample.add(event, true)
	schemaVersion.reservoirSample.add(event, true)
	updatedEventModels[eventModel.UUID] = eventModel
}

func (manager *EventSchemaManagerT) createModel(writeKey string, eventType string, eventIdentifier string, eventModel *EventModelT, totalEventModels int, archiveOldestLastSeenModel func()) *EventModelT {
	eventModelID := uuid.Must(uuid.NewV4()).String()
	eventModel = &EventModelT{
		UUID:            eventModelID,
		WriteKey:        writeKey,
		EventType:       eventType,
		EventIdentifier: eventIdentifier,
		Schema:          []byte("{}"),
	}

	eventModel.reservoirSample = NewReservoirSampler(reservoirSampleSize, 0, 0)

	if totalEventModels >= eventModelLimit {
		archiveOldestLastSeenModel()
	}
	manager.updateEventModelCache(eventModel, true)
	stats.NewTaggedStat("record_new_event_model", stats.CountType, stats.Tags{"module": "event_schemas", "writeKey": eventModel.WriteKey, "eventIdentifier": eventModel.EventIdentifier}).Increment()
	return eventModel
}

func (manager *EventSchemaManagerT) createSchema(schema map[string]string, schemaHash string, eventModel *EventModelT, totalSchemaVersions int, archiveOldestLastSeenVersion func()) *SchemaVersionT {
	versionID := uuid.Must(uuid.NewV4()).String()
	schemaVersion := manager.NewSchemaVersion(versionID, schema, schemaHash, eventModel.UUID)
	eventModel.mergeSchema(schemaVersion)

	if totalSchemaVersions >= schemaVersionPerEventModelLimit {
		archiveOldestLastSeenVersion()
	}
	stats.NewTaggedStat("record_new_schema_version", stats.CountType, stats.Tags{"module": "event_schemas", "writeKey": eventModel.WriteKey, "eventIdentifier": eventModel.EventIdentifier}).Increment()
	return schemaVersion
}

func (manager *EventSchemaManagerT) oldestSeenModel(writeKey string) *EventModelT {
	var oldestSeenModel *EventModelT
	var minLastSeen time.Time
	for _, eventIdentifierMap := range manager.eventModelMap[WriteKey(writeKey)] {
		for _, model := range eventIdentifierMap {
			if !model.LastSeen.IsZero() && (model.LastSeen.Sub(minLastSeen).Seconds() <= 0 || minLastSeen.IsZero()) {
				oldestSeenModel = model
				minLastSeen = model.LastSeen
			}
		}
	}
	for _, offloadedModel := range offloadedEventModels[writeKey] {
		if !offloadedModel.LastSeen.IsZero() && (offloadedModel.LastSeen.Sub(minLastSeen).Seconds() <= 0 || minLastSeen.IsZero()) {
			model := EventModelT{}
			model.UUID = offloadedModel.UUID
			model.WriteKey = offloadedModel.WriteKey
			model.EventType = offloadedModel.EventType
			model.EventIdentifier = offloadedModel.EventIdentifier
			model.LastSeen = offloadedModel.LastSeen
			minLastSeen = offloadedModel.LastSeen
			oldestSeenModel = &model
		}
	}
	return oldestSeenModel
}

func (manager *EventSchemaManagerT) oldestSeenVersion(modelID string) *SchemaVersionT {
	var oldestSeenSchemaVersion SchemaVersionT
	var minLastSeen time.Time
	for _, schemaVersion := range manager.schemaVersionMap[modelID] {
		if !schemaVersion.LastSeen.IsZero() && (schemaVersion.LastSeen.Sub(minLastSeen).Seconds() <= 0 || minLastSeen.IsZero()) {
			oldestSeenSchemaVersion = *schemaVersion
			minLastSeen = schemaVersion.LastSeen
		}
	}
	for _, offloadedVersion := range offloadedSchemaVersions[modelID] {
		if !offloadedVersion.LastSeen.IsZero() && (offloadedVersion.LastSeen.Sub(minLastSeen).Seconds() <= 0 || minLastSeen.IsZero()) {
			oldestSeenSchemaVersion = SchemaVersionT{}
			oldestSeenSchemaVersion.UUID = offloadedVersion.UUID
			oldestSeenSchemaVersion.EventModelID = offloadedVersion.EventModelID
			oldestSeenSchemaVersion.SchemaHash = offloadedVersion.SchemaHash
			oldestSeenSchemaVersion.LastSeen = offloadedVersion.LastSeen
			minLastSeen = offloadedVersion.LastSeen
		}
	}
	return &oldestSeenSchemaVersion
}

func (em *EventModelT) mergeSchema(sv *SchemaVersionT) {
	masterSchema := make(map[string]string)
	err := json.Unmarshal(em.Schema, &masterSchema)
	assertError(err)

	schema := make(map[string]string)
	err = json.Unmarshal(sv.Schema, &schema)
	assertError(err)

	errors := make([]string, 0)
	for k := range schema {
		t, ok := masterSchema[k]
		if !ok {
			masterSchema[k] = schema[k]
			continue
		}
		if !strings.Contains(t, schema[k]) {
			masterSchema[k] = fmt.Sprintf("%s,%s", t, schema[k])
		}
	}

	if len(errors) > 0 {
		pkgLogger.Errorf("EventModel with ID: %s has encountered following disparities:\n%s", em.ID, strings.Join(errors, "\n"))
	}

	masterSchemaJSON, err := json.Marshal(masterSchema)
	assertError(err)
	em.Schema = masterSchemaJSON
}

//NewSchemaVersion should be used when a schemaVersion is not found in its cache and requires, a schemaVersionID for the newSchema and the eventModelID to which it belongs along with schema and schemaHash
func (manager *EventSchemaManagerT) NewSchemaVersion(versionID string, schema map[string]string, schemaHash string, eventModelID string) *SchemaVersionT {
	schemaJSON, err := json.Marshal(schema)
	assertError(err)

	schemaVersion := &SchemaVersionT{
		UUID:         versionID,
		SchemaHash:   schemaHash,
		EventModelID: eventModelID,
		Schema:       schemaJSON,
		FirstSeen:    timeutil.Now(),
		LastSeen:     timeutil.Now(),
	}
	schemaVersion.reservoirSample = NewReservoirSampler(reservoirSampleSize, 0, 0)
	return schemaVersion
}

func (manager *EventSchemaManagerT) recordEvents() {
	for gatewayEventBatch := range eventSchemaChannel {
		if !areEventSchemasPopulated {
			continue
		}
		var eventPayload EventPayloadT
		err := json.Unmarshal([]byte(gatewayEventBatch.eventBatch), &eventPayload)
		assertError(err)
		for _, event := range eventPayload.Batch {
			manager.handleEvent(eventPayload.WriteKey, event)
		}
	}
}

func getMetadataJSON(reservoirSample *ReservoirSample, schemaHash string) []byte {
	metadata := &MetaDataT{
		SampledEvents: reservoirSample.getSamples(),
		TotalCount:    reservoirSample.getTotalCount(),
	}
	metadata.Counters = getSchemaVersionCounters(schemaHash)

	metadataJSON, err := json.Marshal(metadata)
	pkgLogger.Debugf("[EventSchemas] Metadata JSON: %s", string(metadataJSON))
	assertError(err)
	return metadataJSON
}

func getPrivateDataJSON(schemaHash string) []byte {
	privateData := &PrivateDataT{
		FrequencyCounters: getAllFrequencyCounters(schemaHash),
	}

	privateDataJSON, err := json.Marshal(privateData)
	pkgLogger.Debugf("[EventSchemas] Private Data JSON: %s", string(privateDataJSON))
	assertError(err)
	return privateDataJSON

}

func (manager *EventSchemaManagerT) flushEventSchemas() {
	var flushDBHandle *sql.DB
	defer func() {
		if r := recover(); r != nil {
			// If some of the panicking happens while doing the flushing of events, then we need to close the connection.
			if flushDBHandle != nil {
				flushDBHandle.Close()
			}
		}
	}()

	// This will run forever. If you want to quit in between, change it to ticker and call stop()
	// Otherwise the ticker won't be GC'ed
	ticker := time.Tick(flushInterval)
	for range ticker {
		if !areEventSchemasPopulated {
			continue
		}

		// If needed, copy the maps and release the lock immediately
		manager.eventModelLock.Lock()
		manager.schemaVersionLock.Lock()

		schemaVersionsInCache := make([]*SchemaVersionT, 0)
		for _, sv := range updatedSchemaVersions {
			schemaVersionsInCache = append(schemaVersionsInCache, sv)
		}

		if len(updatedEventModels) == 0 && len(schemaVersionsInCache) == 0 {
			manager.eventModelLock.Unlock()
			manager.schemaVersionLock.Unlock()
			continue
		}

		flushDBHandle = createDBConnection()

		txn, err := flushDBHandle.Begin()
		assertError(err)

		// Handle Event Models
		if len(updatedEventModels) > 0 {
			eventModelIds := make([]string, 0, len(updatedEventModels))
			for _, em := range updatedEventModels {
				eventModelIds = append(eventModelIds, em.UUID)
			}

			deleteOldEventModelsSQL := fmt.Sprintf(`DELETE FROM %s WHERE uuid IN ('%s')`, EVENT_MODELS_TABLE, strings.Join(eventModelIds, "', '"))
			_, err := txn.Exec(deleteOldEventModelsSQL)
			assertTxnError(err, txn)

			if len(toDeleteEventModelIDs) > 0 {
				archiveOldEventModelsSQL := fmt.Sprintf(`UPDATE %s SET archived=%t WHERE uuid IN ('%s')`, EVENT_MODELS_TABLE, true, strings.Join(toDeleteEventModelIDs, "', '"))
				_, err := txn.Exec(archiveOldEventModelsSQL)
				assertTxnError(err, txn)

				archiveVersionsForArchivedModelsSQL := fmt.Sprintf(`UPDATE %s SET archived=%t WHERE event_model_id IN ('%s')`, SCHEMA_VERSIONS_TABLE, true, strings.Join(toDeleteEventModelIDs, "', '"))
				_, err = txn.Exec(archiveVersionsForArchivedModelsSQL)
				assertTxnError(err, txn)
			}

			stmt, err := txn.Prepare(pq.CopyIn(EVENT_MODELS_TABLE, "uuid", "write_key", "event_type", "event_model_identifier", "schema", "metadata", "private_data", "last_seen", "total_count"))
			assertTxnError(err, txn)
			//skipcq: SCC-SA9001
			defer stmt.Close()
			for eventModelID, eventModel := range updatedEventModels {
				metadataJSON := getMetadataJSON(eventModel.reservoirSample, eventModel.UUID)
				privateDataJSON := getPrivateDataJSON(eventModel.UUID)
				eventModel.TotalCount = eventModel.reservoirSample.totalCount

				_, err = stmt.Exec(eventModelID, eventModel.WriteKey, eventModel.EventType, eventModel.EventIdentifier, string(eventModel.Schema), string(metadataJSON), string(privateDataJSON), eventModel.LastSeen, eventModel.TotalCount)
				assertTxnError(err, txn)
			}
			_, err = stmt.Exec()
			assertTxnError(err, txn)
			stats.NewTaggedStat("update_event_model_count", stats.GaugeType, stats.Tags{"module": "event_schemas"}).Gauge(len(eventModelIds))
			pkgLogger.Debugf("[EventSchemas][Flush] %d new event types", len(updatedEventModels))
		}

		//Handle Schema Versions
		if len(schemaVersionsInCache) > 0 {
			versionIDs := make([]string, 0, len(schemaVersionsInCache))
			for uid := range updatedSchemaVersions {
				versionIDs = append(versionIDs, uid)
			}

			deleteOldVersionsSQL := fmt.Sprintf(`DELETE FROM %s WHERE uuid IN ('%s')`, SCHEMA_VERSIONS_TABLE, strings.Join(versionIDs, "', '"))
			_, err := txn.Exec(deleteOldVersionsSQL)
			assertTxnError(err, txn)

			if len(toDeleteSchemaVersionIDs) > 0 {
				archiveVersionsSQL := fmt.Sprintf(`UPDATE %s SET archived=%t WHERE uuid IN ('%s')`, SCHEMA_VERSIONS_TABLE, true, strings.Join(toDeleteSchemaVersionIDs, "', '"))
				_, err = txn.Exec(archiveVersionsSQL)
				assertTxnError(err, txn)
			}

			stmt, err := txn.Prepare(pq.CopyIn(SCHEMA_VERSIONS_TABLE, "uuid", "event_model_id", "schema_hash", "schema", "metadata", "private_data", "first_seen", "last_seen", "total_count"))
			assertTxnError(err, txn)
			//skipcq: SCC-SA9001
			defer stmt.Close()
			for _, sv := range schemaVersionsInCache {
				metadataJSON := getMetadataJSON(sv.reservoirSample, sv.SchemaHash)
				privateDataJSON := getPrivateDataJSON(sv.SchemaHash)
				sv.TotalCount = sv.reservoirSample.totalCount

				_, err = stmt.Exec(sv.UUID, sv.EventModelID, sv.SchemaHash, string(sv.Schema), string(metadataJSON), string(privateDataJSON), sv.FirstSeen, sv.LastSeen, sv.TotalCount)
				assertTxnError(err, txn)
			}
			_, err = stmt.Exec()
			assertTxnError(err, txn)
			stats.NewTaggedStat("update_schema_version_count", stats.GaugeType, stats.Tags{"module": "event_schemas"}).Gauge(len(versionIDs))
			pkgLogger.Debugf("[EventSchemas][Flush] %d new schema versions", len(schemaVersionsInCache))
		}

		err = txn.Commit()
		assertTxnError(err, txn)

		flushDBHandle.Close()

		updatedEventModels = make(map[string]*EventModelT)
		updatedSchemaVersions = make(map[string]*SchemaVersionT)
		toDeleteEventModelIDs = []string{}
		toDeleteSchemaVersionIDs = []string{}

		manager.schemaVersionLock.Unlock()
		manager.eventModelLock.Unlock()
	}
}

func eventTypeIdentifier(eventType, eventIdentifier string) string {
	return fmt.Sprintf(`%s::%s`, eventType, eventIdentifier)
}

func (manager *EventSchemaManagerT) offloadEventSchemas() {
	for {
		if !areEventSchemasPopulated {
			time.Sleep(time.Second * 10)
			continue
		}

		time.Sleep(offloadLoopInterval)
		manager.eventModelLock.Lock()
		manager.schemaVersionLock.Lock()
		for _, modelsByWriteKey := range manager.eventModelMap {
			for _, modelsByEventType := range modelsByWriteKey {
				for _, model := range modelsByEventType {
					if timeutil.Now().Sub(model.LastSeen) > offloadThreshold {
						pkgLogger.Infof("offloading model: %s-%s UUID:%s", model.EventType, model.EventIdentifier, model.UUID)
						if _, ok := offloadedEventModels[model.WriteKey]; !ok {
							offloadedEventModels[model.WriteKey] = make(map[string]*OffloadedModelT)
						}
						manager.deleteFromEventModelCache(model)
						offloadedEventModels[model.WriteKey][eventTypeIdentifier(model.EventType, model.EventIdentifier)] = &OffloadedModelT{UUID: model.UUID, LastSeen: model.LastSeen, WriteKey: model.WriteKey, EventType: model.EventType, EventIdentifier: model.EventIdentifier}
						stats.NewTaggedStat("offload_event_model", stats.CountType, stats.Tags{"module": "event_schemas", "writeKey": model.WriteKey, "eventIdentifier": model.EventIdentifier}).Increment()
					}
				}
			}
		}
		for _, modelsByWriteKey := range manager.schemaVersionMap {
			for _, version := range modelsByWriteKey {
				if timeutil.Now().Sub(version.LastSeen) > offloadThreshold {
					if _, ok := offloadedSchemaVersions[version.EventModelID]; !ok {
						offloadedSchemaVersions[version.EventModelID] = make(map[string]*OffloadedSchemaVersionT)
					}
					manager.deleteFromSchemaVersionCache(&SchemaVersionT{EventModelID: version.EventModelID, SchemaHash: version.SchemaHash})
					offloadedSchemaVersions[version.EventModelID][version.SchemaHash] = &OffloadedSchemaVersionT{UUID: version.UUID, LastSeen: version.LastSeen, EventModelID: version.EventModelID, SchemaHash: version.SchemaHash}
					stats.NewTaggedStat("offload_schema_version", stats.CountType, stats.Tags{"module": "event_schemas"}).Increment()
				}
			}
		}
		manager.schemaVersionLock.Unlock()
		manager.eventModelLock.Unlock()
	}
}

func (manager *EventSchemaManagerT) reloadModel(offloadedModel *OffloadedModelT) error {
	pkgLogger.Infof("reloading event model from db: %s\n", offloadedModel.UUID)
	err := manager.populateEventModels(offloadedModel.UUID)
	if err != nil {
		return err
	}
	manager.populateSchemaVersionsMinimal(offloadedModel.UUID)
	delete(offloadedEventModels[offloadedModel.WriteKey], eventTypeIdentifier(offloadedModel.EventType, offloadedModel.EventIdentifier))
	delete(archivedEventModels[offloadedModel.WriteKey], eventTypeIdentifier(offloadedModel.EventType, offloadedModel.EventIdentifier))
	return nil
}

func (manager *EventSchemaManagerT) reloadSchemaVersion(offloadedVersion *OffloadedSchemaVersionT) error {
	pkgLogger.Debugf("reloading schema vesion from db: %s\n", offloadedVersion.UUID)
	err := manager.populateSchemaVersion(offloadedVersion)
	if err != nil {
		return err
	}
	delete(offloadedSchemaVersions[offloadedVersion.EventModelID], offloadedVersion.SchemaHash)
	delete(archivedSchemaVersions[offloadedVersion.EventModelID], offloadedVersion.SchemaHash)
	return nil
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
		pkgLogger.Info(fmt.Sprintf("%#v\n", err))
		pkgLogger.Info(fmt.Sprintf("%#v\n", txn))
		panic(err)
	}
}

func (manager *EventSchemaManagerT) populateEventModels(uuidFilters ...string) error {

	var uuidFilter string
	if len(uuidFilters) > 0 {
		uuidFilter = fmt.Sprintf(`WHERE uuid in ('%s')`, strings.Join(uuidFilters, "', '"))
	}

	eventModelsSelectSQL := fmt.Sprintf(`SELECT id, uuid, write_key, event_type, event_model_identifier, created_at, schema, private_data, total_count, last_seen, (metadata->>'TotalCount')::int, metadata->'SampledEvents' FROM %s %s`, EVENT_MODELS_TABLE, uuidFilter)

	rows, err := manager.dbHandle.Query(eventModelsSelectSQL)
	if err == sql.ErrNoRows {
		return err
	} else {
		assertError(err)
	}
	defer rows.Close()

	for rows.Next() {
		var eventModel EventModelT
		var privateDataRaw json.RawMessage
		var totalCount int64
		var sampleEventsRaw json.RawMessage
		err := rows.Scan(&eventModel.ID, &eventModel.UUID, &eventModel.WriteKey, &eventModel.EventType,
			&eventModel.EventIdentifier, &eventModel.CreatedAt, &eventModel.Schema,
			&privateDataRaw, &eventModel.TotalCount, &eventModel.LastSeen, &totalCount, &sampleEventsRaw)

		assertError(err)

		var privateData PrivateDataT
		err = json.Unmarshal(privateDataRaw, &privateData)
		assertError(err)

		var sampleEvents []interface{}
		err = json.Unmarshal(sampleEventsRaw, &sampleEvents)
		assertError(err)

		reservoirSize := len(sampleEvents)
		if reservoirSize > reservoirSampleSize {
			reservoirSize = reservoirSampleSize
		}
		eventModel.reservoirSample = NewReservoirSampler(reservoirSampleSize, reservoirSize, totalCount)
		for idx, sampledEvent := range sampleEvents {
			if idx > reservoirSampleSize-1 {
				continue
			}
			eventModel.reservoirSample.add(sampledEvent, false)
		}
		manager.updateEventModelCache(&eventModel, false)
		populateFrequencyCounters(eventModel.UUID, privateData.FrequencyCounters)
	}
	return nil
}

func (manager *EventSchemaManagerT) populateEventModelsMinimal() {
	eventModelsSelectSQL := fmt.Sprintf(`SELECT uuid, event_type, event_model_identifier, write_key, last_seen, archived FROM %s`, EVENT_MODELS_TABLE)

	rows, err := manager.dbHandle.Query(eventModelsSelectSQL)
	assertError(err)
	defer rows.Close()

	for rows.Next() {
		var eventModel EventModelT
		err := rows.Scan(&eventModel.UUID, &eventModel.EventType, &eventModel.EventIdentifier, &eventModel.WriteKey, &eventModel.LastSeen, &eventModel.Archived)

		assertError(err)

		if eventModel.Archived {
			if _, ok := archivedEventModels[eventModel.WriteKey]; !ok {
				archivedEventModels[eventModel.WriteKey] = make(map[string]*OffloadedModelT)
			}

			archivedEventModels[eventModel.WriteKey][eventTypeIdentifier(eventModel.EventType, eventModel.EventIdentifier)] = &OffloadedModelT{UUID: eventModel.UUID, LastSeen: eventModel.LastSeen, WriteKey: eventModel.WriteKey, EventType: eventModel.EventType, EventIdentifier: eventModel.EventIdentifier}
		} else {
			if _, ok := offloadedEventModels[eventModel.WriteKey]; !ok {
				offloadedEventModels[eventModel.WriteKey] = make(map[string]*OffloadedModelT)
			}

			offloadedEventModels[eventModel.WriteKey][eventTypeIdentifier(eventModel.EventType, eventModel.EventIdentifier)] = &OffloadedModelT{UUID: eventModel.UUID, LastSeen: eventModel.LastSeen, WriteKey: eventModel.WriteKey, EventType: eventModel.EventType, EventIdentifier: eventModel.EventIdentifier}
		}
	}
}

func (manager *EventSchemaManagerT) populateSchemaVersionsMinimal(modelIDFilters ...string) {
	var modelIDFilter string
	if len(modelIDFilters) > 0 {
		modelIDFilter = fmt.Sprintf(`WHERE event_model_id in ('%s')`, strings.Join(modelIDFilters, "', '"))
	}

	schemaVersionsSelectSQL := fmt.Sprintf(`SELECT uuid, event_model_id, schema_hash, last_seen, archived FROM %s %s`, SCHEMA_VERSIONS_TABLE, modelIDFilter)

	rows, err := manager.dbHandle.Query(schemaVersionsSelectSQL)
	assertError(err)
	defer rows.Close()

	for rows.Next() {
		var schemaVersion SchemaVersionT
		err := rows.Scan(&schemaVersion.UUID, &schemaVersion.EventModelID, &schemaVersion.SchemaHash, &schemaVersion.LastSeen, &schemaVersion.Archived)
		assertError(err)

		if schemaVersion.Archived {
			if _, ok := archivedSchemaVersions[schemaVersion.EventModelID]; !ok {
				archivedSchemaVersions[schemaVersion.EventModelID] = make(map[string]*OffloadedSchemaVersionT)
			}
			archivedSchemaVersions[schemaVersion.EventModelID][schemaVersion.SchemaHash] = &OffloadedSchemaVersionT{UUID: schemaVersion.UUID, LastSeen: schemaVersion.LastSeen, EventModelID: schemaVersion.EventModelID, SchemaHash: schemaVersion.SchemaHash}
		} else {
			if _, ok := offloadedSchemaVersions[schemaVersion.EventModelID]; !ok {
				offloadedSchemaVersions[schemaVersion.EventModelID] = make(map[string]*OffloadedSchemaVersionT)
			}
			offloadedSchemaVersions[schemaVersion.EventModelID][schemaVersion.SchemaHash] = &OffloadedSchemaVersionT{UUID: schemaVersion.UUID, LastSeen: schemaVersion.LastSeen, EventModelID: schemaVersion.EventModelID, SchemaHash: schemaVersion.SchemaHash}
		}

	}
}

func (manager *EventSchemaManagerT) populateSchemaVersion(o *OffloadedSchemaVersionT) error {
	schemaVersionsSelectSQL := fmt.Sprintf(`SELECT id, uuid, event_model_id, schema_hash, schema, private_data,first_seen, last_seen, total_count, (metadata->>'TotalCount')::int, metadata->'SampledEvents' FROM %s WHERE uuid = '%s'`, SCHEMA_VERSIONS_TABLE, o.UUID)

	var schemaVersion SchemaVersionT
	var privateDataRaw json.RawMessage
	var totalCount int64
	var sampleEventsRaw json.RawMessage

	err := manager.dbHandle.QueryRow(schemaVersionsSelectSQL).Scan(&schemaVersion.ID, &schemaVersion.UUID, &schemaVersion.EventModelID, &schemaVersion.SchemaHash, &schemaVersion.Schema, &privateDataRaw, &schemaVersion.FirstSeen, &schemaVersion.LastSeen, &schemaVersion.TotalCount, &totalCount, &sampleEventsRaw)
	if err == sql.ErrNoRows {
		return err
	} else {
		assertError(err)
	}

	var privateData PrivateDataT
	err = json.Unmarshal(privateDataRaw, &privateData)
	assertError(err)

	var sampleEvents []interface{}
	err = json.Unmarshal(sampleEventsRaw, &sampleEvents)
	assertError(err)

	reservoirSize := len(sampleEvents)
	if reservoirSize > reservoirSampleSize {
		reservoirSize = reservoirSampleSize
	}
	schemaVersion.reservoirSample = NewReservoirSampler(reservoirSampleSize, reservoirSize, totalCount)
	for idx, sampledEvent := range sampleEvents {
		if idx > reservoirSampleSize-1 {
			continue
		}
		schemaVersion.reservoirSample.add(sampledEvent, false)
	}

	manager.updateSchemaVersionCache(&schemaVersion, false)

	populateFrequencyCounters(schemaVersion.SchemaHash, privateData.FrequencyCounters)
	return nil
}

// This should be called during the Initialize() to populate existing event Schemas
func (manager *EventSchemaManagerT) populateEventSchemas() {
	pkgLogger.Infof(`Populating event models and their schema versions into in-memory`)
	manager.populateEventModelsMinimal()
	manager.populateSchemaVersionsMinimal()
}

func setEventSchemasPopulated(status bool) {
	areEventSchemasPopulated = status
}

func getSchema(flattenedEvent map[string]interface{}) map[string]string {
	schema := make(map[string]string)
	for k, v := range flattenedEvent {
		reflectType := reflect.TypeOf(v)
		if reflectType != nil {
			schema[k] = reflectType.String()
		} else {
			if !(v == nil && !shouldCaptureNilAsUnknowns) {
				schema[k] = "unknown"
				pkgLogger.Errorf("[EventSchemas] Got invalid reflectType %+v", v)
			}
		}
	}
	return schema
}

func getSchemaHash(schema map[string]string) string {
	keys := make([]string, 0, len(schema))
	for k := range schema {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var sb strings.Builder
	for _, k := range keys {
		sb.WriteString(k)
		sb.WriteString(":")
		sb.WriteString(schema[k])
		sb.WriteString(",")
	}

	schemaHash := misc.GetMD5Hash(sb.String())
	return schemaHash
}

func computeFrequencies(flattenedEvent map[string]interface{}, schemaHash string) {
	// Frequency Counting: Second pass, dependent on schemaHash
	for k, v := range flattenedEvent {
		fc := getFrequencyCounter(schemaHash, k)
		stringVal := fmt.Sprintf("%v", v)
		fc.Observe(&stringVal)
	}
}

func (manager *EventSchemaManagerT) Setup() {
	pkgLogger.Info("[EventSchemas] Setting up eventSchemas...")
	// Clean this up
	manager.dbHandle = createDBConnection()

	// Following data structures store events and schemas since last flush
	updatedEventModels = make(map[string]*EventModelT)
	updatedSchemaVersions = make(map[string]*SchemaVersionT)

	manager.eventModelMap = make(EventModelMapT)
	manager.schemaVersionMap = make(SchemaVersionMapT)

	offloadedEventModels = make(map[string]map[string]*OffloadedModelT)
	offloadedSchemaVersions = make(map[string]map[string]*OffloadedSchemaVersionT)
	archivedEventModels = make(map[string]map[string]*OffloadedModelT)
	archivedSchemaVersions = make(map[string]map[string]*OffloadedSchemaVersionT)

	if !manager.disableInMemoryCache {
		rruntime.GoForWarehouse(func() {
			defer setEventSchemasPopulated(true)

			populateESTimer := stats.NewTaggedStat("populate_event_schemas", stats.TimerType, stats.Tags{"module": "event_schemas"})
			populateESTimer.Start()
			defer populateESTimer.End()

			manager.populateEventSchemas()
		})
	}
	eventSchemaChannel = make(chan *GatewayEventBatchT, 10000)

	for i := 0; i < noOfWorkers; i++ {
		rruntime.GoForWarehouse(func() {
			manager.recordEvents()
		})
	}

	rruntime.GoForWarehouse(func() {
		manager.flushEventSchemas()
	})

	rruntime.GoForWarehouse(func() {
		manager.offloadEventSchemas()
	})

	pkgLogger.Info("[EventSchemas] Set up eventSchemas successful.")
}
