// Package event_schema
// Handling HTTP requests to expose the schemas
package event_schema

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/gorilla/mux"

	"github.com/rudderlabs/rudder-server/gateway/response"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

func handleBasicAuth(r *http.Request) error {
	username, password, ok := r.BasicAuth()
	if !ok {
		return fmt.Errorf("Basic auth credentials missing")
	}
	if username != adminUser || password != adminPassword {
		return fmt.Errorf("Invalid admin credentials")
	}
	return nil
}

func (manager *EventSchemaManagerT) GetEventModels(w http.ResponseWriter, r *http.Request) {
	err := handleBasicAuth(r)
	if err != nil {
		http.Error(w, response.MakeResponse(err.Error()), 400)
		return
	}

	if r.Method != http.MethodGet {
		http.Error(w, response.MakeResponse("Only HTTP GET method is supported"), 400)
		return
	}

	writeKeys, ok := r.URL.Query()["WriteKey"]
	writeKey := ""
	if ok && writeKeys[0] != "" {
		writeKey = writeKeys[0]
	}

	eventTypes := manager.fetchEventModelsByWriteKey(writeKey)

	eventTypesJSON, err := json.Marshal(eventTypes)
	if err != nil {
		http.Error(w, response.MakeResponse("Internal Error: Failed to Marshal event types"), 500)
		return
	}

	_, _ = w.Write(eventTypesJSON)
}

func (manager *EventSchemaManagerT) GetJsonSchemas(w http.ResponseWriter, r *http.Request) {
	err := handleBasicAuth(r)
	if err != nil {
		http.Error(w, response.MakeResponse(err.Error()), 400)
		return
	}

	if r.Method != http.MethodGet {
		http.Error(w, response.MakeResponse("Only HTTP GET method is supported"), 400)
		return
	}

	writeKeys, ok := r.URL.Query()["WriteKey"]
	writeKey := ""
	if ok && writeKeys[0] != "" {
		writeKey = writeKeys[0]
	}

	eventModels := manager.fetchEventModelsByWriteKey(writeKey)
	if len(eventModels) == 0 {
		http.Error(w, response.MakeResponse("No event models exists to create a tracking plan."), 404)
		return
	}

	// generating json schema from eventModels
	jsonSchemas, err := generateJsonSchFromEM(eventModels)
	if err != nil {
		http.Error(w, response.MakeResponse("Internal Error: Failed to Marshal event types"), 500)
		return
	}

	_, _ = w.Write(jsonSchemas)
}

type JSPropertyTypeT struct {
	Type []string `json:"type"`
}

type JSPropertyT struct {
	Property map[string]interface{} `json:"properties"`
}

type JsonSchemaT struct {
	Schema            map[string]interface{} `json:"schema"`
	SchemaType        string                 `json:"schemaType"`
	SchemaTIdentifier string                 `json:"schemaIdentifier"`
}

// generateJsonSchFromEM Generates Json schemas from Event Models
func generateJsonSchFromEM(eventModels []*EventModelT) ([]byte, error) {
	var jsonSchemas []JsonSchemaT
	for _, eventModel := range eventModels {
		flattenedSch := make(map[string]interface{})
		err := json.Unmarshal(eventModel.Schema, &flattenedSch)
		if err != nil {
			pkgLogger.Errorf("Error unmarshalling eventModelSch: %v for ID: %v", err, eventModel.ID)
			continue
		}
		unFlattenedSch, err := unflatten(flattenedSch)
		if err != nil {
			pkgLogger.Errorf("Error unflattening flattenedSch: %v for ID: %v", err, eventModel.ID)
			continue
		}
		schemaProperties, err := getETSchProp(eventModel.EventType, unFlattenedSch)
		if err != nil {
			pkgLogger.Errorf("Error while getting schema properties: %v for ID: %v", err, eventModel.ID)
			continue
		}

		jsonSchema := generateJsonSchFromSchProp(schemaProperties)
		jsonSchema["additionalProperties"] = false
		jsonSchema["$schema"] = "http://json-schema.org/draft-07/schema#"

		// TODO: validate if the jsonSchema is correct.
		jsonSchemas = append(jsonSchemas, JsonSchemaT{
			Schema:            jsonSchema,
			SchemaType:        eventModel.EventType,
			SchemaTIdentifier: eventModel.EventIdentifier,
		})
	}
	eventJsonSchs, err := json.Marshal(jsonSchemas)
	if err != nil {
		return nil, err
	}
	return eventJsonSchs, nil
}

// getETSchProp Get Event Type schema from Event Model Schema
// Empty map schema is allowed for event json schema
func getETSchProp(eventType string, eventModelSch map[string]interface{}) (map[string]interface{}, error) {
	switch eventType {
	case "track", "screen", "page":
		filtered, _ := eventModelSch["properties"].(map[string]interface{})
		return filtered, nil
	case "identify", "group":
		filtered, _ := eventModelSch["traits"].(map[string]interface{})
		return filtered, nil
	}
	return nil, fmt.Errorf("invalid eventType")
}

// generateJsonSchFromSchProp Generated Json schema from unflattened schema properties.
func generateJsonSchFromSchProp(schemaProperties map[string]interface{}) map[string]interface{} {
	jsProperties := JSPropertyT{
		Property: make(map[string]interface{}),
	}
	finalSchema := make(map[string]interface{})

	for k, v := range schemaProperties {
		switch value := v.(type) {
		case string:
			jsProperties.Property[k] = getPropertyTypesFromSchValue(value)
		case map[string]interface{}:
			// check if map is an array or map
			if checkIfArray(value) {
				var vType interface{}
				for _, v := range value {
					vt, ok := v.(string)
					if ok {
						vType = getPropertyTypesFromSchValue(vt)
					} else {
						vType = generateJsonSchFromSchProp(v.(map[string]interface{}))
					}
					break
				}
				jsProperties.Property[k] = map[string]interface{}{
					"type":  "array",
					"items": vType,
				}
				break
			}
			jsProperties.Property[k] = generateJsonSchFromSchProp(value)
		default:
			pkgLogger.Errorf("unknown type found")
		}
	}
	finalSchema["properties"] = jsProperties.Property
	finalSchema["type"] = "object"
	return finalSchema
}

func getPropertyTypesFromSchValue(schVal string) *JSPropertyTypeT {
	types := strings.Split(schVal, ",")
	for i, v := range types {
		types[i] = misc.GetJsonSchemaDTFromGoDT(v)
	}
	return &JSPropertyTypeT{
		Type: types,
	}
}

// prop.myarr.0
// will not be able to say if above is prop{myarr:[0]} or prop{myarr{"0":0}}
func checkIfArray(value map[string]interface{}) bool {
	if len(value) == 0 {
		return false
	}

	for k := range value {
		_, err := strconv.Atoi(k)
		if err != nil {
			return false
		}
		// need not check the array continuity
	}
	return true
}

// https://play.golang.org/p/4juOff38ea
// or use https://pkg.go.dev/github.com/wolfeidau/unflatten
// or use https://github.com/nqd/flat
func unflatten(flat map[string]interface{}) (map[string]interface{}, error) {
	unflat := map[string]interface{}{}

	for key, value := range flat {
		keyParts := strings.Split(key, ".")

		// Walk the keys until we get to a leaf node.
		m := unflat
		for i, k := range keyParts[:len(keyParts)-1] {
			v, exists := m[k]
			if !exists {
				newMap := map[string]interface{}{}
				m[k] = newMap
				m = newMap
				continue
			}

			innerMap, ok := v.(map[string]interface{})
			if !ok {
				fmt.Printf("key=%v is not an object\n", strings.Join(keyParts[0:i+1], "."))
				newMap := map[string]interface{}{}
				m[k] = newMap
				m = newMap
				continue
			}
			m = innerMap
		}

		leafKey := keyParts[len(keyParts)-1]
		if _, exists := m[leafKey]; exists {
			fmt.Printf("key=%v already exists", key)
			continue
		}
		m[keyParts[len(keyParts)-1]] = value
	}

	return unflat, nil
}

func (manager *EventSchemaManagerT) GetEventVersions(w http.ResponseWriter, r *http.Request) {
	err := handleBasicAuth(r)
	if err != nil {
		http.Error(w, response.MakeResponse(err.Error()), 400)
		return
	}

	if r.Method != http.MethodGet {
		http.Error(w, response.MakeResponse("Only HTTP GET method is supported"), 400)
		return
	}

	eventIDs, ok := r.URL.Query()["EventID"]
	if !ok {
		http.Error(w, response.MakeResponse("Mandatory field: EventID missing"), 400)
		return
	}
	eventID := eventIDs[0]

	schemaVersions := manager.fetchSchemaVersionsByEventID(eventID)
	schemaVersionsJSON, err := json.Marshal(schemaVersions)
	if err != nil {
		http.Error(w, response.MakeResponse("Internal Error: Failed to Marshal event types"), 500)
		return
	}

	_, _ = w.Write(schemaVersionsJSON)
}

// TODO: Complete this
func (manager *EventSchemaManagerT) GetKeyCounts(w http.ResponseWriter, r *http.Request) {
	err := handleBasicAuth(r)
	if err != nil {
		http.Error(w, response.MakeResponse(err.Error()), 400)
		return
	}

	if r.Method != http.MethodGet {
		http.Error(w, response.MakeResponse("Only HTTP GET method is supported"), 400)
		return
	}

	vars := mux.Vars(r)
	eventID, ok := vars["EventID"]
	if !ok {
		http.Error(w, response.MakeResponse("Mandatory field: EventID missing"), 400)
		return
	}

	keyCounts, err := manager.getKeyCounts(eventID)
	if err != nil {
		logID := uuid.New().String()
		pkgLogger.Errorf("logID : %s, err: %s", logID, err.Error())
		http.Error(w, response.MakeResponse(fmt.Sprintf("Internal Error: An error has been logged with logID : %s", logID)), 500)
		return
	}
	keyCountsJSON, err := json.Marshal(keyCounts)
	if err != nil {
		logID := uuid.New().String()
		pkgLogger.Errorf("logID : %s, err: %s", logID, err.Error())
		http.Error(w, response.MakeResponse(fmt.Sprintf("Interna Error: An error has been logged with logID : %s", logID)), 500)
		return
	}

	_, _ = w.Write(keyCountsJSON)
}

func (manager *EventSchemaManagerT) getKeyCounts(eventID string) (keyCounts map[string]int64, err error) {
	schemaVersions := manager.fetchSchemaVersionsByEventID(eventID)

	keyCounts = make(map[string]int64)
	for _, sv := range schemaVersions {
		var schema map[string]string
		err = json.Unmarshal(sv.Schema, &schema)
		if err != nil {
			return
		}
		for k := range schema {
			_, ok := keyCounts[k]
			if !ok {
				keyCounts[k] = 0
			}
			keyCounts[k] = keyCounts[k] + sv.TotalCount
		}
	}
	return
}

func (manager *EventSchemaManagerT) GetEventModelMetadata(w http.ResponseWriter, r *http.Request) {
	err := handleBasicAuth(r)
	if err != nil {
		http.Error(w, response.MakeResponse(err.Error()), 400)
		return
	}

	if r.Method != http.MethodGet {
		http.Error(w, response.MakeResponse("Only HTTP GET method is supported"), 400)
		return
	}

	vars := mux.Vars(r)
	eventID, ok := vars["EventID"]
	if !ok {
		http.Error(w, response.MakeResponse("Mandatory field: EventID missing"), 400)
		return
	}

	metadata, err := manager.fetchMetadataByEventModelID(eventID)
	if err != nil {
		http.Error(w, response.MakeResponse(err.Error()), 400)
		return
	}

	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		http.Error(w, response.MakeResponse("Internal Error: Failed to Marshal metadata"), 500)
		return
	}

	_, _ = w.Write(metadataJSON)
}

func (manager *EventSchemaManagerT) GetSchemaVersionMetadata(w http.ResponseWriter, r *http.Request) {
	err := handleBasicAuth(r)
	if err != nil {
		http.Error(w, response.MakeResponse(err.Error()), 400)
		return
	}

	if r.Method != http.MethodGet {
		http.Error(w, response.MakeResponse("Only HTTP GET method is supported"), 400)
		return
	}

	vars := mux.Vars(r)
	versionID, ok := vars["VersionID"]
	if !ok {
		http.Error(w, response.MakeResponse("Mandatory field: VersionID missing"), 400)
		return
	}

	metadata, err := manager.fetchMetadataByEventVersionID(versionID)
	if err != nil {
		http.Error(w, response.MakeResponse(err.Error()), 400)
		return
	}

	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		http.Error(w, response.MakeResponse("Internal Error: Failed to Marshal metadata"), 500)
		return
	}

	_, _ = w.Write(metadataJSON)
}

func (manager *EventSchemaManagerT) GetSchemaVersionMissingKeys(w http.ResponseWriter, r *http.Request) {
	err := handleBasicAuth(r)
	if err != nil {
		http.Error(w, response.MakeResponse(err.Error()), 400)
		return
	}

	if r.Method != http.MethodGet {
		http.Error(w, response.MakeResponse("Only HTTP GET method is supported"), 400)
		return
	}

	vars := mux.Vars(r)
	versionID, ok := vars["VersionID"]
	if !ok {
		http.Error(w, response.MakeResponse("Mandatory field: VersionID missing"), 400)
		return
	}

	schema, err := manager.fetchSchemaVersionByID(versionID)
	if err != nil {
		http.Error(w, response.MakeResponse(err.Error()), 500)
		return
	}

	eventModel, err := manager.fetchEventModelByID(schema.EventModelID)
	if err != nil {
		_, _ = w.Write([]byte("[]"))
		return
	}

	schemaMap := make(map[string]string)
	masterSchemaMap := make(map[string]string)

	err = json.Unmarshal(schema.Schema, &schemaMap)
	if err != nil {
		logID := uuid.New().String()
		pkgLogger.Errorf("logID : %s, err: %s", logID, err.Error())
		http.Error(w, response.MakeResponse(fmt.Sprintf("Internal Error: An error has been logged with logID : %s", logID)), 500)
		return
	}

	err = json.Unmarshal(eventModel.Schema, &masterSchemaMap)
	if err != nil {
		logID := uuid.New().String()
		pkgLogger.Errorf("logID : %s, err: %s", logID, err.Error())
		http.Error(w, response.MakeResponse(fmt.Sprintf("Interna Error: An error has been logged with logID : %s", logID)), 500)
		return
	}

	missingKeys := make([]string, 0)

	for k := range masterSchemaMap {
		if _, ok := schemaMap[k]; !ok {
			missingKeys = append(missingKeys, k)
		}
	}

	missingKeyJSON, err := json.Marshal(missingKeys)
	if err != nil {
		http.Error(w, response.MakeResponse("Internal Error: Failed to Marshal metadata"), 500)
		return
	}

	_, _ = w.Write(missingKeyJSON)
}

func (manager *EventSchemaManagerT) fetchEventModelsByWriteKey(writeKey string) []*EventModelT {
	var eventModelsSelectSQL string
	if writeKey == "" {
		eventModelsSelectSQL = fmt.Sprintf(`SELECT id, uuid, write_key, event_type, event_model_identifier, created_at, schema, total_count, last_seen FROM %s`, EVENT_MODELS_TABLE)
	} else {
		eventModelsSelectSQL = fmt.Sprintf(`SELECT id, uuid, write_key, event_type, event_model_identifier, created_at, schema, total_count, last_seen FROM %s WHERE write_key = '%s'`, EVENT_MODELS_TABLE, writeKey)
	}

	rows, err := manager.dbHandle.Query(eventModelsSelectSQL)
	assertError(err)
	defer func() { _ = rows.Close() }()

	eventModels := make([]*EventModelT, 0)

	for rows.Next() {
		var eventModel EventModelT
		err := rows.Scan(&eventModel.ID, &eventModel.UUID, &eventModel.WriteKey, &eventModel.EventType,
			&eventModel.EventIdentifier, &eventModel.CreatedAt, &eventModel.Schema, &eventModel.TotalCount, &eventModel.LastSeen)
		assertError(err)

		eventModels = append(eventModels, &eventModel)
	}

	return eventModels
}

func (manager *EventSchemaManagerT) fetchSchemaVersionsByEventID(eventID string) []*SchemaVersionT {
	schemaVersionsSelectSQL := fmt.Sprintf(`SELECT id, uuid, event_model_id, schema, first_seen, last_seen, total_count FROM %s WHERE event_model_id = '%s'`, SCHEMA_VERSIONS_TABLE, eventID)

	rows, err := manager.dbHandle.Query(schemaVersionsSelectSQL)
	assertError(err)
	defer func() { _ = rows.Close() }()

	schemaVersions := make([]*SchemaVersionT, 0)

	for rows.Next() {
		var schemaVersion SchemaVersionT
		err := rows.Scan(&schemaVersion.ID, &schemaVersion.UUID, &schemaVersion.EventModelID,
			&schemaVersion.Schema, &schemaVersion.FirstSeen, &schemaVersion.LastSeen, &schemaVersion.TotalCount)
		assertError(err)

		schemaVersions = append(schemaVersions, &schemaVersion)
	}

	return schemaVersions
}

func (manager *EventSchemaManagerT) fetchEventModelByID(id string) (*EventModelT, error) {
	eventModelsSelectSQL := fmt.Sprintf(`SELECT id, uuid, write_key, event_type, event_model_identifier, created_at, schema, total_count, last_seen FROM %s WHERE uuid = '%s'`, EVENT_MODELS_TABLE, id)

	rows, err := manager.dbHandle.Query(eventModelsSelectSQL)
	assertError(err)
	defer func() { _ = rows.Close() }()

	eventModels := make([]*EventModelT, 0)

	for rows.Next() {
		var eventModel EventModelT
		err := rows.Scan(&eventModel.ID, &eventModel.UUID, &eventModel.WriteKey, &eventModel.EventType,
			&eventModel.EventIdentifier, &eventModel.CreatedAt, &eventModel.Schema, &eventModel.TotalCount, &eventModel.LastSeen)
		assertError(err)

		eventModels = append(eventModels, &eventModel)
	}

	if len(eventModels) == 0 {
		err = fmt.Errorf("No eventModels found for given eventModelID : %s", id)
		return nil, err
	}

	if len(eventModels) > 1 {
		panic(fmt.Sprintf("More than one entry found for eventModelId : %s. Make sure a unique key constraint is present on uuid column", id))
	}

	return eventModels[0], nil
}

func (manager *EventSchemaManagerT) fetchSchemaVersionByID(id string) (*SchemaVersionT, error) {
	schemaVersionsSelectSQL := fmt.Sprintf(`SELECT id, uuid, event_model_id, schema, first_seen, last_seen, total_count FROM %s WHERE uuid = '%s'`, SCHEMA_VERSIONS_TABLE, id)

	rows, err := manager.dbHandle.Query(schemaVersionsSelectSQL)
	assertError(err)
	defer func() { _ = rows.Close() }()

	schemaVersions := make([]*SchemaVersionT, 0)

	for rows.Next() {
		var schemaVersion SchemaVersionT
		err := rows.Scan(&schemaVersion.ID, &schemaVersion.UUID, &schemaVersion.EventModelID, &schemaVersion.Schema, &schemaVersion.FirstSeen, &schemaVersion.LastSeen, &schemaVersion.TotalCount)
		assertError(err)

		schemaVersions = append(schemaVersions, &schemaVersion)
	}

	if len(schemaVersions) == 0 {
		err = fmt.Errorf("No SchemaVersion found for given VersionID : %s", id)
		return nil, err
	}

	if len(schemaVersions) > 1 {
		panic(fmt.Sprintf("More than one entry found for eventVersionID : %s. Make sure a unique key constraint is present on uuid column", id))
	}

	return schemaVersions[0], nil
}

func (manager *EventSchemaManagerT) fetchMetadataByEventVersionID(eventVersionID string) (metadata *MetaDataT, err error) {
	metadataSelectSQL := fmt.Sprintf(`SELECT metadata FROM %s WHERE uuid = '%s'`, SCHEMA_VERSIONS_TABLE, eventVersionID)

	rows, err := manager.dbHandle.Query(metadataSelectSQL)
	assertError(err)
	defer func() { _ = rows.Close() }()

	metadatas := make([]*MetaDataT, 0)

	for rows.Next() {
		var metadataRaw []byte
		err := rows.Scan(&metadataRaw)
		assertError(err)

		var metadata MetaDataT
		err = json.Unmarshal(metadataRaw, &metadata)
		assertError(err)
		metadatas = append(metadatas, &metadata)
	}

	if len(metadatas) > 1 {
		err = fmt.Errorf("More than one entry found for eventVersionID : %s. Make sure a unique key constraint is present on uuid column", eventVersionID)
		assertError(err)
	}

	if len(metadatas) == 0 {
		err = fmt.Errorf("No Metadata found for given VersionID : %s", eventVersionID)
		return nil, err
	}

	metadata = metadatas[0]
	return
}

func (manager *EventSchemaManagerT) fetchMetadataByEventModelID(eventModelID string) (metadata *MetaDataT, err error) {
	metadataSelectSQL := fmt.Sprintf(`SELECT metadata FROM %s WHERE uuid = '%s'`, EVENT_MODELS_TABLE, eventModelID)

	rows, err := manager.dbHandle.Query(metadataSelectSQL)
	assertError(err)
	defer func() { _ = rows.Close() }()

	metadatas := make([]*MetaDataT, 0)

	for rows.Next() {
		var metadataRaw []byte
		err := rows.Scan(&metadataRaw)
		assertError(err)

		var metadata MetaDataT
		err = json.Unmarshal(metadataRaw, &metadata)
		assertError(err)
		metadatas = append(metadatas, &metadata)
	}

	if len(metadatas) > 1 {
		err = fmt.Errorf("More than one entry found for eventVersionID : %s. Make sure a unique key constraint is present on uuid column", eventModelID)
		assertError(err)
	}

	if len(metadatas) == 0 {
		err = fmt.Errorf("No Metadata found for given VersionID : %s", eventModelID)
		return nil, err
	}

	metadata = metadatas[0]
	return
}
