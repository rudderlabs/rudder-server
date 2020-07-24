/*
 * Handling HTTP requests to expose the schemas
 *
 */
package protocols

import (
	"encoding/json"
	"fmt"
	"net/http"
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

func (manager *ProtocolManagerT) GetEventTypes(w http.ResponseWriter, r *http.Request) {
	err := handleBasicAuth(r)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}

	writeKeys, ok := r.URL.Query()["writeKey"]
	writeKey := ""
	if ok && writeKeys[0] != "" {
		writeKey = writeKeys[0]
	}

	eventTypes := manager.fetchEventTypesByWriteKey(writeKey)

	eventTypesJSON, err := json.Marshal(eventTypes)
	if err != nil {
		http.Error(w, "Internal Error: Failed to Marshal event types", 500)
		return
	}

	w.Write(eventTypesJSON)
}

func (manager *ProtocolManagerT) GetEventVersions(w http.ResponseWriter, r *http.Request) {
	err := handleBasicAuth(r)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}

	eventIDs, ok := r.URL.Query()["eventID"]
	if !ok {
		http.Error(w, "Mandatory field: eventID missing", 400)
		return
	}
	eventID := eventIDs[0]

	schemaVersions := manager.fetchSchemaVersionsByEventID(eventID)
	schemaVersionsJSON, err := json.Marshal(schemaVersions)
	if err != nil {
		http.Error(w, "Internal Error: Failed to Marshal event types", 500)
		return
	}

	w.Write(schemaVersionsJSON)
}

func (manager *ProtocolManagerT) fetchEventTypesByWriteKey(writeKey string) []*EventTypeT {
	var eventTypesSelectSQL string
	if writeKey == "" {
		eventTypesSelectSQL = fmt.Sprintf(`SELECT * FROM %s`, EVENT_TYPES_TABLE)
	} else {
		eventTypesSelectSQL = fmt.Sprintf(`SELECT * FROM %s WHERE write_key = '%s'`, EVENT_TYPES_TABLE, writeKey)
	}

	rows, err := manager.dbHandle.Query(eventTypesSelectSQL)
	assertError(err)
	defer rows.Close()

	eventTypes := make([]*EventTypeT, 0)

	for rows.Next() {
		var eventType EventTypeT
		err := rows.Scan(&eventType.ID, &eventType.UUID, &eventType.WriteKey, &eventType.EvType,
			&eventType.EventIdentifier, &eventType.CreatedAt)
		assertError(err)

		eventTypes = append(eventTypes, &eventType)
	}

	return eventTypes
}

func (manager *ProtocolManagerT) fetchSchemaVersionsByEventID(eventID string) []*SchemaVersionT {
	schemaVersionsSelectSQL := fmt.Sprintf(`SELECT * FROM %s WHERE event_id = '%s'`, SCHEMA_VERSIONS_TABLE, eventID)

	rows, err := manager.dbHandle.Query(schemaVersionsSelectSQL)
	assertError(err)
	defer rows.Close()

	schemaVersions := make([]*SchemaVersionT, 0)

	for rows.Next() {
		var schemaVersion SchemaVersionT
		err := rows.Scan(&schemaVersion.ID, &schemaVersion.UUID, &schemaVersion.EventID, &schemaVersion.SchemaHash,
			&schemaVersion.Schema, &schemaVersion.Metadata, &schemaVersion.FirstSeen, &schemaVersion.LastSeen)
		assertError(err)

		schemaVersions = append(schemaVersions, &schemaVersion)
	}

	return schemaVersions
}
