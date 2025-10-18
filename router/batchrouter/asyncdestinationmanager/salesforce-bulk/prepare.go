package salesforcebulk

import (
	"fmt"

	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
)

func prepareAsyncJob(eventPayload []byte, jobID int64, defaultOperation string) (*common.AsyncJob, error) {
	message, err := extractMessage(eventPayload)
	if err != nil {
		return nil, err
	}

	clonedMessage := cloneMessage(message)
	metadata := map[string]interface{}{
		"job_id": jobID,
	}

	normalizedExternalIDs := collectNormalizedExternalIDs(message)
	if len(normalizedExternalIDs) > 0 {
		metadata["externalId"] = normalizedExternalIDs
	}

	operation := determineOperation(normalizedExternalIDs, defaultOperation)
	clonedMessage["rudderOperation"] = operation

	if field, value, ok := identifierColumn(normalizedExternalIDs); ok {
		clonedMessage[field] = value
	}

	return &common.AsyncJob{
		Message:  clonedMessage,
		Metadata: metadata,
	}, nil
}

func extractMessage(eventPayload []byte) (map[string]interface{}, error) {
	body := gjson.GetBytes(eventPayload, "body.JSON")
	if !body.Exists() || len(body.Raw) == 0 {
		return map[string]interface{}{}, nil
	}

	var message map[string]interface{}
	if err := jsonrs.Unmarshal([]byte(body.Raw), &message); err != nil {
		return nil, err
	}

	if message == nil {
		message = map[string]interface{}{}
	}

	return message, nil
}

func cloneMessage(message map[string]interface{}) map[string]interface{} {
	cloned := make(map[string]interface{}, len(message))
	for key, value := range message {
		cloned[key] = value
	}
	return cloned
}

func collectNormalizedExternalIDs(message map[string]interface{}) []map[string]string {
	var normalized []map[string]string

	if contextRaw, ok := message["context"].(map[string]interface{}); ok {
		normalized = append(normalized, normalizeExternalIDArray(contextRaw["externalId"])...)
	}

	normalized = append(normalized, normalizeExternalIDArray(message["externalId"])...)

	return normalized
}

func normalizeExternalIDArray(raw interface{}) []map[string]string {
	array, ok := raw.([]interface{})
	if !ok || len(array) == 0 {
		return nil
	}

	result := make([]map[string]string, 0, len(array))
	for _, entry := range array {
		if normalized, ok := normalizeExternalIDEntry(entry); ok {
			result = append(result, normalized)
		}
	}

	return result
}

func normalizeExternalIDEntry(raw interface{}) (map[string]string, bool) {
	entry, ok := raw.(map[string]interface{})
	if !ok {
		return nil, false
	}

	normalized := map[string]string{
		"type":           "",
		"id":             "",
		"identifierType": "",
	}

	if typeVal, ok := entry["type"]; ok {
		normalized["type"] = fmt.Sprint(typeVal)
	}

	if idVal, ok := entry["id"]; ok {
		normalized["id"] = fmt.Sprint(idVal)
	}

	if identifierVal, ok := entry["identifierType"]; ok {
		normalized["identifierType"] = fmt.Sprint(identifierVal)
	}

	if normalized["identifierType"] == "" && normalized["id"] != "" {
		normalized["identifierType"] = "Id"
	}

	return normalized, true
}

func determineOperation(externalIDs []map[string]string, defaultOperation string) string {
	if defaultOperation == "" {
		defaultOperation = "insert"
	}

	if len(externalIDs) == 0 {
		return defaultOperation
	}

	for _, externalID := range externalIDs {
		if externalID["id"] != "" {
			return "upsert"
		}
	}

	for _, externalID := range externalIDs {
		if externalID["identifierType"] != "" {
			return "upsert"
		}
	}

	return defaultOperation
}

func identifierColumn(externalIDs []map[string]string) (string, string, bool) {
	for _, externalID := range externalIDs {
		if externalID["id"] == "" {
			continue
		}

		field := externalID["identifierType"]
		if field == "" {
			field = "Id"
		}

		return field, externalID["id"], true
	}

	return "", "", false
}
