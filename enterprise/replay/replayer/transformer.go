package replayer

import (
	"encoding/json"
	"strings"

	"github.com/google/uuid"
)

type originalPayload struct {
	createdAt string          `json:"createdAt"`
	messageID string          `json:"messageId"`
	payload   json.RawMessage `json:"payload"`
	userID    string          `json:"userId"`
}
type desiredPayload struct {
	createdAt    string  `json:"created_at"`
	customVal    string  `json:"custom_val"`
	eventCount   int     `json:"event_count"`
	eventPayload payload `json:"event_payload"`
	expireAt     string  `json:"expire_at"`
	jobID        int     `json:"job_id"`
	parameters   params  `json:"parameters"`
	userID       string  `json:"user_id"`
	uuid         string  `json:"uuid"`
	workspaceID  string  `json:"workspace_id"`
}
type payload struct {
	batch      json.RawMessage `json:"batch"`
	receivedAt string          `json:"receivedAt"`
	requestIP  string          `json:"requestIP"`
	writeKey   string          `json:"writeKey"`
}
type params struct {
	sourceID        string `json:"source_id"`
	sourceJobRunID  string `json:"source_job_run_id"`
	sourceTaskRunID string `json:"source_task_run_id"`
}

func transformArchivalToBackup(input []byte, path string) ([]byte, error) {
	var originalPayload originalPayload
	err := json.Unmarshal(input, &originalPayload)
	if err != nil {
		return nil, err
	}
	sourceId := strings.Split(path, "/")[0]
	desiredPayload := desiredPayload{
		createdAt: originalPayload.createdAt,
		customVal: "GW",
		eventPayload: payload{
			batch: originalPayload.payload,
		},
		uuid: uuid.Must(uuid.NewRandom()).String(),
		parameters: params{
			sourceID: sourceId,
		},
		eventCount: 1,
		userID:     originalPayload.userID,
	}
	desiredJSON, err := json.Marshal(desiredPayload)
	if err != nil {
		return nil, err
	}
	return desiredJSON, nil
}
