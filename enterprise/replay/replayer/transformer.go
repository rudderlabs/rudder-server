package replayer

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/google/uuid"
)

type OriginalPayload struct {
	CreatedAt time.Time       `json:"createdAt"`
	MessageID string          `json:"messageId"`
	Payload   json.RawMessage `json:"payload"`
	UserID    string          `json:"userId"`
}
type DesiredPayload struct {
	CreatedAt    time.Time `json:"created_at"`
	CustomVal    string    `json:"custom_val"`
	EventCount   int       `json:"event_count"`
	EventPayload Payload   `json:"event_payload"`
	ExpireAt     string    `json:"expire_at"`
	JobID        int       `json:"job_id"`
	Parameters   Params    `json:"parameters"`
	UserID       string    `json:"user_id"`
	UUID         string    `json:"uuid"`
	WorkspaceID  string    `json:"workspace_id"`
}
type Payload struct {
	Batch      json.RawMessage `json:"batch"`
	ReceivedAt string          `json:"receivedAt"`
	RequestIP  string          `json:"requestIP"`
	WriteKey   string          `json:"writeKey"`
}
type Params struct {
	SourceID        string `json:"source_id"`
	SourceJobRunID  string `json:"source_job_run_id"`
	SourceTaskRunID string `json:"source_task_run_id"`
}

func transformArchivalToBackup(input []byte, path string) ([]byte, error) {
	var originalPayload OriginalPayload
	var sourceId string
	err := json.Unmarshal(input, &originalPayload)
	if err != nil {
		return nil, err
	}
	if len(strings.Split(path, "/")) == 7 {
		sourceId = strings.Split(path, "/")[1]
	} else {
		sourceId = strings.Split(path, "/")[0]
	}
	desiredPayload := DesiredPayload{
		CreatedAt: originalPayload.CreatedAt,
		CustomVal: "GW",
		EventPayload: Payload{
			Batch: originalPayload.Payload,
		},
		UUID: uuid.Must(uuid.NewRandom()).String(),
		Parameters: Params{
			SourceID: sourceId,
		},
		EventCount: 1,
		UserID:     originalPayload.UserID,
	}
	desiredJSON, err := json.Marshal(desiredPayload)
	if err != nil {
		return nil, err
	}
	return desiredJSON, nil
}
