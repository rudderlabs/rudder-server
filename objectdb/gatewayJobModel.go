//go:generate go run github.com/objectbox/objectbox-go/cmd/objectbox-gogen
package objectdb

import (
	"encoding/json"
	"time"
)

type GatewayJob struct {
	// Fundamental Job Attributes
	JobID       uint64 `objectbox:"id"`
	UserID      string
	CustomVal   string `objectbox:"index"`
	WorkspaceID string `objectbox:"index"`

	// job statistics
	ReceivedAt   time.Time `objectbox:"date"`
	CreatedAt    time.Time `objectbox:"date"`
	ExpireAt     time.Time `objectbox:"date"`
	EventCount   int
	EventPayload json.RawMessage
	PayloadSize  int64

	// job state attributes
	JobState         string `objectbox:"index"`
	Stage            string `objectbox:"index"`
	StatusCode       int
	AttemptNum       int
	FirstAttemptedAt time.Time `objectbox:"date"`
	ExecTime         time.Time `objectbox:"date"`
	RetryTime        time.Time `objectbox:"date"`
	ErrorResponse    json.RawMessage

	// connection attributes
	WriteKey                string
	IPAddress               string
	SourceID                string
	DestinationID           string
	SourceBatchID           string
	SourceTaskID            string
	SourceTaskRunID         string
	SourceJobID             string
	SourceJobRunID          string
	SourceDefinitionID      string
	DestinationDefinitionID string
	SourceCategory          string

	// generic job data
	EventName   string
	EventType   string
	MessageID   string
	TransformAt string
	RecordID    json.RawMessage
}

// func (box *Box) PutJobsInTxn(jobs []*GatewayJob) error {
// 	err := box.ObjectBox.RunInWriteTx(func() error {
// 		for _, job := range jobs {
// 			_, err := box.gatewayJobBox.Put(job)
// 			if err != nil {
// 				return err
// 			}
// 		}
// 		return nil
// 	})
// 	return err
// }
