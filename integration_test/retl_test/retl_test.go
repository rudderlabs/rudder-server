package retltest

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/testhelper/rand"
	"github.com/rudderlabs/rudder-server/services/rsources"
)

type batch struct {
	Batch []record `json:"batch"`
}

type record struct {
	Context recordContext `json:"context"`

	Type      string    `json:"type"`
	MessageID string    `json:"messageId"`
	UserID    string    `json:"userId"`
	SentAt    time.Time `json:"sentAt"`
	Timestamp time.Time `json:"timestamp"`
}

type rudderSource struct {
	JobID     string `json:"job_id"`
	JobRunID  string `json:"job_run_id"`
	TaskRunID string `json:"task_run_id"`
}

type recordContext struct {
	Sources rudderSource `json:"sources"`
}

type rETLSource struct {
	id   string
	once sync.Once
}

func (s *rETLSource) ID() string {
	s.once.Do(func() {
		s.id = rand.String(27)
	})
	return s.id
}

func TestRETL(t *testing.T) {

	rETLSource := &rETLSource{}
	webhook_1 := &Webhook{name: "webhook_1"}
	webhook_2 := &Webhook{name: "webhook_2"}

	s := &SUT{
		Sources: []srcWithDst{
			Connect(rETLSource, webhook_1, webhook_2),
		},
	}
	s.Start(t)
	defer s.Shutdown(t)

	jobID := rand.String(27)
	jobRunID := rand.String(27)

	taskRunID := rand.String(27)
	numOfRecords := 10

	s.SendRETL(t, rETLSource.ID(), webhook_1.ID(), ManyRecords(rudderSource{
		JobID:     jobID,
		JobRunID:  jobRunID,
		TaskRunID: taskRunID,
	}, "identify", numOfRecords))

	require.Eventually(t, func() bool {
		return !pendingTask(s.JobStatus(t, rETLSource.ID(), jobRunID, taskRunID))
	}, 10*time.Second, 2*time.Second, "using job-status to check if the task is completed")

	require.Equal(t, numOfRecords, webhook_1.Count())
	require.Zero(t, webhook_2.Count(), "webhook_2 should not receive any events")

	t.Run("second task is sending to webhook_2", func(t *testing.T) {

		t.Skip("TODO: fix this test")

		taskRunID_2 := rand.String(27)
		jobRunID_2 := rand.String(27)
		numOfRecords_2 := 8

		s.SendRETL(t, rETLSource.ID(), webhook_2.ID(), ManyRecords(rudderSource{
			JobID:     jobID,
			JobRunID:  jobRunID_2,
			TaskRunID: taskRunID_2,
		}, "identify", numOfRecords_2))

		require.Eventually(t, func() bool {
			t.Logf("webhook count %d", webhook_2.Count())
			return !pendingTask(s.JobStatus(t, rETLSource.ID(), jobRunID_2, taskRunID_2))

		}, 10*time.Second, 2*time.Second, "using job-status to check if the task is completed")

		require.Equal(t, numOfRecords, webhook_1.Count())
		require.Equal(t, numOfRecords_2, webhook_2.Count())
	})

	// TODO: add test for failed-records and delete failed-records
}

// pendingTask helper function to check if the task is completed.
func pendingTask(status rsources.JobStatus, found bool) bool {
	// if the task is not found, it is considered pending
	if !found {
		return true
	}

	for _, task := range status.TasksStatus {
		for _, source := range task.SourcesStatus {
			if !source.Completed {
				return true
			}
		}
	}

	return false
}

// ManyRecords helper function to generate a batch of records for rETL endpoint.
func ManyRecords(sources rudderSource, eventType string, num int) batch {
	rr := make([]record, num)
	for i := 0; i < num; i++ {
		rr[i] = record{
			Context: recordContext{
				Sources: sources,
			},
			Type:      eventType,
			MessageID: rand.String(27),
			UserID:    rand.String(27),
			SentAt:    time.Now(),
			Timestamp: time.Now(),
		}
	}

	return batch{Batch: rr}
}
