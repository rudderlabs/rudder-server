package retltest

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/testhelper/rand"
	"github.com/rudderlabs/rudder-server/services/rsources"
)

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
	webhook_1 := &webhook{name: "webhook_1"}
	webhook_2 := &webhook{name: "webhook_2"}

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

	s.SendRETL(t, rETLSource.ID(), webhook_1.ID(), manyRecords(rudderSource{
		JobID:     jobID,
		JobRunID:  jobRunID,
		TaskRunID: taskRunID,
	}, "identify", numOfRecords))

	require.Eventually(t, func() bool {
		return !pendingTask(s.JobStatus(t, rETLSource.ID(), jobRunID, taskRunID))
	}, 15*time.Second, 250*time.Millisecond, "using job-status to check if the task is completed")

	require.Equal(t, numOfRecords, webhook_1.Count())
	require.Zero(t, webhook_2.Count(), "webhook_2 should not receive any events")

	t.Run("second task is sending to webhook_2", func(t *testing.T) {
		taskRunID2 := rand.String(27)
		jobRunID2 := rand.String(27)
		numOfRecords2 := 8

		s.SendRETL(t, rETLSource.ID(), webhook_2.ID(), manyRecords(rudderSource{
			JobID:     jobID,
			JobRunID:  jobRunID2,
			TaskRunID: taskRunID2,
		}, "identify", numOfRecords2))

		require.Eventually(t, func() bool {
			t.Logf("webhook count %d", webhook_2.Count())
			return !pendingTask(s.JobStatus(t, rETLSource.ID(), jobRunID2, taskRunID2))
		}, 10*time.Second, 2*time.Second, "using job-status to check if the task is completed")

		require.Equal(t, numOfRecords, webhook_1.Count())
		require.Equal(t, numOfRecords2, webhook_2.Count())
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

// manyRecords helper function to generate a batch of records for rETL endpoint.
func manyRecords(sources rudderSource, eventType string, num int) batch {
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
