package forwarder

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	pgdocker "github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"
	"github.com/rudderlabs/rudder-server/jobsdb"
)

func Test_BaseForwarder(t *testing.T) {
	bf := BaseForwarder{}
	conf := config.New()
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	postgres, err := pgdocker.Setup(pool, t)
	require.NoError(t, err)
	t.Setenv("JOBS_DB_HOST", postgres.Host)
	t.Setenv("JOBS_DB_PORT", postgres.Port)
	t.Setenv("JOBS_DB_USER", postgres.User)
	t.Setenv("JOBS_DB_DB_NAME", postgres.Database)
	t.Setenv("JOBS_DB_PASSWORD", postgres.Password)
	schemasDB := jobsdb.NewForReadWrite(
		"test_event_schema",
		jobsdb.WithStats(stats.NOP),
	)
	err = schemasDB.Start()
	require.NoError(t, err)
	defer schemasDB.TearDown()
	bf.LoadMetaData(func(error) {}, schemasDB, logger.NOP, conf, stats.Default)

	t.Run("Test GetSleepTime", func(t *testing.T) {
		require.Equal(t, 10*time.Second, bf.GetSleepTime(false))
		require.Equal(t, 0*time.Second, bf.GetSleepTime(true))
	})

	t.Run("Test GetJobs", func(t *testing.T) {
		generateJobs := func(numOfJob int) []*jobsdb.JobT {
			customVal := "MOCKDS"
			js := make([]*jobsdb.JobT, numOfJob)
			for i := 0; i < numOfJob; i++ {
				js[i] = &jobsdb.JobT{
					Parameters:   []byte(`{"batch_id":1,"source_id":"sourceID","source_job_run_id":""}`),
					EventPayload: []byte(`{"testKey":"testValue"}`),
					UserID:       "a-292e-4e79-9880-f8009e0ae4a3",
					UUID:         uuid.New(),
					CustomVal:    customVal,
					EventCount:   1,
					WorkspaceId:  "test_workspace",
				}
			}
			return js
		}
		err := schemasDB.Store(context.Background(), generateJobs(10))
		require.NoError(t, err)
		getJobs, limitReached, err := bf.GetJobs(context.Background())
		require.NoError(t, err)
		require.Equal(t, 10, len(getJobs))
		require.Equal(t, false, limitReached)
	})
}

func TestBaseForwarder_MarkJobStautses(t *testing.T) {
	bf := BaseForwarder{}
	conf := config.New()
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	postgres, err := pgdocker.Setup(pool, t)
	require.NoError(t, err)
	t.Setenv("JOBS_DB_HOST", postgres.Host)
	t.Setenv("JOBS_DB_PORT", postgres.Port)
	t.Setenv("JOBS_DB_USER", postgres.User)
	t.Setenv("JOBS_DB_DB_NAME", postgres.Database)
	t.Setenv("JOBS_DB_PASSWORD", postgres.Password)
	schemasDB := jobsdb.NewForReadWrite(
		"test_event_schema",
		jobsdb.WithStats(stats.NOP),
	)
	err = schemasDB.Start()
	require.NoError(t, err)
	defer schemasDB.TearDown()
	bf.LoadMetaData(func(error) {}, schemasDB, logger.NOP, conf, stats.Default)

	generateJobs := func(numOfJob int) []*jobsdb.JobT {
		customVal := "MOCKDS"
		js := make([]*jobsdb.JobT, numOfJob)
		for i := 0; i < numOfJob; i++ {
			js[i] = &jobsdb.JobT{
				Parameters:   []byte(`{"batch_id":1,"source_id":"sourceID","source_job_run_id":""}`),
				EventPayload: []byte(`{"testKey":"testValue"}`),
				UserID:       "a-292e-4e79-9880-f8009e0ae4a3",
				UUID:         uuid.New(),
				CustomVal:    customVal,
				EventCount:   1,
				WorkspaceId:  "test_workspace",
			}
		}
		return js
	}
	jobs := generateJobs(10)
	err = schemasDB.Store(context.Background(), jobs)
	require.NoError(t, err)
	jobs, limitReached, err := bf.GetJobs(context.Background())
	require.NoError(t, err)
	require.Equal(t, 10, len(jobs))
	require.Equal(t, false, limitReached)
	var statuses []*jobsdb.JobStatusT
	for _, job := range jobs {
		statuses = append(statuses, &jobsdb.JobStatusT{
			JobID:         job.JobID,
			JobState:      jobsdb.Succeeded.State,
			AttemptNum:    1,
			ExecTime:      time.Now(),
			RetryTime:     time.Now(),
			ErrorCode:     "200",
			ErrorResponse: []byte(`{"success":true}`),
			Parameters:    []byte(`{}`),
			JobParameters: job.Parameters,
		})
	}
	err = bf.MarkJobStatuses(context.Background(), statuses)
	require.NoError(t, err)
	jobs, limitReached, err = bf.GetJobs(context.Background())
	require.NoError(t, err)
	require.Equal(t, 0, len(jobs))
	require.Equal(t, false, limitReached)
}
