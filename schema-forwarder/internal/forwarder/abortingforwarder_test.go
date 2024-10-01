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
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"
	"github.com/rudderlabs/rudder-server/jobsdb"
)

func Test_AbortingForwarder(t *testing.T) {
	conf := config.New()
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	postgres, err := postgres.Setup(pool, t)
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
	forwarder := NewAbortingForwarder(func(error) {}, schemasDB, conf, logger.NOP, stats.Default)
	require.NotNil(t, forwarder)

	require.NoError(t, forwarder.Start())
	defer forwarder.Stop()

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

	require.Eventually(t, func() bool {
		jobs, err := schemasDB.GetAborted(context.Background(), jobsdb.GetQueryParams{
			JobsLimit: 10,
		})
		require.NoError(t, err)
		return len(jobs.Jobs) == 10
	}, 30*time.Second, 5*time.Second)
}
