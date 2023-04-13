package noopforwarder

import (
	"context"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/google/uuid"
	"github.com/ory/dockertest/v3"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/stretchr/testify/require"
)

func Test_NOOPForwarder(t *testing.T) {
	g, ctx := errgroup.WithContext(context.Background())
	conf := config.New()
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	jobsdb.Init()
	jobsdb.Init2()
	postgres, err := resource.SetupPostgres(pool, t)
	require.NoError(t, err)
	t.Setenv("JOBS_DB_PORT", postgres.Port)
	t.Setenv("JOBS_DB_USER", postgres.User)
	t.Setenv("JOBS_DB_DB_NAME", postgres.Database)
	t.Setenv("JOBS_DB_PASSWORD", postgres.Password)
	schemasDB := jobsdb.NewForReadWrite(
		"test_event_schema",
	)
	err = schemasDB.Start()
	require.NoError(t, err)
	defer schemasDB.TearDown()
	nf, err := New(ctx, g, schemasDB, conf, logger.NOP)
	require.NoError(t, err)
	require.NotNil(t, nf)

	nf.Start()
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
	err = schemasDB.Store(ctx, jobs)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		jobs, err := schemasDB.GetProcessed(ctx, jobsdb.GetQueryParamsT{
			StateFilters: []string{jobsdb.Aborted.State},
			JobsLimit:    10,
		})
		require.NoError(t, err)
		return len(jobs.Jobs) == 10
	}, 30*time.Second, 5*time.Second)

	defer nf.Stop()
}
