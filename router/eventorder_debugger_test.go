package router

import (
	"context"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/router/internal/eventorder"
	migrator "github.com/rudderlabs/rudder-server/services/sql-migrator"
)

func TestEventOrderDebugInfo(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pgContainer, err := postgres.Setup(pool, t, postgres.WithShmSize(256*bytesize.MB))
	require.NoError(t, err)

	m := &migrator.Migrator{
		Handle:                     pgContainer.DB,
		MigrationsTable:            "node_migrations",
		ShouldForceSetLowerVersion: config.GetBool("SQLMigrator.forceSetLowerVersion", true),
	}
	require.NoError(t, m.Migrate("node"))

	jdb := jobsdb.NewForReadWrite("rt", jobsdb.WithDBHandle(pgContainer.DB), jobsdb.WithStats(stats.NOP))
	require.NoError(t, jdb.Start())
	defer jdb.Stop()

	err = jdb.Store(context.Background(), []*jobsdb.JobT{{
		UserID:       "user1",
		WorkspaceId:  "workspace1",
		Parameters:   []byte(`{"destination_id": "destination1"}`),
		EventCount:   1,
		EventPayload: []byte(`{"type": "track", "event": "test_event", "properties": {"key": "value"}}`),
		CustomVal:    "dummy",
	}})
	require.NoError(t, err)

	jobs, err := jdb.GetJobs(context.Background(), []string{jobsdb.Unprocessed.State}, jobsdb.GetQueryParams{JobsLimit: 1})
	require.NoError(t, err)
	require.Len(t, jobs.Jobs, 1)
	job := jobs.Jobs[0]
	require.NoError(t, jdb.UpdateJobStatus(context.Background(), []*jobsdb.JobStatusT{{
		WorkspaceId:   "workspace1",
		JobID:         job.JobID,
		JobState:      jobsdb.Executing.State,
		ErrorResponse: []byte("{}"),
		Parameters:    []byte(`{}`),
		JobParameters: []byte(`{"destination_id", "destination1"}`),
	}}, nil, nil))
	require.NoError(t, jdb.UpdateJobStatus(context.Background(), []*jobsdb.JobStatusT{{
		WorkspaceId:   "workspace1",
		JobID:         job.JobID,
		JobState:      jobsdb.Succeeded.State,
		ErrorResponse: []byte("{}"),
		Parameters:    []byte(`{}`),
		JobParameters: []byte(`{"destination_id", "destination1"}`),
	}}, nil, nil))

	rt := &Handle{
		jobsDB: jdb,
	}
	refTime := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)

	_, err = pgContainer.DB.Exec("UPDATE rt_jobs_1 SET created_at = $1", refTime)
	require.NoError(t, err)

	debugInfo := rt.eventOrderDebugInfo(eventorder.BarrierKey{UserID: "user1", DestinationID: "destination1"})
	require.Equal(t,
		` |    t_name| job_id|                    created_at| status_id| job_state| attempt|                     exec_time| error_code| parameters| error_response|
 |       ---|    ---|                           ---|       ---|       ---|     ---|                           ---|        ---|        ---|            ---|
 | rt_jobs_1|      1| 2023-01-01 00:00:00 +0000 UTC|         1| executing|       0| 0001-01-01 00:00:00 +0000 UTC|           |         {}|             {}|
 | rt_jobs_1|      1| 2023-01-01 00:00:00 +0000 UTC|         2| succeeded|       0| 0001-01-01 00:00:00 +0000 UTC|           |         {}|             {}|
`, debugInfo)
}
