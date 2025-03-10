package jobsdb

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestJobsdbClearAllOnStartup(t *testing.T) {
	pg := startPostgres(t)

	db := NewForWrite(
		"test",
		WithDBHandle(pg.DB),
		WithSkipMaintenanceErr(true),
	)
	require.NoError(t, db.Start())

	err := db.Store(context.Background(), []*JobT{
		{
			UUID:         uuid.New(),
			UserID:       "user-1",
			WorkspaceId:  "workspace-1",
			CustomVal:    "custom-val",
			Parameters:   []byte("{}"),
			EventPayload: []byte("{}"),
			CreatedAt:    time.Now().UTC(),
			ExpireAt:     time.Now().UTC().Add(time.Hour),
			EventCount:   1,
		},
	})
	require.NoError(t, err, "it should be able to store a job")
	jobResult, err := db.GetUnprocessed(context.Background(), GetQueryParams{JobsLimit: 1})
	require.NoError(t, err)
	require.Len(t, jobResult.Jobs, 1)
	db.Stop()

	db = NewForWrite(
		"test",
		WithClearDB(true),
		WithDBHandle(pg.DB),
		WithSkipMaintenanceErr(true),
	)
	require.NoError(t, db.Start())

	jobResult, err = db.GetUnprocessed(context.Background(), GetQueryParams{JobsLimit: 1})
	require.NoError(t, err)
	require.Len(t, jobResult.Jobs, 0, "it should not have any jobs since the db was created with clearDB=true option")
	db.Stop()
}
