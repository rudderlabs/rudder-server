package jobsdb

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/rudderlabs/rudder-go-kit/testhelper/rand"
	"github.com/rudderlabs/rudder-server/jobsdb/prebackup"
	fileuploader "github.com/rudderlabs/rudder-server/services/fileuploader"
	"github.com/stretchr/testify/require"
)

func TestMigration(t *testing.T) {
	maxDSSize := 1
	_ = startPostgres(t)

	triggerAddNewDS := make(chan time.Time)
	triggerMigrateDS := make(chan time.Time)

	jobDB := HandleT{
		TriggerAddNewDS: func() <-chan time.Time {
			return triggerAddNewDS
		},
		TriggerMigrateDS: func() <-chan time.Time {
			return triggerMigrateDS
		},
		MaxDSSize: &maxDSSize,
	}
	tablePrefix := strings.ToLower(rand.String(5))
	err := jobDB.Setup(
		ReadWrite,
		true,
		tablePrefix,
		true,
		[]prebackup.Handler{},
		fileuploader.NewDefaultProvider(),
	)
	require.NoError(t, err)
	defer jobDB.TearDown()

	jobDB.MaxDSRetentionPeriod = time.Millisecond

	customVal := rand.String(5)
	jobs := genJobs(defaultWorkspaceID, customVal, 30, 1)
	require.NoError(t, jobDB.Store(context.Background(), jobs[:10]))

	// let 8 jobs succeed, and 2 repeatedly fail
	require.NoError(
		t,
		jobDB.UpdateJobStatus(
			context.Background(),
			genJobStatuses(jobs[:9], "executing"),
			[]string{customVal},
			[]ParameterFilterT{},
		),
	)
	require.NoError(
		t,
		jobDB.UpdateJobStatus(
			context.Background(),
			genJobStatuses(jobs[:9], "succeeded"),
			[]string{customVal},
			[]ParameterFilterT{},
		),
	)

	require.NoError(
		t,
		jobDB.UpdateJobStatus(
			context.Background(),
			genJobStatuses(jobs[9:10], "executing"),
			[]string{customVal},
			[]ParameterFilterT{},
		),
		`status update failed in 1st DS`,
	)
	require.NoError(
		t,
		jobDB.UpdateJobStatus(
			context.Background(),
			genJobStatuses(jobs[9:10], "failed"),
			[]string{customVal},
			[]ParameterFilterT{},
		),
		`status update failed in 1st DS`,
	)

	require.EqualValues(t, 1, jobDB.GetMaxDSIndex())
	triggerAddNewDS <- time.Now() // trigger addNewDSLoop to run
	triggerAddNewDS <- time.Now() // Second time, waits for the first loop to finish
	require.EqualValues(t, 2, jobDB.GetMaxDSIndex())

	// add some more jobs to the new DS
	require.NoError(t, jobDB.Store(context.Background(), jobs[10:20]))

	triggerAddNewDS <- time.Now() // trigger addNewDSLoop to run
	triggerAddNewDS <- time.Now() // Second time, waits for the first loop to finish
	require.EqualValues(t, 3, jobDB.GetMaxDSIndex())

	// last DS
	// should have enough statuses for a clean up to be triggered
	// all non-terminal
	require.NoError(t, jobDB.Store(context.Background(), jobs[20:30]))
	for i := 0; i < 10; i++ {
		require.NoError(
			t,
			jobDB.UpdateJobStatus(
				context.Background(),
				genJobStatuses(jobs[20:30], "executing"),
				[]string{customVal},
				[]ParameterFilterT{},
			),
			`status update failed in 3rd DS`,
		)
		require.NoError(
			t,
			jobDB.UpdateJobStatus(
				context.Background(),
				genJobStatuses(jobs[20:30], "failed"),
				[]string{customVal},
				[]ParameterFilterT{},
			),
			`status update failed in 3rd DS`,
		)
	}
	_, err = jobDB.dbHandle.Exec(
		fmt.Sprintf(
			`ANALYZE %[1]s_jobs_1, %[1]s_jobs_2, %[1]s_jobs_3,
			%[1]s_job_status_1, %[1]s_job_status_2, %[1]s_job_status_3`,
			tablePrefix,
		),
	)
	require.NoError(t, err)
	triggerMigrateDS <- time.Now() // trigger migrateDSLoop to run
	triggerMigrateDS <- time.Now() // waits for last loop to finish

	// we should see that in the three DSs we have,
	// the first one should only have non-terminal jobs left now(with only the last status) in an jobs_1_1
	// the second one should have all jobs
	// the third DS should have all jobs with only the last status per job

	// check that the first DS has only non-terminal jobs
	dsList := jobDB.getDSList()
	require.Equal(t, `1_1`, dsList[0].Index)
	var count int64
	err = jobDB.dbHandle.QueryRow(
		fmt.Sprintf(
			`SELECT COUNT(*) FROM %[1]s_jobs_1_1 WHERE %[1]s_jobs_1_1.custom_val = $1`,
			tablePrefix,
		),
		customVal,
	).Scan(&count)
	require.NoError(t, err)
	require.EqualValues(t, 1, count)

	// second DS must be untouched by this migrationLoop
	require.Equal(t, `2`, dsList[1].Index)
	err = jobDB.dbHandle.QueryRow(
		fmt.Sprintf(
			`SELECT COUNT(*) FROM %[1]s_jobs_2 WHERE %[1]s_jobs_2.custom_val = $1`,
			tablePrefix,
		),
		customVal,
	).Scan(&count)
	require.NoError(t, err)
	require.EqualValues(t, 10, count)

	err = jobDB.dbHandle.QueryRow(
		fmt.Sprintf(
			`SELECT COUNT(*) FROM %[1]s_job_status_2`,
			tablePrefix,
		),
	).Scan(&count)
	require.NoError(t, err)
	require.EqualValues(t, 0, count)

	// third DS's status table must've been cleaned up
	require.Equal(t, `3`, dsList[2].Index)
	err = jobDB.dbHandle.QueryRow(
		fmt.Sprintf(
			`SELECT COUNT(*) FROM %[1]s_jobs_3 WHERE %[1]s_jobs_3.custom_val = $1`,
			tablePrefix,
		),
		customVal,
	).Scan(&count)
	require.NoError(t, err)
	require.EqualValues(t, 10, count)

	err = jobDB.dbHandle.QueryRow(
		fmt.Sprintf(
			`SELECT COUNT(*) FROM %[1]s_job_status_3 where job_state = 'failed';`,
			tablePrefix,
		),
	).Scan(&count)
	require.NoError(t, err)
	require.EqualValues(t, 10, count)
}
