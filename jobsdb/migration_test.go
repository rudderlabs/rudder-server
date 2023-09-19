package jobsdb

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/testhelper/rand"
	"github.com/rudderlabs/rudder-server/jobsdb/prebackup"
	fileuploader "github.com/rudderlabs/rudder-server/services/fileuploader"
)

func TestMigration(t *testing.T) {
	t.Run("main", func(t *testing.T) {
		config.Reset()
		c := config.New()
		c.Set("JobsDB.maxDSSize", 1)

		_ = startPostgres(t)

		triggerAddNewDS := make(chan time.Time)
		triggerMigrateDS := make(chan time.Time)

		jobDB := Handle{
			TriggerAddNewDS: func() <-chan time.Time {
				return triggerAddNewDS
			},
			TriggerMigrateDS: func() <-chan time.Time {
				return triggerMigrateDS
			},
			config: c,
		}
		tablePrefix := strings.ToLower(rand.String(5))
		err := jobDB.Setup(
			ReadWrite,
			true,
			tablePrefix,
			[]prebackup.Handler{},
			fileuploader.NewDefaultProvider(),
		)
		require.NoError(t, err)
		defer jobDB.TearDown()

		c.Set("JobsDB."+tablePrefix+"."+"maxDSRetention", "1ms")

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
	})

	t.Run("cleanup status tables", func(t *testing.T) {
		_ = startPostgres(t)

		triggerAddNewDS := make(chan time.Time)
		triggerMigrateDS := make(chan time.Time)
		config.Reset()
		c := config.New()
		c.Set("JobsDB.maxDSSize", 1)

		jobDB := Handle{
			TriggerAddNewDS: func() <-chan time.Time {
				return triggerAddNewDS
			},
			TriggerMigrateDS: func() <-chan time.Time {
				return triggerMigrateDS
			},
			config: c,
		}
		tablePrefix := strings.ToLower(rand.String(5))
		require.NoError(t, jobDB.Setup(
			ReadWrite,
			true,
			tablePrefix,
			[]prebackup.Handler{},
			fileuploader.NewDefaultProvider(),
		))
		defer jobDB.TearDown()

		c.Set("JobsDB."+tablePrefix+"."+"maxDSRetention", "1ms")

		// 3 datasets with 10 jobs each, 1 dataset with 0 jobs
		for i := 0; i < 3; i++ {
			require.NoError(t, jobDB.Store(context.Background(), genJobs(defaultWorkspaceID, "test", 10, 1)))
			triggerAddNewDS <- time.Now() // trigger addNewDSLoop to run
			triggerAddNewDS <- time.Now() // waits for last loop to finish
			require.Equal(t, i+2, len(jobDB.getDSList()))
		}

		// 1st ds 5 statuses each
		var jobs []*JobT
		for i := 0; i < 10; i++ {
			jobs = append(jobs, &JobT{JobID: int64(i + 1)})
		}
		for i := 0; i < 5; i++ {
			require.NoError(t, jobDB.UpdateJobStatus(context.Background(), genJobStatuses(jobs, "failed"), []string{"test"}, []ParameterFilterT{}))
		}
		var count int
		require.NoError(t, jobDB.dbHandle.QueryRow(fmt.Sprintf(`SELECT COUNT(*) FROM %[1]s_job_status_1 `, tablePrefix)).Scan(&count))
		require.EqualValues(t, 5*10, count)

		// 2nd ds 10 statuses each
		jobs = nil
		for i := 0; i < 10; i++ {
			jobs = append(jobs, &JobT{JobID: int64(i + 11)})
		}
		for i := 0; i < 10; i++ {
			require.NoError(t, jobDB.UpdateJobStatus(context.Background(), genJobStatuses(jobs, "failed"), []string{"test"}, []ParameterFilterT{}))
		}
		require.NoError(t, jobDB.dbHandle.QueryRow(fmt.Sprintf(`SELECT COUNT(*) FROM %[1]s_job_status_2 `, tablePrefix)).Scan(&count))
		require.EqualValues(t, 10*10, count)

		getTableSizes := func(dsList []dataSetT) map[string]int64 {
			for _, ds := range dsList { // first analyze all tables so that we get correct table sizes
				_, err := jobDB.dbHandle.Exec(fmt.Sprintf(`ANALYZE %q`, ds.JobStatusTable))
				require.NoError(t, err)
				_, err = jobDB.dbHandle.Exec(fmt.Sprintf(`ANALYZE %q`, ds.JobTable))
				require.NoError(t, err)
			}
			tableSizes := make(map[string]int64)
			rows, err := jobDB.dbHandle.QueryContext(context.Background(),
				`SELECT relname, pg_table_size(oid) AS size
				FROM pg_class
				where relname = ANY(
					SELECT tablename
						FROM pg_catalog.pg_tables
						WHERE schemaname NOT IN ('pg_catalog','information_schema')
						AND tablename like $1
				) order by relname;`,
				tablePrefix+"_job_status%",
			)
			require.NoError(t, err)
			defer func() { _ = rows.Close() }()
			for rows.Next() {
				var (
					tableName string
					tableSize int64
				)
				require.NoError(t, rows.Scan(&tableName, &tableSize))
				tableSizes[tableName] = tableSize
			}
			require.NoError(t, rows.Err())
			return tableSizes
		}
		// capture table sizes
		originalTableSizes := getTableSizes(jobDB.getDSList())

		c.Set("JobsDB.jobStatusMigrateThreshold", 1)
		c.Set("JobsDB.vacuumAnalyzeStatusTableThreshold", 4)
		c.Set("JobsDB.vacuumFullStatusTableThreshold", fmt.Sprint(
			originalTableSizes[fmt.Sprintf("%s_job_status_2", tablePrefix)]-1,
		))

		// run cleanup status tables
		require.NoError(t, jobDB.cleanupStatusTables(context.Background(), jobDB.getDSList()))

		newTableSizes := getTableSizes(jobDB.getDSList())

		// 1st DS should have 10 jobs with 1 status each
		require.NoError(t, jobDB.dbHandle.QueryRow(fmt.Sprintf(`SELECT COUNT(*) FROM %[1]s_job_status_1`, tablePrefix)).Scan(&count))
		require.EqualValues(t, 10, count)
		require.GreaterOrEqual(t, newTableSizes[fmt.Sprintf("%s_job_status_1", tablePrefix)], originalTableSizes[fmt.Sprintf("%s_job_status_1", tablePrefix)])

		// 2nd DS should have 10 jobs with 1 status each
		require.NoError(t, jobDB.dbHandle.QueryRow(fmt.Sprintf(`SELECT COUNT(*) FROM %[1]s_job_status_2`, tablePrefix)).Scan(&count))
		require.EqualValues(t, 10, count)
		require.Less(t, newTableSizes[fmt.Sprintf("%s_job_status_2", tablePrefix)], originalTableSizes[fmt.Sprintf("%s_job_status_2", tablePrefix)])

		// after adding some statuses to the 1st DS its table size shouldn't increase
		for i := 0; i < 10; i++ {
			jobs = append(jobs, &JobT{JobID: int64(i + 1)})
		}
		for i := 0; i < 4; i++ {
			require.NoError(t, jobDB.UpdateJobStatus(context.Background(), genJobStatuses(jobs, "failed"), []string{"test"}, []ParameterFilterT{}))
		}

		updatedTableSizes := getTableSizes(jobDB.getDSList())
		require.Equal(t, newTableSizes[fmt.Sprintf("%s_job_status_1", tablePrefix)], updatedTableSizes[fmt.Sprintf("%s_job_status_1", tablePrefix)])
	})
}
