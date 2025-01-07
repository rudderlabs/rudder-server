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
	"github.com/rudderlabs/rudder-server/utils/tx"
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
		// should have enough statuses for a cleanup to be triggered
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
		// the first one should only have non-terminal jobs left now(with only the last status) in jobs_1_1
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

	t.Run("migration between different table types(jsonb, text, bytea)", func(t *testing.T) {
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
				genJobStatuses(jobs[:8], "executing"),
				[]string{customVal},
				[]ParameterFilterT{},
			),
		)
		require.NoError(
			t,
			jobDB.UpdateJobStatus(
				context.Background(),
				genJobStatuses(jobs[:8], "succeeded"),
				[]string{customVal},
				[]ParameterFilterT{},
			),
		)

		require.NoError(
			t,
			jobDB.UpdateJobStatus(
				context.Background(),
				genJobStatuses(jobs[8:10], "executing"),
				[]string{customVal},
				[]ParameterFilterT{},
			),
			`status update failed in 1st DS`,
		)
		require.NoError(
			t,
			jobDB.UpdateJobStatus(
				context.Background(),
				genJobStatuses(jobs[8:10], "failed"),
				[]string{customVal},
				[]ParameterFilterT{},
			),
			`status update failed in 1st DS`,
		)
		require.EqualValues(t, 1, jobDB.GetMaxDSIndex())

		jobDB.conf.payloadColumnType = "bytea"
		triggerAddNewDS <- time.Now() // trigger addNewDSLoop to run
		triggerAddNewDS <- time.Now() // Second time, waits for the first loop to finish
		require.EqualValues(t, 2, jobDB.GetMaxDSIndex())

		var payloadType string
		secondTableName := fmt.Sprintf("%s_jobs_2", tablePrefix)
		err = jobDB.dbHandle.QueryRowContext(context.Background(), fmt.Sprintf(`select data_type from information_schema.columns where table_name='%s' and column_name='event_payload';`, secondTableName)).Scan(&payloadType)
		require.NoError(t, err)
		require.EqualValues(t, "bytea", payloadType)

		// add some more jobs to the new DS
		require.NoError(t, jobDB.Store(context.Background(), jobs[10:20]))

		// triggerMigrateDS <- time.Now()
		// triggerMigrateDS <- time.Now()
		// var payloadType_1_1 string
		// err = jobDB.dbHandle.QueryRowContext(context.Background(), fmt.Sprintf(`select data_type from information_schema.columns where table_name='%s' and column_name='event_payload';`, tablePrefix+"_jobs_1_1")).Scan(&payloadType_1_1)
		// require.NoError(t, err)
		// require.EqualValues(t, "bytea", payloadType_1_1)

		triggerAddNewDS <- time.Now() // trigger addNewDSLoop to run
		triggerAddNewDS <- time.Now() // Second time, waits for the first loop to finish
		require.EqualValues(t, 3, jobDB.GetMaxDSIndex())
		thirdTableName := fmt.Sprintf("%s_jobs_3", tablePrefix)
		err = jobDB.dbHandle.QueryRowContext(context.Background(), fmt.Sprintf(`select data_type from information_schema.columns where table_name='%s' and column_name='event_payload';`, thirdTableName)).Scan(&payloadType)
		require.NoError(t, err)
		require.EqualValues(t, "bytea", payloadType)

		// last DS
		// should have enough statuses for a cleanup to be triggered
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

		c.Set("JobsDB.maxDSSize", 100000)
		jobDB.conf.payloadColumnType = "text"
		triggerMigrateDS <- time.Now() // trigger migrateDSLoop to run
		triggerMigrateDS <- time.Now() // waits for last loop to finish

		// data moved from both jsonb and bytea columns to a text column

		// we should see that in the three DSs we have,
		// the first one should only have non-terminal jobs left now(with only the last status) in jobs_1_1
		// the second one should have all jobs
		// the third DS should have all jobs with only the last status per job

		// check that the first DS has only non-terminal jobs
		dsList := jobDB.getDSList()
		require.Len(t, dsList, 2) // 2_1, 3
		require.Equal(t, `2_1`, dsList[0].Index)
		var count int64
		err = jobDB.dbHandle.QueryRow(
			fmt.Sprintf(
				`SELECT COUNT(*) FROM %[1]s_jobs_2_1 WHERE %[1]s_jobs_2_1.custom_val = $1`,
				tablePrefix,
			),
			customVal,
		).Scan(&count)
		require.NoError(t, err)
		require.EqualValues(t, 12, count)

		err = jobDB.dbHandle.QueryRowContext(context.Background(), fmt.Sprintf(`select data_type from information_schema.columns where table_name='%s' and column_name='event_payload';`, tablePrefix+"_jobs_2_1")).Scan(&payloadType)
		require.NoError(t, err)
		require.EqualValues(t, "text", payloadType)

		require.Equal(t, `3`, dsList[1].Index)
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
		require.EqualValues(t, 100, count)

		getJobs, err := jobDB.GetToProcess(context.Background(), GetQueryParams{
			IgnoreCustomValFiltersInQuery: true,
			EventsLimit:                   1,
			JobsLimit:                     1,
		}, nil)
		require.NoError(t, err)
		require.Equal(t, 1, getJobs.EventsCount)
		require.JSONEq(
			t,
			`{"receivedAt":"2021-06-06T20:26:39.598+05:30","writeKey":"writeKey","requestIP":"[::1]",  "batch": [{"anonymousId":"anon_id","channel":"android-sdk","context":{"app":{"build":"1","name":"RudderAndroidClient","namespace":"com.rudderlabs.android.sdk","version":"1.0"},"device":{"id":"49e4bdd1c280bc00","manufacturer":"Google","model":"Android SDK built for x86","name":"generic_x86"},"library":{"name":"com.rudderstack.android.sdk.core"},"locale":"en-US","network":{"carrier":"Android"},"screen":{"density":420,"height":1794,"width":1080},"traits":{"anonymousId":"49e4bdd1c280bc00"},"user_agent":"Dalvik/2.1.0 (Linux; U; Android 9; Android SDK built for x86 Build/PSR1.180720.075)"},"event":"Demo Track","integrations":{"All":true},"messageId":"b96f3d8a-7c26-4329-9671-4e3202f42f15","originalTimestamp":"2019-08-12T05:08:30.909Z","properties":{"category":"Demo Category","floatVal":4.501,"label":"Demo Label","testArray":[{"id":"elem1","value":"e1"},{"id":"elem2","value":"e2"}],"testMap":{"t1":"a","t2":4},"value":5},"rudderId":"a-292e-4e79-9880-f8009e0ae4a3","sentAt":"2019-08-12T05:08:30.909Z","type":"track"}]}`,
			string(getJobs.Jobs[0].EventPayload),
		)
	})
}

func TestPayloadLiteral(t *testing.T) {
	config.Reset()
	c := config.New()
	c.Set("JobsDB.maxDSSize", 1)

	pg := startPostgres(t)
	db := pg.DB

	byteJD := Handle{
		config: c,
	}
	byteJD.conf.payloadColumnType = BYTEA
	require.NoError(t, byteJD.Setup(
		ReadWrite,
		true,
		string(BYTEA),
	))
	defer byteJD.TearDown()

	jsonbJD := Handle{
		config: c,
	}
	jsonbJD.conf.payloadColumnType = JSONB
	require.NoError(t, jsonbJD.Setup(
		ReadWrite,
		true,
		string(JSONB),
	))
	defer jsonbJD.TearDown()

	textJD := Handle{
		config: c,
	}
	textJD.conf.payloadColumnType = TEXT
	require.NoError(t, textJD.Setup(
		ReadWrite,
		true,
		string(TEXT),
	))
	defer textJD.TearDown()

	ctx := context.Background()
	jobs := genJobs("wsid", "cv", 1, 1)
	require.NoError(t, byteJD.Store(ctx, jobs))
	require.NoError(t, textJD.Store(ctx, jobs))
	require.NoError(t, jsonbJD.Store(ctx, jobs))

	prefixes := []string{string(TEXT), string(JSONB), string(BYTEA)}
	for i := range prefixes {
		_, err := db.ExecContext(ctx, fmt.Sprintf(`ALTER TABLE %[1]s_job_status_1 DROP CONSTRAINT fk_%[1]s_job_status_1_job_id`, prefixes[i]))
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, fmt.Sprintf(`ALTER TABLE %[1]s_jobs_1 DROP CONSTRAINT %[1]s_jobs_1_pkey`, prefixes[i]))
		require.NoError(t, err)
	} // we drop these two because migrateJobsInTx moved jobIDs too, and we're only interested in moving jobs between two different column types
	txn, err := db.Begin()
	require.NoError(t, err)
	for i := range prefixes {
		for j := range prefixes {
			if i == j {
				continue
			}
			src := prefixes[i]
			dest := prefixes[j]
			_, err := textJD.migrateJobsInTx(
				ctx,
				&tx.Tx{Tx: txn},
				dataSetT{
					JobTable:       src + "_jobs_1",
					JobStatusTable: src + "_job_status_1",
					Index:          "1",
				},
				dataSetT{
					JobTable:       dest + "_jobs_1",
					JobStatusTable: dest + "_job_status_1",
					Index:          "1",
				},
			)
			require.NoError(t, err, src, dest)
		}
	}
	require.NoError(t, txn.Commit())

	byteJobs, err := byteJD.GetUnprocessed(ctx, GetQueryParams{
		EventsLimit: 100, JobsLimit: 100, IgnoreCustomValFiltersInQuery: true,
	})
	require.NoError(t, err)
	textJobs, err := textJD.GetUnprocessed(ctx, GetQueryParams{
		EventsLimit: 100, JobsLimit: 100, IgnoreCustomValFiltersInQuery: true,
	})
	require.NoError(t, err)
	jsonbJobs, err := jsonbJD.GetUnprocessed(ctx, GetQueryParams{
		EventsLimit: 100, JobsLimit: 100, IgnoreCustomValFiltersInQuery: true,
	})
	require.NoError(t, err)
	require.Equal(t, 4, byteJobs.EventsCount)
	require.Equal(t, 7, textJobs.EventsCount)
	require.Equal(t, 6, jsonbJobs.EventsCount)
	expectedPayload := `{"receivedAt":"2021-06-06T20:26:39.598+05:30","writeKey":"writeKey","requestIP":"[::1]",  "batch": [{"anonymousId":"anon_id","channel":"android-sdk","context":{"app":{"build":"1","name":"RudderAndroidClient","namespace":"com.rudderlabs.android.sdk","version":"1.0"},"device":{"id":"49e4bdd1c280bc00","manufacturer":"Google","model":"Android SDK built for x86","name":"generic_x86"},"library":{"name":"com.rudderstack.android.sdk.core"},"locale":"en-US","network":{"carrier":"Android"},"screen":{"density":420,"height":1794,"width":1080},"traits":{"anonymousId":"49e4bdd1c280bc00"},"user_agent":"Dalvik/2.1.0 (Linux; U; Android 9; Android SDK built for x86 Build/PSR1.180720.075)"},"event":"Demo Track","integrations":{"All":true},"messageId":"b96f3d8a-7c26-4329-9671-4e3202f42f15","originalTimestamp":"2019-08-12T05:08:30.909Z","properties":{"category":"Demo Category","floatVal":4.501,"label":"Demo Label","testArray":[{"id":"elem1","value":"e1"},{"id":"elem2","value":"e2"}],"testMap":{"t1":"a","t2":4},"value":5},"rudderId":"a-292e-4e79-9880-f8009e0ae4a3","sentAt":"2019-08-12T05:08:30.909Z","type":"track"}]}`

	for i := range byteJobs.Jobs {
		require.JSONEq(
			t,
			expectedPayload,
			string(byteJobs.Jobs[i].EventPayload),
		)
	}
	for i := range textJobs.Jobs {
		require.JSONEq(
			t,
			expectedPayload,
			string(textJobs.Jobs[i].EventPayload),
		)
	}
	for i := range jsonbJobs.Jobs {
		require.JSONEq(
			t,
			expectedPayload,
			string(jsonbJobs.Jobs[i].EventPayload),
		)
	}
}

func Test_GetColumnConversion(t *testing.T) {
	t.Run("bytea to text", func(t *testing.T) {
		res, err := getColumnConversion(string(BYTEA), string(TEXT))
		require.NoError(t, err)
		require.Equal(t, `convert_from(j.event_payload, 'UTF8')`, res)
	})
	t.Run("bytea to jsonb", func(t *testing.T) {
		res, err := getColumnConversion(string(BYTEA), string(JSONB))
		require.NoError(t, err)
		require.Equal(t, `convert_from(j.event_payload, 'UTF8')::jsonb`, res)
	})
	t.Run("text to bytea", func(t *testing.T) {
		res, err := getColumnConversion(string(TEXT), string(BYTEA))
		require.NoError(t, err)
		require.Equal(t, `convert_to(j.event_payload, 'UTF8')`, res)
	})
	t.Run("text to jsonb", func(t *testing.T) {
		res, err := getColumnConversion(string(TEXT), string(JSONB))
		require.NoError(t, err)
		require.Equal(t, `j.event_payload::jsonb`, res)
	})
	t.Run("jsonb to bytea", func(t *testing.T) {
		res, err := getColumnConversion(string(JSONB), string(BYTEA))
		require.NoError(t, err)
		require.Equal(t, `convert_to(j.event_payload::TEXT, 'UTF8')`, res)
	})
	t.Run("jsonb to text", func(t *testing.T) {
		res, err := getColumnConversion(string(JSONB), string(TEXT))
		require.NoError(t, err)
		require.Equal(t, `j.event_payload::TEXT`, res)
	})
	t.Run("invalid conversion", func(t *testing.T) {
		_, err := getColumnConversion(string(JSONB), "random")
		require.Error(t, err)
	})
}
