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
			),
		)
		require.NoError(
			t,
			jobDB.UpdateJobStatus(
				context.Background(),
				genJobStatuses(jobs[:9], "succeeded"),
			),
		)

		require.NoError(
			t,
			jobDB.UpdateJobStatus(
				context.Background(),
				genJobStatuses(jobs[9:10], "executing"),
			),
			`status update failed in 1st DS`,
		)
		require.NoError(
			t,
			jobDB.UpdateJobStatus(
				context.Background(),
				genJobStatuses(jobs[9:10], "failed"),
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
		for range 10 {
			require.NoError(
				t,
				jobDB.UpdateJobStatus(
					context.Background(),
					genJobStatuses(jobs[20:30], "executing"),
				),
				`status update failed in 3rd DS`,
			)
			require.NoError(
				t,
				jobDB.UpdateJobStatus(
					context.Background(),
					genJobStatuses(jobs[20:30], "failed"),
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
		for i := range 3 {
			require.NoError(t, jobDB.Store(context.Background(), genJobs(defaultWorkspaceID, "test", 10, 1)))
			triggerAddNewDS <- time.Now() // trigger addNewDSLoop to run
			triggerAddNewDS <- time.Now() // waits for last loop to finish
			require.Equal(t, i+2, len(jobDB.getDSList()))
		}

		// 1st ds 5 statuses each
		var jobs []*JobT
		for i := range 10 {
			jobs = append(jobs, &JobT{JobID: int64(i + 1)})
		}
		for range 5 {
			require.NoError(t, jobDB.UpdateJobStatus(context.Background(), genJobStatuses(jobs, "failed")))
		}
		var count int
		require.NoError(t, jobDB.dbHandle.QueryRow(fmt.Sprintf(`SELECT COUNT(*) FROM %[1]s_job_status_1 `, tablePrefix)).Scan(&count))
		require.EqualValues(t, 5*10, count)

		// 2nd ds 10 statuses each
		jobs = nil
		for i := range 10 {
			jobs = append(jobs, &JobT{JobID: int64(i + 11)})
		}
		for range 10 {
			require.NoError(t, jobDB.UpdateJobStatus(context.Background(), genJobStatuses(jobs, "failed")))
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
		for i := range 10 {
			jobs = append(jobs, &JobT{JobID: int64(i + 1)})
		}
		for range 4 {
			require.NoError(t, jobDB.UpdateJobStatus(context.Background(), genJobStatuses(jobs, "failed")))
		}

		updatedTableSizes := getTableSizes(jobDB.getDSList())
		require.Equal(t, newTableSizes[fmt.Sprintf("%s_job_status_1", tablePrefix)], updatedTableSizes[fmt.Sprintf("%s_job_status_1", tablePrefix)])
	})

	t.Run("migration between different table types(jsonb, text, bytea)", func(t *testing.T) {
		config.Reset()
		c := config.New()
		c.Set("JobsDB.maxDSSize", 1)
		c.Set("JobsDB.jobMinRowsLeftMigrateThres", 0.2)

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
			),
		)
		require.NoError(
			t,
			jobDB.UpdateJobStatus(
				context.Background(),
				genJobStatuses(jobs[:8], "succeeded"),
			),
		)

		require.NoError(
			t,
			jobDB.UpdateJobStatus(
				context.Background(),
				genJobStatuses(jobs[8:10], "executing"),
			),
			`status update failed in 1st DS`,
		)
		require.NoError(
			t,
			jobDB.UpdateJobStatus(
				context.Background(),
				genJobStatuses(jobs[8:10], "failed"),
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
		for range 10 {
			require.NoError(
				t,
				jobDB.UpdateJobStatus(
					context.Background(),
					genJobStatuses(jobs[20:30], "executing"),
				),
				`status update failed in 3rd DS`,
			)
			require.NoError(
				t,
				jobDB.UpdateJobStatus(
					context.Background(),
					genJobStatuses(jobs[20:30], "failed"),
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
			EventsLimit: 1,
			JobsLimit:   1,
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
		_, err := db.ExecContext(ctx, fmt.Sprintf(`ALTER TABLE %[1]s_jobs_1 DROP CONSTRAINT %[1]s_jobs_1_pkey`, prefixes[i]))
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
		EventsLimit: 100, JobsLimit: 100,
	})
	require.NoError(t, err)
	textJobs, err := textJD.GetUnprocessed(ctx, GetQueryParams{
		EventsLimit: 100, JobsLimit: 100,
	})
	require.NoError(t, err)
	jsonbJobs, err := jsonbJD.GetUnprocessed(ctx, GetQueryParams{
		EventsLimit: 100, JobsLimit: 100,
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

func TestMigrationMaxDSSizeGuard(t *testing.T) {
	_ = startPostgres(t)

	// newJobDB creates a Handle with the given maxDSSize and jobMinRowsLeftMigrateThreshold.
	newJobDB := func(t *testing.T, maxDSSize int, threshold float64) (*Handle, chan time.Time, *config.Config) {
		t.Helper()
		c := config.New()
		c.Set("JobsDB.maxDSSize", maxDSSize)
		c.Set("JobsDB.jobMinRowsLeftMigrateThreshold", threshold)
		triggerAddNewDS := make(chan time.Time)
		jd := &Handle{
			TriggerAddNewDS:  func() <-chan time.Time { return triggerAddNewDS },
			TriggerMigrateDS: func() <-chan time.Time { return make(chan time.Time) },
			config:           c,
		}
		require.NoError(t, jd.Setup(ReadWrite, true, strings.ToLower(rand.String(5))))
		t.Cleanup(jd.TearDown)
		return jd, triggerAddNewDS, c
	}

	// addDS stores `jobs` with len(jobs)-pending marked as succeeded, then triggers addNewDS.
	// Jobs must be a slice of a larger pre-created slice so that their pre-set IDs match the
	// DB-assigned IDs (which start at 1 and increment globally per table).
	addDS := func(t *testing.T, jd *Handle, trigger chan time.Time, jobs []*JobT, pending int) {
		t.Helper()
		require.NoError(t, jd.Store(context.Background(), jobs))
		if terminal := len(jobs) - pending; terminal > 0 {
			require.NoError(t, jd.UpdateJobStatus(context.Background(), genJobStatuses(jobs[:terminal], "executing")))
			require.NoError(t, jd.UpdateJobStatus(context.Background(), genJobStatuses(jobs[:terminal], "succeeded")))
		}
		trigger <- time.Now()
		trigger <- time.Now()
	}

	t.Run("accumulation stops before exceeding maxDSSize", func(t *testing.T) {
		// maxDSSize=10, threshold=0.7 → pair threshold=7
		// DS1..DS4: 3 pending each (needsPair since 3 < 7)
		// DS5: last DS (exempt)
		//
		// getMigrationList walk:
		//   DS1 → waiting
		//   DS2 → pair: 3+3=6 ≤ 10, pendingJobsCount=6
		//   DS3 → piggyback: 6+3=9 ≤ 10, pendingJobsCount=9
		//   DS4 → piggyback: 9+3=12 > 10 → break
		// expected: migrateFrom=[DS1,DS2,DS3], pendingJobsCount=9 ≤ maxDSSize
		jd, trigger, c := newJobDB(t, 10, 0.7)
		allJobs := genJobs(defaultWorkspaceID, "test", 50, 1) // 4 regular DSes + 1 last, 10 jobs each
		for i := range 4 {
			addDS(t, jd, trigger, allJobs[i*10:(i+1)*10], 3)
		}
		require.NoError(t, jd.Store(context.Background(), allJobs[40:]))

		dsList := jd.getDSList()
		c.Set("JobsDB."+jd.tablePrefix+"."+"maxMigrateDSProbe", len(dsList))

		result, err := jd.getMigrationList(dsList, nil)
		require.NoError(t, err)
		require.Len(t, result.migrateFrom, 3)
		require.Equal(t, 9, result.pendingJobsCount)
		require.LessOrEqual(t, result.pendingJobsCount, 10)
	})

	t.Run("pair exceeding maxDSSize is discarded", func(t *testing.T) {
		// maxDSSize=10, threshold=0.7 → pair threshold=7
		// DS1: 6 pending (needsPair since 6 < 7)
		// DS2: 6 pending → 6+6=12 > maxDSSize → waiting cleared, nothing migrates
		// DS3: last DS (exempt)
		jd, trigger, c := newJobDB(t, 10, 0.7)
		allJobs := genJobs(defaultWorkspaceID, "test", 30, 1) // 2 regular DSes + 1 last, 10 jobs each
		addDS(t, jd, trigger, allJobs[:10], 6)
		addDS(t, jd, trigger, allJobs[10:20], 6)
		require.NoError(t, jd.Store(context.Background(), allJobs[20:]))

		dsList := jd.getDSList()
		c.Set("JobsDB."+jd.tablePrefix+"."+"maxMigrateDSProbe", len(dsList))

		result, err := jd.getMigrationList(dsList, nil)
		require.NoError(t, err)
		require.Empty(t, result.migrateFrom)
	})
}

func TestMigrationSkipsDatasets(t *testing.T) {
	config.Reset()
	c := config.New()
	c.Set("JobsDB.maxDSSize", 10)

	_ = startPostgres(t)

	triggerAddNewDS := make(chan time.Time)
	triggerMigrateDS := make(chan time.Time)

	jobDB := Handle{
		TriggerAddNewDS:  func() <-chan time.Time { return triggerAddNewDS },
		TriggerMigrateDS: func() <-chan time.Time { return triggerMigrateDS },
		config:           c,
	}
	tablePrefix := strings.ToLower(rand.String(5))
	require.NoError(t, jobDB.Setup(ReadWrite, true, tablePrefix))
	defer jobDB.TearDown()

	const totalDS = 200
	const eligibleDSPos = 150 // 0-indexed position of the dataset we'll make eligible

	// Create totalDS datasets: store jobs then trigger addNewDS to create the next one
	for i := range totalDS - 1 { // -1 because Setup already creates DS 1
		_ = i
		require.NoError(t, jobDB.Store(context.Background(), genJobs(defaultWorkspaceID, "test", 10, 1)))
		triggerAddNewDS <- time.Now()
		triggerAddNewDS <- time.Now()
	}
	// Store jobs in the last DS too
	require.NoError(t, jobDB.Store(context.Background(), genJobs(defaultWorkspaceID, "test", 10, 1)))

	dsList := jobDB.getDSList()
	require.Len(t, dsList, totalDS)

	// Make all jobs in the dataset at eligibleDSPos terminal (succeeded)
	eligibleDS := dsList[eligibleDSPos]
	rows, err := jobDB.dbHandle.Query(fmt.Sprintf(`SELECT job_id FROM %q`, eligibleDS.JobTable))
	require.NoError(t, err)
	var statusJobs []*JobT
	for rows.Next() {
		var id int64
		require.NoError(t, rows.Scan(&id))
		statusJobs = append(statusJobs, &JobT{JobID: id})
	}
	require.NoError(t, rows.Err())
	_ = rows.Close()

	require.NoError(t, jobDB.UpdateJobStatus(context.Background(), genJobStatuses(statusJobs, "succeeded")))

	t.Run("intra-invocation skip", func(t *testing.T) {
		// Allow probing enough datasets to reach the eligible one in a single call
		c.Set("JobsDB."+tablePrefix+"."+"maxMigrateDSProbe", totalDS)

		// Measure first getMigrationList call (full scan, no skip)
		checkStart := time.Now()
		checkResult, err := jobDB.getMigrationList(dsList, nil)
		checkDuration := time.Since(checkStart)
		require.NoError(t, err)
		require.NotEmpty(t, checkResult.migrateFrom, "should find eligible datasets")
		require.NotNil(t, checkResult.firstEligible, "should have firstEligible set")

		// Measure second getMigrationList call (with skipBefore from first call)
		lockCheckStart := time.Now()
		lockResult, err := jobDB.getMigrationList(dsList, checkResult.firstEligible)
		lockCheckDuration := time.Since(lockCheckStart)
		require.NoError(t, err)
		require.NotEmpty(t, lockResult.migrateFrom)

		// Both calls should find the same eligible datasets
		require.Equal(t, len(checkResult.migrateFrom), len(lockResult.migrateFrom))
		for i := range checkResult.migrateFrom {
			require.Equal(t, checkResult.migrateFrom[i].ds.Index, lockResult.migrateFrom[i].ds.Index)
		}

		t.Logf("check duration (no skip):   %v", checkDuration)
		t.Logf("lock check duration (skip):  %v", lockCheckDuration)

		// The second call with skipBefore should be significantly faster
		require.Greater(t, checkDuration, lockCheckDuration,
			"getMigrationList with skipBefore should be faster than without",
		)
	})

	t.Run("cross-invocation resume", func(t *testing.T) {
		// Set maxMigrateDSProbe to 100 so the first iteration can't reach the
		// eligible dataset at position 150. It will need 2 iterations.
		c.Set("JobsDB."+tablePrefix+"."+"maxMigrateDSProbe", 100)
		jobDB.lastMigrateProbeIndex = nil

		// 1st iteration: probes datasets 1..100, finds nothing, hits probe limit.
		// Should store lastMigrateProbeIndex for resumption.
		result1, err := jobDB.getMigrationList(dsList, jobDB.lastMigrateProbeIndex)
		require.NoError(t, err)
		require.Empty(t, result1.migrateFrom, "should not find eligible datasets in first 100")
		require.True(t, result1.probeLimitReached, "should hit probe limit")
		require.NotNil(t, result1.lastProbed, "should have lastProbed set")

		// Simulate what doMigrateDS does: save the resume point
		jobDB.lastMigrateProbeIndex = result1.lastProbed

		// 2nd iteration: resumes from where the first left off, finds the eligible dataset.
		resumeStart := time.Now()
		result2, err := jobDB.getMigrationList(dsList, jobDB.lastMigrateProbeIndex)
		resumeDuration := time.Since(resumeStart)
		require.NoError(t, err)
		require.NotEmpty(t, result2.migrateFrom, "should find eligible datasets in second iteration")
		require.Equal(t, dsList[eligibleDSPos].Index, result2.migrateFrom[0].ds.Index)

		// Compare with a full scan from scratch
		c.Set("JobsDB."+tablePrefix+"."+"maxMigrateDSProbe", totalDS)
		fullStart := time.Now()
		resultFull, err := jobDB.getMigrationList(dsList, nil)
		fullDuration := time.Since(fullStart)
		require.NoError(t, err)
		require.NotEmpty(t, resultFull.migrateFrom)

		t.Logf("full scan duration:    %v", fullDuration)
		t.Logf("resumed scan duration: %v", resumeDuration)

		// The resumed scan should be faster than a full scan
		require.Greater(t, fullDuration, resumeDuration,
			"resumed scan should be faster than full scan from scratch",
		)
	})
}
