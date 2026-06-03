package jobsdb

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/lib/pq"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/testhelper/rand"

	"github.com/rudderlabs/rudder-server/jobsdb/internal/lock"
	"github.com/rudderlabs/rudder-server/utils/tx"
)

func TestCompaction(t *testing.T) {
	t.Run("main", func(t *testing.T) {
		config.Reset()
		c := config.New()
		c.Set("JobsDB.maxDSSize", 1)

		_ = startPostgres(t)

		triggerAddNewDS := make(chan time.Time)
		triggerCompaction := make(chan time.Time)

		jobDB := Handle{
			TriggerAddNewDS: func() <-chan time.Time {
				return triggerAddNewDS
			},
			TriggerCompaction: func() <-chan time.Time {
				return triggerCompaction
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
		triggerCompaction <- time.Now() // trigger compactionLoop to run
		triggerCompaction <- time.Now() // waits for last loop to finish

		// we should see that in the three DSs we have,
		// the first one should only have non-terminal jobs left now(with only the last status) in jobs_1_1
		// the second one should have all jobs
		// the third DS should have all jobs with only the last status per job

		// check that the first DS has only non-terminal jobs
		dsList := jobDB.getDSListSnapshot()
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

		// second DS must be untouched by this compactionLoop
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
		triggerCompaction := make(chan time.Time)
		config.Reset()
		c := config.New()
		c.Set("JobsDB.maxDSSize", 1)

		jobDB := Handle{
			TriggerAddNewDS: func() <-chan time.Time {
				return triggerAddNewDS
			},
			TriggerCompaction: func() <-chan time.Time {
				return triggerCompaction
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
			require.Equal(t, i+2, len(jobDB.getDSListSnapshot()))
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
				`SELECT c.relname, pg_catalog.pg_table_size(c.oid) AS size
				FROM pg_catalog.pg_class c
				JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
				LEFT JOIN pg_catalog.pg_description d ON d.objoid = c.oid AND d.objsubid = 0
				WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
					AND c.relkind = 'r'
					AND c.relname LIKE $1
					AND (d.description IS NULL OR d.description NOT LIKE 'rudder:pre_drop:%')
				ORDER BY c.relname;`,
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
		originalTableSizes := getTableSizes(jobDB.getDSListSnapshot())

		c.Set("JobsDB.jobStatusMigrateThreshold", 1)
		c.Set("JobsDB.vacuumAnalyzeStatusTableThreshold", 4)
		c.Set("JobsDB.vacuumFullStatusTableThreshold", fmt.Sprint(
			originalTableSizes[fmt.Sprintf("%s_job_status_2", tablePrefix)]-1,
		))

		// run cleanup status tables
		require.NoError(t, jobDB.cleanupStatusTables(context.Background(), jobDB.getDSListSnapshot()))

		newTableSizes := getTableSizes(jobDB.getDSListSnapshot())

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

		updatedTableSizes := getTableSizes(jobDB.getDSListSnapshot())
		require.Equal(t, newTableSizes[fmt.Sprintf("%s_job_status_1", tablePrefix)], updatedTableSizes[fmt.Sprintf("%s_job_status_1", tablePrefix)])
	})

	t.Run("compaction between different table types(jsonb, text, bytea)", func(t *testing.T) {
		config.Reset()
		c := config.New()
		c.Set("JobsDB.maxDSSize", 1)
		c.Set("JobsDB.jobMinRowsLeftMigrateThres", 0.2)

		_ = startPostgres(t)

		triggerAddNewDS := make(chan time.Time)
		triggerCompaction := make(chan time.Time)

		jobDB := Handle{
			TriggerAddNewDS: func() <-chan time.Time {
				return triggerAddNewDS
			},
			TriggerCompaction: func() <-chan time.Time {
				return triggerCompaction
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
		triggerCompaction <- time.Now() // trigger compactionLoop to run
		triggerCompaction <- time.Now() // waits for last loop to finish

		// data moved from both jsonb and bytea columns to a text column

		// we should see that in the three DSs we have,
		// the first one should only have non-terminal jobs left now(with only the last status) in jobs_1_1
		// the second one should have all jobs
		// the third DS should have all jobs with only the last status per job

		// check that the first DS has only non-terminal jobs
		dsList := jobDB.getDSListSnapshot()
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
	} // we drop these two because compactJobsInTx moved jobIDs too, and we're only interested in moving jobs between two different column types
	txn, err := db.Begin()
	require.NoError(t, err)
	for i := range prefixes {
		for j := range prefixes {
			if i == j {
				continue
			}
			src := prefixes[i]
			dest := prefixes[j]
			_, err := textJD.compactJobsInTx(
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

func TestCompactionMaxDSSizeGuard(t *testing.T) {
	_ = startPostgres(t)

	// newJobDB creates a Handle with the given maxDSSize and jobMinRowsLeftCompactionThreshold.
	newJobDB := func(t *testing.T, maxDSSize int, threshold float64) (*Handle, chan time.Time, *config.Config) {
		t.Helper()
		c := config.New()
		c.Set("JobsDB.maxDSSize", maxDSSize)
		c.Set("JobsDB.jobMinRowsLeftMigrateThreshold", threshold)
		triggerAddNewDS := make(chan time.Time)
		jd := &Handle{
			TriggerAddNewDS:   func() <-chan time.Time { return triggerAddNewDS },
			TriggerCompaction: func() <-chan time.Time { return make(chan time.Time) },
			config:            c,
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
		// getCompactionList walk:
		//   DS1 → waiting
		//   DS2 → pair: 3+3=6 ≤ 10, pendingJobsCount=6
		//   DS3 → piggyback: 6+3=9 ≤ 10, pendingJobsCount=9
		//   DS4 → piggyback: 9+3=12 > 10 → break
		// expected: compactFrom=[DS1,DS2,DS3], pendingJobsCount=9 ≤ maxDSSize
		jd, trigger, c := newJobDB(t, 10, 0.7)
		allJobs := genJobs(defaultWorkspaceID, "test", 50, 1) // 4 regular DSes + 1 last, 10 jobs each
		for i := range 4 {
			addDS(t, jd, trigger, allJobs[i*10:(i+1)*10], 3)
		}
		require.NoError(t, jd.Store(context.Background(), allJobs[40:]))

		dsList := jd.getDSListSnapshot()
		c.Set("JobsDB."+jd.tablePrefix+"."+"maxMigrateDSProbe", len(dsList))

		result, err := jd.getCompactionList(dsList, nil, jd.maintenanceDB())
		require.NoError(t, err)
		require.Len(t, result.compactFrom, 3)
		require.Equal(t, 9, result.pendingJobsCount)
		require.LessOrEqual(t, result.pendingJobsCount, 10)
	})

	t.Run("pair exceeding maxDSSize is discarded", func(t *testing.T) {
		// maxDSSize=10, threshold=0.7 → pair threshold=7
		// DS1: 6 pending (needsPair since 6 < 7)
		// DS2: 6 pending → 6+6=12 > maxDSSize → waiting cleared, nothing compacts
		// DS3: last DS (exempt)
		jd, trigger, c := newJobDB(t, 10, 0.7)
		allJobs := genJobs(defaultWorkspaceID, "test", 30, 1) // 2 regular DSes + 1 last, 10 jobs each
		addDS(t, jd, trigger, allJobs[:10], 6)
		addDS(t, jd, trigger, allJobs[10:20], 6)
		require.NoError(t, jd.Store(context.Background(), allJobs[20:]))

		dsList := jd.getDSListSnapshot()
		c.Set("JobsDB."+jd.tablePrefix+"."+"maxMigrateDSProbe", len(dsList))

		result, err := jd.getCompactionList(dsList, nil, jd.maintenanceDB())
		require.NoError(t, err)
		require.Empty(t, result.compactFrom)
	})
}

func TestCompactionSkipsDatasets(t *testing.T) {
	config.Reset()
	c := config.New()
	c.Set("JobsDB.maxDSSize", 10)

	_ = startPostgres(t)

	triggerAddNewDS := make(chan time.Time)
	triggerCompaction := make(chan time.Time)

	jobDB := Handle{
		TriggerAddNewDS:   func() <-chan time.Time { return triggerAddNewDS },
		TriggerCompaction: func() <-chan time.Time { return triggerCompaction },
		config:            c,
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

	dsList := jobDB.getDSListSnapshot()
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

		// Measure first getCompactionList call (full scan, no skip)
		checkStart := time.Now()
		checkResult, err := jobDB.getCompactionList(dsList, nil, jobDB.maintenanceDB())
		checkDuration := time.Since(checkStart)
		require.NoError(t, err)
		require.NotEmpty(t, checkResult.compactFrom, "should find eligible datasets")
		require.NotNil(t, checkResult.firstEligible, "should have firstEligible set")

		// Measure second getCompactionList call (with skipBefore from first call)
		lockCheckStart := time.Now()
		lockResult, err := jobDB.getCompactionList(dsList, checkResult.firstEligible, jobDB.maintenanceDB())
		lockCheckDuration := time.Since(lockCheckStart)
		require.NoError(t, err)
		require.NotEmpty(t, lockResult.compactFrom)

		// Both calls should find the same eligible datasets
		require.Equal(t, len(checkResult.compactFrom), len(lockResult.compactFrom))
		for i := range checkResult.compactFrom {
			require.Equal(t, checkResult.compactFrom[i].ds.Index, lockResult.compactFrom[i].ds.Index)
		}

		t.Logf("check duration (no skip):   %v", checkDuration)
		t.Logf("lock check duration (skip):  %v", lockCheckDuration)

		// The second call with skipBefore should be significantly faster
		require.Greater(t, checkDuration, lockCheckDuration,
			"getCompactionList with skipBefore should be faster than without",
		)
	})

	t.Run("cross-invocation resume", func(t *testing.T) {
		// Set maxMigrateDSProbe to 100 so the first iteration can't reach the
		// eligible dataset at position 150. It will need 2 iterations.
		c.Set("JobsDB."+tablePrefix+"."+"maxMigrateDSProbe", 100)
		jobDB.lastCompactionProbeIndex = nil

		// 1st iteration: probes datasets 1..100, finds nothing, hits probe limit.
		// Should store lastCompactionProbeIndex for resumption.
		result1, err := jobDB.getCompactionList(dsList, jobDB.lastCompactionProbeIndex, jobDB.maintenanceDB())
		require.NoError(t, err)
		require.Empty(t, result1.compactFrom, "should not find eligible datasets in first 100")
		require.True(t, result1.probeLimitReached, "should hit probe limit")
		require.NotNil(t, result1.lastProbed, "should have lastProbed set")

		// Simulate what doCompaction does: save the resume point
		jobDB.lastCompactionProbeIndex = result1.lastProbed

		// 2nd iteration: resumes from where the first left off, finds the eligible dataset.
		resumeStart := time.Now()
		result2, err := jobDB.getCompactionList(dsList, jobDB.lastCompactionProbeIndex, jobDB.maintenanceDB())
		resumeDuration := time.Since(resumeStart)
		require.NoError(t, err)
		require.NotEmpty(t, result2.compactFrom, "should find eligible datasets in second iteration")
		require.Equal(t, dsList[eligibleDSPos].Index, result2.compactFrom[0].ds.Index)

		// Compare with a full scan from scratch
		c.Set("JobsDB."+tablePrefix+"."+"maxMigrateDSProbe", totalDS)
		fullStart := time.Now()
		resultFull, err := jobDB.getCompactionList(dsList, nil, jobDB.maintenanceDB())
		fullDuration := time.Since(fullStart)
		require.NoError(t, err)
		require.NotEmpty(t, resultFull.compactFrom)

		t.Logf("full scan duration:    %v", fullDuration)
		t.Logf("resumed scan duration: %v", resumeDuration)

		// The resumed scan should be faster than a full scan
		require.Greater(t, fullDuration, resumeDuration,
			"resumed scan should be faster than full scan from scratch",
		)
	})
}

func TestCompactionNonBlockingCompletedDSDrop(t *testing.T) {
	_ = startPostgres(t)

	// newJobDB creates a ReadWrite Handle with the compaction and addNewDS loop triggers
	// wired to never-firing channels — the tests below drive state changes synchronously
	// via createDSInTx so there is no background addNewDSLoop racing with Store/UpdateJobStatus.
	newJobDB := func(t *testing.T) (*Handle, *config.Config) {
		t.Helper()
		c := config.New()
		c.Set("JobsDB.maxDSSize", 100000)
		jd := &Handle{
			TriggerAddNewDS:   func() <-chan time.Time { return make(chan time.Time) },
			TriggerCompaction: func() <-chan time.Time { return make(chan time.Time) },
			config:            c,
		}
		require.NoError(t, jd.Setup(ReadWrite, true, strings.ToLower(rand.String(5))))
		t.Cleanup(jd.TearDown)
		return jd, c
	}

	// createDS synchronously appends a new DS at the given index using addNewDSInTx
	// (which also advances the new DS's job_id sequence to continue from the previous
	// DS's max, preserving the monotonic range invariant), then refreshes the in-memory list.
	createDS := func(t *testing.T, jd *Handle, idx string) {
		t.Helper()
		require.NoError(t, jd.dsListLock.WithLockInCtx(context.Background(), func(l lock.LockToken) error {
			dsList, _ := jd.dsList.snapshot()
			if err := jd.WithTx(context.Background(), func(txn *tx.Tx) error {
				return jd.addNewDSInTx(context.Background(), txn, l, dsList, newDataSet(jd.tablePrefix, idx))
			}); err != nil {
				return err
			}
			return jd.doRefreshDSRangeList(l)
		}))
	}

	// fillDS stores jobs in the current last DS, marks (len(jobs)-pending) of them as succeeded.
	fillDS := func(t *testing.T, jd *Handle, jobs []*JobT, pending int) {
		t.Helper()
		require.NoError(t, jd.Store(context.Background(), jobs))
		if terminal := len(jobs) - pending; terminal > 0 {
			require.NoError(t, jd.UpdateJobStatus(context.Background(), genJobStatuses(jobs[:terminal], "executing")))
			require.NoError(t, jd.UpdateJobStatus(context.Background(), genJobStatuses(jobs[:terminal], "succeeded")))
		}
	}

	tableExists := func(t *testing.T, jd *Handle, ds dataSetT) bool {
		t.Helper()
		var exists bool
		require.NoError(t, jd.dbHandle.QueryRow(`SELECT to_regclass($1) IS NOT NULL`, ds.JobTable).Scan(&exists))
		return exists
	}

	t.Run("flag off: completed DS dropped in legacy in-tx path", func(t *testing.T) {
		jd, c := newJobDB(t)
		c.Set("JobsDB."+jd.tablePrefix+"."+"nonBlockingCompaction", false)
		jobs := genJobs(defaultWorkspaceID, "test", 10, 1)
		fillDS(t, jd, jobs, 0) // DS_1 → all 10 succeeded (completed)
		createDS(t, jd, "2")   // DS_2 → empty last (exempt from compaction)

		snapshot := jd.getDSListSnapshot()
		require.Len(t, snapshot, 2)
		completedDS := snapshot[0]

		require.NoError(t, jd.doCompaction(context.Background()))

		listIndices := lo.Map(jd.getDSListSnapshot(), func(ds dataSetT, _ int) string { return ds.Index })
		require.NotContains(t, listIndices, completedDS.Index, "completed DS should be removed from the published list")
		require.False(t, tableExists(t, jd, completedDS), "table must be dropped synchronously by the legacy in-tx path")
		jd.dropDSListLock.RLock()
		require.Empty(t, jd.dropDSList, "legacy path must not enqueue any async drop entries")
		jd.dropDSListLock.RUnlock()
	})

	t.Run("flag on: completed DS routed to async drop list", func(t *testing.T) {
		jd, c := newJobDB(t)
		c.Set("JobsDB."+jd.tablePrefix+"."+"nonBlockingCompaction", false)
		c.Set("JobsDB."+jd.tablePrefix+"."+"nonBlockingCompletedDSDrop", true)

		jobs := genJobs(defaultWorkspaceID, "test", 10, 1)
		fillDS(t, jd, jobs, 0) // DS_1 completed
		createDS(t, jd, "2")   // DS_2 empty last

		snapshot := jd.getDSListSnapshot()
		require.Len(t, snapshot, 2)
		completedDS := snapshot[0]

		// Hold a reader on the current dsList version so dropDSLoop blocks on
		// waitUntilDrained — lets us observe the enqueued entry before it is physically dropped.
		_, _, release, err := jd.acquireDSListForRead(context.Background())
		require.NoError(t, err)

		require.NoError(t, jd.doCompaction(context.Background()))

		listIndices := lo.Map(jd.getDSListSnapshot(), func(ds dataSetT, _ int) string { return ds.Index })
		require.NotContains(t, listIndices, completedDS.Index, "completed DS hidden from new readers")
		jd.dropDSListLock.RLock()
		require.Len(t, jd.dropDSList, 1, "completed DS must be enqueued for async drop")
		require.Equal(t, completedDS.Index, jd.dropDSList[0].ds.Index)
		jd.dropDSListLock.RUnlock()
		require.Never(t,
			func() bool { return !tableExists(t, jd, completedDS) },
			200*time.Millisecond, 20*time.Millisecond,
			"physical drop must block until the held reader releases",
		)

		release()

		require.Eventually(t, func() bool {
			jd.dropDSListLock.RLock()
			pending := len(jd.dropDSList)
			jd.dropDSListLock.RUnlock()
			return !tableExists(t, jd, completedDS) && pending == 0
		}, 5*time.Second, 50*time.Millisecond, "table must be physically dropped once the reader releases")
	})

	t.Run("flag on: DS with pending jobs still compacted through the legacy TX", func(t *testing.T) {
		jd, c := newJobDB(t)
		c.Set("JobsDB."+jd.tablePrefix+"."+"nonBlockingCompaction", false)
		// maxDSRetention=1ms makes DSes with old terminal jobs eligible without the needsPair logic,
		// so a DS with pending jobs can be compacted alone.
		c.Set("JobsDB."+jd.tablePrefix+"."+"maxDSRetention", "1ms")

		jobs := genJobs(defaultWorkspaceID, "test", 10, 1)
		fillDS(t, jd, jobs, 5) // DS_1: 5 succeeded + 5 pending
		createDS(t, jd, "2")   // DS_2 empty last

		snapshot := jd.getDSListSnapshot()
		require.Len(t, snapshot, 2)
		pendingDS := snapshot[0]

		require.NoError(t, jd.doCompaction(context.Background()))

		require.False(t, tableExists(t, jd, pendingDS), "source DS must be dropped via legacy compaction TX")
		jd.dropDSListLock.RLock()
		require.Empty(t, jd.dropDSList, "non-completed compact paths must not touch dropDSList")
		jd.dropDSListLock.RUnlock()
		listIndices := lo.Map(jd.getDSListSnapshot(), func(ds dataSetT, _ int) string { return ds.Index })
		require.NotContains(t, listIndices, pendingDS.Index, "source DS must be removed from published list")
		require.Contains(t, listIndices, "1_1", "expecting a freshly compacted destination DS at index 1_1")
	})

	t.Run("flag on: mixed - completed goes async, pending uses legacy TX", func(t *testing.T) {
		jd, c := newJobDB(t)
		c.Set("JobsDB."+jd.tablePrefix+"."+"nonBlockingCompaction", false)
		c.Set("JobsDB."+jd.tablePrefix+"."+"nonBlockingCompletedDSDrop", true)
		// maxDSRetention=1ms lets both DSes appear together in compactFrom in a single call.
		c.Set("JobsDB."+jd.tablePrefix+"."+"maxDSRetention", "1ms")

		allJobs := genJobs(defaultWorkspaceID, "test", 20, 1)
		fillDS(t, jd, allJobs[:10], 0) // DS_1 completed
		createDS(t, jd, "2")
		fillDS(t, jd, allJobs[10:20], 5) // DS_2: 5 pending
		createDS(t, jd, "3")             // DS_3 empty last

		snapshot := jd.getDSListSnapshot()
		require.Len(t, snapshot, 3)
		completedDS := snapshot[0]
		pendingDS := snapshot[1]

		// Hold a reader so the async drop for the completed DS blocks; this lets us
		// observe the dropDSList entry before dropDSLoop drains it.
		_, _, release, err := jd.acquireDSListForRead(context.Background())
		require.NoError(t, err)
		defer release()

		require.NoError(t, jd.doCompaction(context.Background()))

		jd.dropDSListLock.RLock()
		require.Len(t, jd.dropDSList, 1, "only the completed DS should be enqueued for async drop")
		require.Equal(t, completedDS.Index, jd.dropDSList[0].ds.Index)
		jd.dropDSListLock.RUnlock()

		require.True(t, tableExists(t, jd, completedDS), "completed DS physical drop is blocked by the held reader")
		require.False(t, tableExists(t, jd, pendingDS), "pending DS source must be dropped via legacy compaction TX")
	})

	t.Run("flag toggled at runtime: takes effect on the next compaction", func(t *testing.T) {
		jd, c := newJobDB(t)
		c.Set("JobsDB."+jd.tablePrefix+"."+"nonBlockingCompaction", false)
		c.Set("JobsDB."+jd.tablePrefix+"."+"nonBlockingCompletedDSDrop", false)

		allJobs := genJobs(defaultWorkspaceID, "test", 20, 1)
		fillDS(t, jd, allJobs[:10], 0) // DS_1 completed
		createDS(t, jd, "2")           // DS_2 empty last
		completed1 := jd.getDSListSnapshot()[0]

		// Flag off (default): legacy in-tx drop.
		require.NoError(t, jd.doCompaction(context.Background()))
		require.False(t, tableExists(t, jd, completed1), "first compaction must use the legacy in-tx drop")
		jd.dropDSListLock.RLock()
		require.Empty(t, jd.dropDSList)
		jd.dropDSListLock.RUnlock()

		// Flip the flag and prepare another completed DS to compact.
		c.Set("JobsDB."+jd.tablePrefix+"."+"nonBlockingCompletedDSDrop", true)
		fillDS(t, jd, allJobs[10:20], 0) // DS_2 now has 10 succeeded jobs (completed)
		createDS(t, jd, "3")             // DS_3 empty last
		snapshot := jd.getDSListSnapshot()
		require.Len(t, snapshot, 2)
		completed2 := snapshot[0]
		require.Equal(t, "2", completed2.Index)

		// Hold a reader so we can observe the async enqueue before dropDSLoop drains it.
		_, _, release, err := jd.acquireDSListForRead(context.Background())
		require.NoError(t, err)
		defer release()

		require.NoError(t, jd.doCompaction(context.Background()))

		jd.dropDSListLock.RLock()
		require.Len(t, jd.dropDSList, 1, "second compaction must use the async drop path after the flag is on")
		require.Equal(t, completed2.Index, jd.dropDSList[0].ds.Index)
		jd.dropDSListLock.RUnlock()
		require.True(t, tableExists(t, jd, completed2), "physical drop blocked by held reader")
	})
}

func TestNonBlockingCompaction(t *testing.T) {
	_ = startPostgres(t)

	// newJobDB mirrors the pattern used by TestCompactionNonBlockingCompletedDSDrop:
	// the compaction and addNewDS loop triggers are wired to never-firing channels so
	// the tests drive state changes synchronously.
	newJobDB := func(t *testing.T) (*Handle, *config.Config) {
		t.Helper()
		c := config.New()
		c.Set("JobsDB.maxDSSize", 100000)
		jd := &Handle{
			TriggerAddNewDS:   func() <-chan time.Time { return make(chan time.Time) },
			TriggerCompaction: func() <-chan time.Time { return make(chan time.Time) },
			config:            c,
		}
		require.NoError(t, jd.Setup(ReadWrite, true, strings.ToLower(rand.String(5))))
		t.Cleanup(jd.TearDown)
		return jd, c
	}

	createDS := func(t *testing.T, jd *Handle, idx string) {
		t.Helper()
		require.NoError(t, jd.dsListLock.WithLockInCtx(context.Background(), func(l lock.LockToken) error {
			dsList, _ := jd.dsList.snapshot()
			if err := jd.WithTx(context.Background(), func(txn *tx.Tx) error {
				return jd.addNewDSInTx(context.Background(), txn, l, dsList, newDataSet(jd.tablePrefix, idx))
			}); err != nil {
				return err
			}
			return jd.doRefreshDSRangeList(l)
		}))
	}

	fillDS := func(t *testing.T, jd *Handle, jobs []*JobT, pending int) {
		t.Helper()
		require.NoError(t, jd.Store(context.Background(), jobs))
		if terminal := len(jobs) - pending; terminal > 0 {
			require.NoError(t, jd.UpdateJobStatus(context.Background(), genJobStatuses(jobs[:terminal], "executing")))
			require.NoError(t, jd.UpdateJobStatus(context.Background(), genJobStatuses(jobs[:terminal], "succeeded")))
		}
	}

	tableExists := func(t *testing.T, jd *Handle, ds dataSetT) bool {
		t.Helper()
		var exists bool
		require.NoError(t, jd.dbHandle.QueryRow(`SELECT to_regclass($1) IS NOT NULL`, ds.JobTable).Scan(&exists))
		return exists
	}

	hasReadonlyTrigger := func(t *testing.T, jd *Handle, ds dataSetT) bool {
		t.Helper()
		var present bool
		// Trigger name is created via an unquoted identifier so PostgreSQL
		// folds it to lower case in pg_trigger.tgname.
		require.NoError(t, jd.dbHandle.QueryRow(
			`SELECT EXISTS (
				SELECT 1 FROM pg_trigger t
				JOIN pg_class c ON c.oid = t.tgrelid
				WHERE c.relname = $1 AND lower(t.tgname) = 'readonlytabletrg'
			)`, ds.JobStatusTable).Scan(&present))
		return present
	}

	tableMarkedPreDrop := func(t *testing.T, jd *Handle, table string) bool {
		t.Helper()
		var marker sql.NullString
		require.NoError(t, jd.dbHandle.QueryRow(
			`SELECT obj_description(c.oid, 'pg_class')
			FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
			WHERE c.relname = $1 AND n.nspname NOT IN ('pg_catalog','information_schema')`,
			table).Scan(&marker))
		return marker.Valid && strings.HasPrefix(marker.String, "rudder:pre_drop:")
	}

	t.Run("flag off: legacy in-tx drop path is used", func(t *testing.T) {
		jd, c := newJobDB(t)
		c.Set("JobsDB."+jd.tablePrefix+"."+"nonBlockingCompaction", false)
		// maxDSRetention=1ms makes a DS with old terminal jobs eligible immediately.
		c.Set("JobsDB."+jd.tablePrefix+"."+"maxDSRetention", "1ms")

		jobs := genJobs(defaultWorkspaceID, "test", 10, 1)
		fillDS(t, jd, jobs, 5) // DS_1: 5 succeeded, 5 pending
		createDS(t, jd, "2")   // empty last DS

		snapshot := jd.getDSListSnapshot()
		require.Len(t, snapshot, 2)
		sourceDS := snapshot[0]

		require.NoError(t, jd.doCompaction(context.Background()))

		require.False(t, tableExists(t, jd, sourceDS), "legacy flow drops source in-TX")
		jd.dropDSListLock.RLock()
		require.Empty(t, jd.dropDSList, "legacy flow must not enqueue async drops")
		jd.dropDSListLock.RUnlock()
	})

	t.Run("flag on: source goes through async drop with pre-drop marker and readonly trigger", func(t *testing.T) {
		jd, c := newJobDB(t)
		c.Set("JobsDB."+jd.tablePrefix+"."+"nonBlockingCompaction", true)
		c.Set("JobsDB."+jd.tablePrefix+"."+"maxDSRetention", "1ms")

		jobs := genJobs(defaultWorkspaceID, "test", 10, 1)
		fillDS(t, jd, jobs, 5) // DS_1: 5 succeeded + 5 pending → copied to dest
		createDS(t, jd, "2")   // DS_2 empty last DS

		snapshot := jd.getDSListSnapshot()
		require.Len(t, snapshot, 2)
		sourceDS := snapshot[0]

		// Hold a reader against the current dsList version so dropDSLoop blocks on the
		// drain wait — lets us observe the async enqueue, marker, and trigger before
		// dropDSLoop physically drops.
		_, _, release, err := jd.acquireDSListForRead(context.Background())
		require.NoError(t, err)

		require.NoError(t, jd.doCompaction(context.Background()))

		// Source must be hidden from the published list and replaced with a dest.
		listIndices := lo.Map(jd.getDSListSnapshot(), func(ds dataSetT, _ int) string { return ds.Index })
		require.NotContains(t, listIndices, sourceDS.Index, "source DS hidden from new readers")
		require.Contains(t, listIndices, "1_1", "destination DS published")

		// Source still physically exists (drop is blocked by held reader).
		require.True(t, tableExists(t, jd, sourceDS), "physical drop must block until held reader releases")
		require.True(t, hasReadonlyTrigger(t, jd, sourceDS), "readonly trigger must be planted on the source status table")
		require.True(t, tableMarkedPreDrop(t, jd, sourceDS.JobTable), "source job table must carry pre-drop marker")
		require.True(t, tableMarkedPreDrop(t, jd, sourceDS.JobStatusTable), "source status table must carry pre-drop marker")

		jd.dropDSListLock.RLock()
		require.Len(t, jd.dropDSList, 1, "source DS must be enqueued for async drop")
		require.Equal(t, sourceDS.Index, jd.dropDSList[0].ds.Index)
		jd.dropDSListLock.RUnlock()

		// Pending jobs must have been copied to the dest.
		var destJobs int64
		require.NoError(t, jd.dbHandle.QueryRow(
			fmt.Sprintf(`SELECT COUNT(*) FROM %q`, jd.tablePrefix+"_jobs_1_1"),
		).Scan(&destJobs))
		require.EqualValues(t, 5, destJobs, "non-terminal jobs must be copied to dest")

		release()
		require.Eventually(t, func() bool {
			jd.dropDSListLock.RLock()
			pending := len(jd.dropDSList)
			jd.dropDSListLock.RUnlock()
			return !tableExists(t, jd, sourceDS) && pending == 0
		}, 5*time.Second, 50*time.Millisecond, "source table must be physically dropped once the reader releases")
	})

	t.Run("flag on: zero-copy compaction drops empty dest but still async-drops source", func(t *testing.T) {
		jd, c := newJobDB(t)
		c.Set("JobsDB."+jd.tablePrefix+"."+"nonBlockingCompaction", true)
		c.Set("JobsDB."+jd.tablePrefix+"."+"maxDSRetention", "1ms")

		jobs := genJobs(defaultWorkspaceID, "test", 10, 1)
		fillDS(t, jd, jobs, 5) // DS_1: 5 succeeded + 5 pending at optimistic check time
		createDS(t, jd, "2")   // DS_2 empty last DS

		snapshot := jd.getDSListSnapshot()
		require.Len(t, snapshot, 2)
		sourceDS := snapshot[0]
		destinationDS := newDataSet(jd.tablePrefix, "1_1")

		// Hold a reader so the source's async physical drop blocks long enough
		// for this test to inspect the pre-drop marker and dropDSList entry.
		_, _, releaseReader, err := jd.acquireDSListForRead(context.Background())
		require.NoError(t, err)
		readerReleased := false
		defer func() {
			if !readerReleased {
				releaseReader()
			}
		}()

		// Block compaction after getCompactionList's optimistic pending-count
		// decision but before the TX copies jobs. doCompactDS takes this shared
		// advisory lock only after computing compactionList.
		blocker, err := jd.dbHandle.BeginTx(context.Background(), nil)
		require.NoError(t, err)
		defer func() { _ = blocker.Rollback() }()
		_, err = blocker.ExecContext(
			context.Background(),
			`SELECT pg_advisory_xact_lock($1)`,
			jd.getAdvisoryLockForOperation("schema_migrate"),
		)
		require.NoError(t, err)

		done := make(chan error, 1)
		go func() {
			done <- jd.doCompaction(context.Background())
		}()

		require.Eventually(t, func() bool {
			select {
			case err := <-done:
				require.NoError(t, err)
				require.FailNow(t, "compaction finished before waiting on schema_migrate advisory lock")
			default:
			}
			var waiting bool
			require.NoError(t, jd.dbHandle.QueryRow(
				`SELECT EXISTS (
						SELECT 1 FROM pg_locks
						WHERE locktype = 'advisory'
							AND granted = false
					)`,
			).Scan(&waiting))
			return waiting
		}, 5*time.Second, 50*time.Millisecond, "compaction should block on schema_migrate advisory lock")

		// The optimistic check saw these as pending; before copy time they all
		// become terminal, so compactJobsInTx copies zero rows.
		require.NoError(t, jd.UpdateJobStatus(context.Background(), genJobStatuses(jobs[5:10], "succeeded")))

		require.NoError(t, blocker.Commit())
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			require.FailNow(t, "compaction did not finish after releasing schema_migrate advisory lock")
		}

		listIndices := lo.Map(jd.getDSListSnapshot(), func(ds dataSetT, _ int) string { return ds.Index })
		require.NotContains(t, listIndices, sourceDS.Index, "source DS hidden from new readers")
		require.NotContains(t, listIndices, destinationDS.Index, "zero-copy destination must not be published")
		require.False(t, tableExists(t, jd, destinationDS), "zero-copy destination must be dropped in the compaction TX")
		require.True(t, tableExists(t, jd, sourceDS), "source physical drop must block until held reader releases")
		require.True(t, tableMarkedPreDrop(t, jd, sourceDS.JobTable), "source job table must carry pre-drop marker")
		require.True(t, tableMarkedPreDrop(t, jd, sourceDS.JobStatusTable), "source status table must carry pre-drop marker")

		jd.dropDSListLock.RLock()
		require.Len(t, jd.dropDSList, 1, "source DS must still be enqueued for async drop")
		require.Equal(t, sourceDS.Index, jd.dropDSList[0].ds.Index)
		jd.dropDSListLock.RUnlock()

		releaseReader()
		readerReleased = true
		require.Eventually(t, func() bool {
			jd.dropDSListLock.RLock()
			pending := len(jd.dropDSList)
			jd.dropDSListLock.RUnlock()
			return !tableExists(t, jd, sourceDS) && pending == 0
		}, 5*time.Second, 50*time.Millisecond, "source table must be physically dropped once the reader releases")
	})

	t.Run("flag on: dsRangeList reflects dest's exact min/max", func(t *testing.T) {
		jd, c := newJobDB(t)
		c.Set("JobsDB."+jd.tablePrefix+"."+"nonBlockingCompaction", true)
		c.Set("JobsDB."+jd.tablePrefix+"."+"maxDSRetention", "1ms")

		// Store 20 jobs but mark the first 10 as terminal. Only the last 10
		// non-terminal jobs (ids 11..20) should land in dest, so the dest's
		// minJobID must be 11 — verifying we use the actual copied min/max
		// rather than the original source range.
		jobs := genJobs(defaultWorkspaceID, "test", 20, 1)
		fillDS(t, jd, jobs, 10) // 10 succeeded (ids 1..10), 10 pending (ids 11..20)
		createDS(t, jd, "2")    // empty last DS

		require.NoError(t, jd.doCompaction(context.Background()))

		_, ranges, release, err := jd.acquireDSListForRead(context.Background())
		require.NoError(t, err)
		defer release()

		var destRange *dataSetRangeT
		for i := range ranges {
			if ranges[i].ds.Index == "1_1" {
				destRange = &ranges[i]
				break
			}
		}
		require.NotNil(t, destRange, "expected a range entry for dest 1_1")
		require.EqualValues(t, 11, destRange.minJobID, "dest min must reflect first non-terminal job_id")
		require.EqualValues(t, 20, destRange.maxJobID, "dest max must reflect last copied job_id")
	})

	t.Run("flag on: published swap preserves non-compacted DSes in the current list", func(t *testing.T) {
		jd, c := newJobDB(t)
		c.Set("JobsDB."+jd.tablePrefix+"."+"nonBlockingCompaction", true)
		c.Set("JobsDB."+jd.tablePrefix+"."+"maxDSRetention", "1ms")
		// Disable needsPair so DS_2 (non-terminal only) is NOT eligible for compaction.
		// We need DS_2 non-empty so checkIfCompactDS's created_at scan doesn't trip on NULL.
		c.Set("JobsDB."+jd.tablePrefix+"."+"jobMinRowsLeftMigrateThreshold", 0.0)

		jobs := genJobs(defaultWorkspaceID, "test", 30, 1)
		fillDS(t, jd, jobs[:10], 5) // DS_1: 5 succeeded + 5 pending — eligible via retentionExpired
		createDS(t, jd, "2")
		fillDS(t, jd, jobs[10:20], 10) // DS_2: 10 non-terminal — ineligible with threshold=0
		createDS(t, jd, "3")           // empty last DS (exempt via idxCheck)

		require.NoError(t, jd.doCompaction(context.Background()))

		listIndices := lo.Map(jd.getDSListSnapshot(), func(ds dataSetT, _ int) string { return ds.Index })
		// Source DS_1 was compacted into DS_1_1. DS_2 and DS_3 were not in compactFrom
		// and must survive the rebased swap.
		require.Contains(t, listIndices, "1_1", "dest must be published")
		require.NotContains(t, listIndices, "1", "source DS_1 must be hidden from the published list")
		require.Contains(t, listIndices, "2", "non-compacted DS_2 must be preserved")
		require.Contains(t, listIndices, "3", "non-compacted DS_3 must be preserved")
	})

	t.Run("flag on: completed-only sources skip the compaction TX and go straight to async drop", func(t *testing.T) {
		jd, c := newJobDB(t)
		c.Set("JobsDB."+jd.tablePrefix+"."+"nonBlockingCompaction", true)
		// No maxDSRetention needed: a DS with all-terminal jobs is naturally eligible.

		jobs := genJobs(defaultWorkspaceID, "test", 10, 1)
		fillDS(t, jd, jobs, 0) // DS_1 fully completed
		createDS(t, jd, "2")   // empty last DS

		snapshot := jd.getDSListSnapshot()
		require.Len(t, snapshot, 2)
		completedDS := snapshot[0]

		// Hold a reader to observe the async enqueue before dropDSLoop drains it.
		_, _, release, err := jd.acquireDSListForRead(context.Background())
		require.NoError(t, err)

		require.NoError(t, jd.doCompaction(context.Background()))

		listIndices := lo.Map(jd.getDSListSnapshot(), func(ds dataSetT, _ int) string { return ds.Index })
		require.NotContains(t, listIndices, completedDS.Index, "completed source hidden from list")
		require.NotContains(t, listIndices, "1_1", "no destination DS should be created when all sources are completed")
		jd.dropDSListLock.RLock()
		require.Len(t, jd.dropDSList, 1)
		require.Equal(t, completedDS.Index, jd.dropDSList[0].ds.Index)
		jd.dropDSListLock.RUnlock()
		require.True(t, tableExists(t, jd, completedDS), "drop blocked by held reader")

		release()
		require.Eventually(t, func() bool {
			jd.dropDSListLock.RLock()
			pending := len(jd.dropDSList)
			jd.dropDSListLock.RUnlock()
			return !tableExists(t, jd, completedDS) && pending == 0
		}, 5*time.Second, 50*time.Millisecond)
	})

	t.Run("flag on: source status table rejects post-commit inserts with RS001", func(t *testing.T) {
		jd, c := newJobDB(t)
		c.Set("JobsDB."+jd.tablePrefix+"."+"nonBlockingCompaction", true)
		c.Set("JobsDB."+jd.tablePrefix+"."+"maxDSRetention", "1ms")

		jobs := genJobs(defaultWorkspaceID, "test", 10, 1)
		fillDS(t, jd, jobs, 5)
		createDS(t, jd, "2")
		snapshot := jd.getDSListSnapshot()
		sourceDS := snapshot[0]

		// Hold a reader so the async drop blocks — the source status table must
		// remain around (with its planted trigger) so we can probe it directly.
		_, _, release, err := jd.acquireDSListForRead(context.Background())
		require.NoError(t, err)
		defer release()

		require.NoError(t, jd.doCompaction(context.Background()))

		// Direct INSERT into the (compacted-away) source status table must trip the
		// readonly trigger and raise RS001 — proof the trigger fences late writers.
		_, err = jd.dbHandle.Exec(fmt.Sprintf(
			`INSERT INTO %q (job_id, job_state, attempt, exec_time, retry_time, error_code, error_response, parameters)
			VALUES (1, 'executing', 0, NOW(), NOW(), '', '{}', '{}')`,
			sourceDS.JobStatusTable))
		require.Error(t, err, "insert on a compacted source status table must be rejected")
		var pqErr *pq.Error
		require.ErrorAs(t, err, &pqErr)
		require.Equal(t, pgErrorCodeTableReadonly, string(pqErr.Code))

		// Also assert that the regular UpdateJobStatus path (now using the refreshed
		// dsRangeList) routes status updates to the dest — verifying end-to-end
		// correctness for the non-blocking flow.
		require.NoError(t,
			jd.UpdateJobStatus(context.Background(), genJobStatuses(jobs[5:10], "executing")),
		)
		var destStatusCount int64
		require.NoError(t, jd.dbHandle.QueryRow(
			fmt.Sprintf(`SELECT COUNT(*) FROM %q WHERE job_state = 'executing'`, jd.tablePrefix+"_job_status_1_1"),
		).Scan(&destStatusCount))
		require.EqualValues(t, 5, destStatusCount, "status updates must land in dest after compaction")
	})

	t.Run("flag on: pre-drop marker survives a crash and cleanupPreDropTables drops the source on next startup", func(t *testing.T) {
		jd, c := newJobDB(t)
		c.Set("JobsDB."+jd.tablePrefix+"."+"nonBlockingCompaction", true)
		c.Set("JobsDB."+jd.tablePrefix+"."+"maxDSRetention", "1ms")

		jobs := genJobs(defaultWorkspaceID, "test", 10, 1)
		fillDS(t, jd, jobs, 5)
		createDS(t, jd, "2")
		snapshot := jd.getDSListSnapshot()
		sourceDS := snapshot[0]

		// Hold a reader so the async drop cannot run — emulates "process crashed
		// before dropDSLoop got to it." The pre_drop marker is now on disk.
		_, _, release, err := jd.acquireDSListForRead(context.Background())
		require.NoError(t, err)

		require.NoError(t, jd.doCompaction(context.Background()))
		require.True(t, tableExists(t, jd, sourceDS), "drop blocked by held reader (simulates crash before drop)")
		require.True(t, tableMarkedPreDrop(t, jd, sourceDS.JobTable))

		// Call cleanupPreDropTables directly — this is what Start invokes at startup.
		release()
		require.NoError(t, jd.cleanupPreDropTables(context.Background()))
		require.False(t, tableExists(t, jd, sourceDS), "cleanupPreDropTables must drop the marked source table")
	})

	t.Run("flag toggled at runtime: takes effect on the next compaction", func(t *testing.T) {
		jd, c := newJobDB(t)
		c.Set("JobsDB."+jd.tablePrefix+"."+"nonBlockingCompaction", false)
		c.Set("JobsDB."+jd.tablePrefix+"."+"maxDSRetention", "1ms")
		// Disable needsPair so the first compaction's dest DS_1_1 isn't pulled back
		// into the second iteration's compactFrom (only the freshly-filled DS_2 is).
		c.Set("JobsDB."+jd.tablePrefix+"."+"jobMinRowsLeftMigrateThreshold", 0.0)

		// Use a single genJobs slice so the local JobID values match the DB-assigned
		// IDs across both batches (the sequence continues from DS_1's max).
		allJobs := genJobs(defaultWorkspaceID, "test", 20, 1)

		// First compaction with flag OFF → legacy in-TX drop.
		fillDS(t, jd, allJobs[:10], 5)
		createDS(t, jd, "2")
		source1 := jd.getDSListSnapshot()[0]
		require.NoError(t, jd.doCompaction(context.Background()))
		require.False(t, tableExists(t, jd, source1), "first compaction uses legacy in-TX drop")
		jd.dropDSListLock.RLock()
		require.Empty(t, jd.dropDSList)
		jd.dropDSListLock.RUnlock()

		// Flip flag ON; fill the (now last) DS_2 with another batch and add DS_3.
		c.Set("JobsDB."+jd.tablePrefix+"."+"nonBlockingCompaction", true)
		fillDS(t, jd, allJobs[10:20], 5)
		createDS(t, jd, "3")
		snapshot := jd.getDSListSnapshot()
		var source2 dataSetT
		for _, ds := range snapshot {
			if ds.Index == "2" {
				source2 = ds
				break
			}
		}
		require.NotEmpty(t, source2.Index)

		_, _, release, err := jd.acquireDSListForRead(context.Background())
		require.NoError(t, err)
		defer release()

		require.NoError(t, jd.doCompaction(context.Background()))
		jd.dropDSListLock.RLock()
		require.NotEmpty(t, jd.dropDSList, "second compaction must use the async drop path")
		jd.dropDSListLock.RUnlock()
		require.True(t, tableExists(t, jd, source2), "drop blocked by held reader")
		require.True(t, hasReadonlyTrigger(t, jd, source2), "source must carry readonly trigger after non-blocking compaction")
	})

	t.Run("flag on + deferStatusLock: copies pending jobs and their latest statuses, fences source", func(t *testing.T) {
		jd, c := newJobDB(t)
		c.Set("JobsDB."+jd.tablePrefix+"."+"nonBlockingCompaction", true)
		c.Set("JobsDB."+jd.tablePrefix+"."+"compactionDeferStatusLock", true)
		c.Set("JobsDB."+jd.tablePrefix+"."+"maxDSRetention", "1ms")

		// jobs 1..4 terminal (succeeded), 5..7 non-terminal (failed), 8..10 never
		// picked up (no status at all).
		jobs := genJobs(defaultWorkspaceID, "test", 10, 1)
		require.NoError(t, jd.Store(context.Background(), jobs))
		require.NoError(t, jd.UpdateJobStatus(context.Background(), genJobStatuses(jobs[:4], "succeeded")))
		require.NoError(t, jd.UpdateJobStatus(context.Background(), genJobStatuses(jobs[4:7], "failed")))
		createDS(t, jd, "2") // empty last DS

		snapshot := jd.getDSListSnapshot()
		require.Len(t, snapshot, 2)
		sourceDS := snapshot[0]

		// Hold a reader so the source's async physical drop blocks long enough to
		// inspect the trigger and pre-drop markers.
		_, _, release, err := jd.acquireDSListForRead(context.Background())
		require.NoError(t, err)

		require.NoError(t, jd.doCompaction(context.Background()))

		listIndices := lo.Map(jd.getDSListSnapshot(), func(ds dataSetT, _ int) string { return ds.Index })
		require.NotContains(t, listIndices, sourceDS.Index, "source DS hidden from new readers")
		require.Contains(t, listIndices, "1_1", "destination DS published")

		// The status table must still be fenced even though it was locked in a
		// separate phase from the jobs copy.
		require.True(t, hasReadonlyTrigger(t, jd, sourceDS), "deferred mode must still plant the readonly trigger on the source status table")
		require.True(t, tableMarkedPreDrop(t, jd, sourceDS.JobTable), "source job table must carry pre-drop marker")
		require.True(t, tableMarkedPreDrop(t, jd, sourceDS.JobStatusTable), "source status table must carry pre-drop marker")

		destJobsTable := jd.tablePrefix + "_jobs_1_1"
		destStatusTable := jd.tablePrefix + "_job_status_1_1"

		// Phase 1: the 6 pending jobs (ids 5..10) are copied; the 4 terminal jobs are not.
		var copiedJobIDs string
		require.NoError(t, jd.dbHandle.QueryRow(
			fmt.Sprintf(`SELECT COALESCE(string_agg(job_id::text, ',' ORDER BY job_id), '') FROM %q`, destJobsTable),
		).Scan(&copiedJobIDs))
		require.Equal(t, "5,6,7,8,9,10", copiedJobIDs, "only pending jobs must be copied")

		// Phase 2: only jobs that had a status (5,6,7) get a status row, and it is
		// their latest status ("failed"). Jobs 8..10 were never picked up so they
		// correctly carry no status and remain unprocessed in the destination.
		var statusJobIDs, distinctStates string
		require.NoError(t, jd.dbHandle.QueryRow(
			fmt.Sprintf(`SELECT COALESCE(string_agg(job_id::text, ',' ORDER BY job_id), ''), COALESCE(string_agg(DISTINCT job_state, ','), '') FROM %q`, destStatusTable),
		).Scan(&statusJobIDs, &distinctStates))
		require.Equal(t, "5,6,7", statusJobIDs, "latest status copied only for moved jobs that had one")
		require.Equal(t, "failed", distinctStates, "the latest (non-terminal) status must be preserved")

		release()
		require.Eventually(t, func() bool {
			jd.dropDSListLock.RLock()
			pending := len(jd.dropDSList)
			jd.dropDSListLock.RUnlock()
			return !tableExists(t, jd, sourceDS) && pending == 0
		}, 5*time.Second, 50*time.Millisecond, "source table must be physically dropped once the reader releases")
	})
}
