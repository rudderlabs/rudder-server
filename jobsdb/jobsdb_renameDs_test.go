package jobsdb

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/jobsdb/prebackup"
	rsRand "github.com/rudderlabs/rudder-server/testhelper/rand"
)

func Test_mustRenameDS(t *testing.T) {
	prefix := strings.ToLower(rsRand.String(5))
	postgresql := startPostgres(t)
	// Given I have a jobsdb with dropSourceIds prebackup handler for 2 sources
	dbHandle := postgresql.DB
	jobsdb := &HandleT{
		tablePrefix: prefix,
		dbHandle:    dbHandle,
		preBackupHandlers: []prebackup.Handler{
			prebackup.DropSourceIds(func() []string { return []string{"one", "two"} }),
		},
	}
	var (
		jobsTable      = prefix + "_jobs"
		jobStatusTable = prefix + "job_status"
	)

	// And I have jobs and job status tables with events from 3 sources
	createTables(t, dbHandle, jobsTable, jobStatusTable)
	addJob(t, dbHandle, "one", "succeeded", jobsTable, jobStatusTable)
	addJob(t, dbHandle, "two", "failed", jobsTable, jobStatusTable)
	addJob(t, dbHandle, "three", "aborted", jobsTable, jobStatusTable)

	requireRowsCount(t, dbHandle, jobsTable, 3)
	requireRowsCount(t, dbHandle, jobStatusTable, 3)

	// when I execute the renameDs method
	err := jobsdb.mustRenameDS(dataSetT{
		JobTable:       jobsTable,
		JobStatusTable: jobStatusTable,
	})
	require.NoError(t, err)

	// then I end up with one event on each pre_drop table
	requireRowsCount(t, dbHandle, fmt.Sprintf("%s%s", preDropTablePrefix, jobsTable), 1)
	requireRowsCount(t, dbHandle, fmt.Sprintf("%s%s", preDropTablePrefix, jobStatusTable), 1)
}

func Test_mustRenameDS_drops_table_if_left_empty(t *testing.T) {
	prefix := strings.ToLower(rsRand.String(5))
	postgresql := startPostgres(t)

	dbHandle := postgresql.DB

	// Given I have a jobsdb with dropSourceIds prebackup handler for 2 sources
	jobsdb := &HandleT{
		tablePrefix: prefix,
		dbHandle:    dbHandle,
		preBackupHandlers: []prebackup.Handler{
			prebackup.DropSourceIds(func() []string { return []string{"one", "two"} }),
		},
	}
	var (
		jobsTable      = prefix + "_jobs"
		jobStatusTable = prefix + "job_status"
	)

	// And I have jobs and job status tables with events from 2 sources
	createTables(t, dbHandle, jobsTable, jobStatusTable)
	addJob(t, dbHandle, "one", "succeeded", jobsTable, jobStatusTable)
	addJob(t, dbHandle, "two", "failed", jobsTable, jobStatusTable)

	requireRowsCount(t, dbHandle, jobsTable, 2)
	requireRowsCount(t, dbHandle, jobStatusTable, 2)

	// when I execute the renameDs method
	err := jobsdb.mustRenameDS(dataSetT{
		JobTable:       jobsTable,
		JobStatusTable: jobStatusTable,
	})
	require.NoError(t, err)

	// then I end up with no pre_drop tables
	requireTableNotExists(t, dbHandle, fmt.Sprintf("%s%s", preDropTablePrefix, jobsTable))
	requireTableNotExists(t, dbHandle, fmt.Sprintf("%s%s", preDropTablePrefix, jobStatusTable))
	requireTableNotExists(t, dbHandle, jobsTable)
	requireTableNotExists(t, dbHandle, jobStatusTable)
}

func createTables(t *testing.T, db *sql.DB, jobsTable, jobStatusTable string) {
	txn, err := db.Begin()
	require.NoError(t, err)

	sqlStatement := fmt.Sprintf(`CREATE TABLE "%s" (
		job_id BIGSERIAL PRIMARY KEY,
		workspace_id TEXT NOT NULL DEFAULT '',
		uuid UUID NOT NULL,
		user_id TEXT NOT NULL,
		parameters JSONB NOT NULL,
		custom_val VARCHAR(64) NOT NULL,
		event_payload JSONB NOT NULL,
		event_count INTEGER NOT NULL DEFAULT 1,
		created_at TIMESTAMP NOT NULL DEFAULT NOW(),
		expire_at TIMESTAMP NOT NULL DEFAULT NOW());`, jobsTable)
	_, err = txn.Exec(sqlStatement)
	if err != nil {
		t.Fatalf("Failed to create jobs table: %s", err)
	}

	sqlStatement = fmt.Sprintf(`CREATE TABLE "%s" (
			id BIGSERIAL,
			job_id BIGINT REFERENCES "%s"(job_id),
			job_state VARCHAR(64),
			attempt SMALLINT,
			exec_time TIMESTAMP,
			retry_time TIMESTAMP,
			error_code VARCHAR(32),
			error_response JSONB DEFAULT '{}'::JSONB,
			parameters JSONB DEFAULT '{}'::JSONB,
			PRIMARY KEY (job_id, job_state, id));`, jobStatusTable, jobsTable)
	_, err = txn.Exec(sqlStatement)
	if err != nil {
		t.Fatalf("Failed to create job status table: %s", err)
	}
	require.NoError(t, txn.Commit())
}

func addJob(t *testing.T, db *sql.DB, sourceId, state, jobsTable, jobStatusTable string) {
	txn, err := db.Begin()
	require.NoError(t, err)

	sqlStatement := fmt.Sprintf(`INSERT INTO "%s" (workspace_id, uuid, user_id, parameters, custom_val, event_payload, event_count) VALUES(
		'workspace_id',
		'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11',
		'uder_id',
		'{ "source_id": "%s" }',
		'custom_val',
		'{}',
		1);`, jobsTable, sourceId)
	_, err = txn.Exec(sqlStatement)
	if err != nil {
		t.Fatalf("failed to insert job entry: %s", err)
	}
	sqlStatement = fmt.Sprintf(`INSERT INTO "%s" (job_id, job_state, attempt) VALUES(
		(SELECT max(job_id) FROM "%s") ,
		'%s',
		1);`, jobStatusTable, jobsTable, state)
	_, err = txn.Exec(sqlStatement)
	if err != nil {
		t.Fatalf("failed to insert job status entry: %s", err)
	}
	require.NoError(t, txn.Commit())
}

func requireRowsCount(t *testing.T, db *sql.DB, tableName string, expectedCount int) {
	t.Helper()
	rows := db.QueryRow(fmt.Sprintf("SELECT COUNT(*) as count FROM %s", tableName))
	var count int
	require.NoError(t, rows.Scan(&count))
	require.EqualValues(t, expectedCount, count)
}

func requireTableNotExists(t *testing.T, db *sql.DB, tableName string) {
	t.Helper()
	stmt, err := db.Prepare("SELECT count(tablename) FROM pg_catalog.pg_tables where tablename = $1")
	require.NoError(t, err)
	defer stmt.Close()
	row := stmt.QueryRow(tableName)
	var count int
	require.NoError(t, row.Scan(&count))
	require.EqualValues(t, 0, count)
}
