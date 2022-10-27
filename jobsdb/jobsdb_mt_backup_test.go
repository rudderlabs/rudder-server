package jobsdb

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/ory/dockertest/v3"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/services/backup"
	"github.com/rudderlabs/rudder-server/testhelper"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/testhelper/destination"
	rsRand "github.com/rudderlabs/rudder-server/testhelper/rand"
)

func TestMTBackup(t *testing.T) {
	prefix := strings.ToLower(rsRand.String(5))
	defaultWorkers := 1

	pool, err := dockertest.NewPool("")
	require.NoError(t, err, "Failed to create docker pool")
	cleanup := &testhelper.Cleanup{}
	defer cleanup.Run()
	
	minioResource, err := destination.SetupMINIO(pool, cleanup)
	require.NoError(t, err)

	t.Setenv("JOBS_BACKUP_BUCKET", "XYZ")
	t.Setenv("MULTITENANT_JOBS_BACKUP_WORKERS", strconv.Itoa(defaultWorkers))

	t.Setenv("MINIO_ENDPOINT", minioResource.Endpoint)
	t.Setenv("MINIO_ACCESS_KEY_ID", minioResource.AccessKey)
	t.Setenv("MINIO_SECRET_ACCESS_KEY", minioResource.SecretKey)
	t.Setenv("MINIO_SSL", "false")

	postgresql := startPostgres(t)
	dbHandle := postgresql.DB
	jobsdb := &HandleT{
		tablePrefix:     prefix,
		dbHandle:        dbHandle,
		storageSettings: backup.StorageSettings{
			StorageBucket: backup.StorageBucket{
				"w-1": backendconfig.StorageBucket{
					Type: "MINIO",
					Config: map[string]interface{}{
						"bucketName":      minioResource.BucketName,
						"prefix":          prefix,
						"endPoint":        minioResource.Endpoint,
						"accessKeyID":     minioResource.AccessKey,
						"secretAccessKey": minioResource.SecretKey,
						"useSSL":          false,
					},
				},
				"w-2": backendconfig.StorageBucket{
					Type: "MINIO",
					Config: map[string]interface{}{
						"bucketName":      minioResource.BucketName,
						"prefix":          prefix,
						"endPoint":        minioResource.Endpoint,
						"accessKeyID":     minioResource.AccessKey,
						"secretAccessKey": minioResource.SecretKey,
						"useSSL":          false,
					},
				},
				"w-3": backendconfig.StorageBucket{
					Type: "MINIO",
					Config: map[string]interface{}{
						"bucketName":      minioResource.BucketName,
						"prefix":          prefix,
						"endPoint":        minioResource.Endpoint,
						"accessKeyID":     minioResource.AccessKey,
						"secretAccessKey": minioResource.SecretKey,
						"useSSL":          false,
					},
				},
			},
		},
		logger:          logger.NewLogger().Child("jobsdb"),
		BackupSettings:  &backupSettings{},
	}
	var (
		jobsTable      = "pre_drop_" + prefix + "_jobs_0"
		jobStatusTable = "pre_drop_" + prefix + "_job_status_0"
	)

	jobsdb.registerBackUpSettings()

	jobsdb.BackupSettings.instanceBackupEnabled = true

	// And I have jobs and job status tables with events from 3 sources
	createTables(t, dbHandle, jobsTable, jobStatusTable)
	addJobForWorkspace(t, dbHandle, "w-1", "succeeded", jobsTable, jobStatusTable)
	addJobForWorkspace(t, dbHandle, "w-2", "failed", jobsTable, jobStatusTable)
	addJobForWorkspace(t, dbHandle, "w-3", "aborted", jobsTable, jobStatusTable)

	requireRowsCount(t, dbHandle, jobsTable, 3)
	requireRowsCount(t, dbHandle, jobStatusTable, 3)

	backupDSRange := dataSetRangeT{
		ds: dataSetT{
			JobTable: 	jobsTable,
			JobStatusTable: jobStatusTable,
			Index: "0",
		},
	}
	require.Equal(t, 0, len(jobsdb.backupWorkers))
	err = jobsdb.backupDS(context.Background(), &backupDSRange)
	require.Equal(t, defaultWorkers, len(jobsdb.backupWorkers)) // backup workers are back to default
	require.NoError(t, err)
}

func addJobForWorkspace(t *testing.T, db *sql.DB, workspaceId, state, jobsTable, jobStatusTable string) {
	txn, err := db.Begin()
	require.NoError(t, err)

	sqlStatement := fmt.Sprintf(`INSERT INTO "%s" (workspace_id, uuid, user_id, parameters, custom_val, event_payload, event_count) VALUES(
		'%s',
		'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11',
		'uder_id',
		'{ "source_id": "some-source" }',
		'custom_val',
		'{}',
		1);`, jobsTable, workspaceId)
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
