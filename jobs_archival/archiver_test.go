package jobs_archival

import (
	"bufio"
	"compress/gzip"
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/jobsdb/prebackup"

	arch "github.com/rudderlabs/rudder-server/services/archiver"
	"github.com/rudderlabs/rudder-server/services/fileuploader"
	"github.com/rudderlabs/rudder-server/testhelper"
	"github.com/rudderlabs/rudder-server/testhelper/destination"
)

func TestJobsArchival(t *testing.T) {
	var (
		// tc                       backupTestCase
		prefix                 = "some-prefix"
		minioResource          []*destination.MINIOResource
		goldenFileJobsFileName = "testdata/MultiWorkspaceBackupJobs.json.gz"
		// goldenFileStatusFileName = "testdata/MultiWorkspaceBackupStatus.json.gz"
		uniqueWorkspaces = 3
		ctx, cancel      = context.WithCancel(context.Background())
	)
	defer cancel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err, "Failed to create docker pool")
	cleanup := &testhelper.Cleanup{}
	// defer cleanup.Run()

	postgresResource, err := resource.SetupPostgres(pool, cleanup)
	require.NoError(t, err)

	minioResource = make([]*destination.MINIOResource, uniqueWorkspaces)
	for i := 0; i < uniqueWorkspaces; i++ {
		minioResource[i], err = destination.SetupMINIO(pool, cleanup)
		require.NoError(t, err)
	}

	// create a unique temporary directory to allow for parallel test execution
	// tmpDir := t.TempDir()

	jobs, err := readGzipJobFile(goldenFileJobsFileName)
	require.NoError(t, err)

	{
		t.Setenv("MINIO_SSL", "false")

		t.Setenv("JOBS_DB_DB_NAME", postgresResource.Database)
		t.Setenv("JOBS_DB_NAME", postgresResource.Database)
		t.Setenv("JOBS_DB_HOST", postgresResource.Host)
		t.Setenv("JOBS_DB_PORT", postgresResource.Port)
		t.Setenv("JOBS_DB_USER", postgresResource.User)
		t.Setenv("JOBS_DB_PASSWORD", postgresResource.Password)
	}

	arch.Init()
	jobsdb.Init2()
	jd := &jobsdb.HandleT{}
	require.NoError(t, jd.Setup(
		jobsdb.ReadWrite,
		false,
		"gw",
		[]prebackup.Handler{},
		nil,
	))
	require.NoError(t, jd.Start())
	defer jd.Close()

	require.NoError(t, jd.Store(ctx, jobs))

	storageSettings := map[string]fileuploader.StorageSettings{
		"defaultWorkspaceID-1": {
			Bucket: backendconfig.StorageBucket{
				Type: "MINIO",
				Config: map[string]interface{}{
					"bucketName":      minioResource[0].BucketName,
					"prefix":          prefix,
					"endPoint":        minioResource[0].Endpoint,
					"accessKeyID":     minioResource[0].AccessKey,
					"secretAccessKey": minioResource[0].SecretKey,
				},
			},
			Preferences: backendconfig.StoragePreferences{
				GatewayDumps:     false,
				BatchRouterDumps: true,
				RouterDumps:      true,
				ProcErrorDumps:   true,
			},
		},
		"defaultWorkspaceID-2": {
			Bucket: backendconfig.StorageBucket{
				Type: "MINIO",
				Config: map[string]interface{}{
					"bucketName":      minioResource[1].BucketName,
					"prefix":          prefix,
					"endPoint":        minioResource[1].Endpoint,
					"accessKeyID":     minioResource[1].AccessKey,
					"secretAccessKey": minioResource[1].SecretKey,
				},
			},
			Preferences: backendconfig.StoragePreferences{
				GatewayDumps:     true,
				BatchRouterDumps: true,
				RouterDumps:      true,
				ProcErrorDumps:   false,
			},
		},
		"defaultWorkspaceID-3": {
			Bucket: backendconfig.StorageBucket{
				Type: "MINIO",
				Config: map[string]interface{}{
					"bucketName":      minioResource[2].BucketName,
					"prefix":          prefix,
					"endPoint":        minioResource[2].Endpoint,
					"accessKeyID":     minioResource[2].AccessKey,
					"secretAccessKey": minioResource[2].SecretKey,
				},
			},
			Preferences: backendconfig.StoragePreferences{
				GatewayDumps:     true,
				BatchRouterDumps: true,
				RouterDumps:      true,
				ProcErrorDumps:   true,
			},
		},
	}
	// fileuploaderProvider := fileuploader.NewStaticProvider(storageSettings)
	fileUploaderProvider := fileuploader.NewStaticProvider(storageSettings)
	trigger := make(chan time.Time)
	archiver := New(
		jd, fileUploaderProvider,
		WithArchiveTrigger(func() <-chan time.Time {
			return trigger
		},
		),
	)

	require.NoError(t, archiver.Start())
	defer archiver.Stop()

	trigger <- time.Now()
	trigger <- time.Now()

	succeeded, err := jd.GetProcessed(
		ctx,
		jobsdb.GetQueryParamsT{
			StateFilters: []string{jobsdb.Succeeded.State},
		},
	)
	require.NoError(t, err)
	require.Equal(t, len(jobs), len(succeeded.Jobs))

}

func readGzipJobFile(filename string) ([]*jobsdb.JobT, error) {
	file, err := os.Open(filename)
	if err != nil {
		return []*jobsdb.JobT{}, err
	}
	defer func() { _ = file.Close() }()

	gz, err := gzip.NewReader(file)
	if err != nil {
		return []*jobsdb.JobT{}, err
	}
	defer gz.Close()

	sc := bufio.NewScanner(gz)
	// default scanner buffer maxCapacity is 64K
	// set it to higher value to avoid read stop on read size error
	maxCapacity := 10240 * 1024 // 10MB
	buf := make([]byte, maxCapacity)
	sc.Buffer(buf, maxCapacity)

	var jobs []*jobsdb.JobT
	for sc.Scan() {
		lineByte := sc.Bytes()
		uuid := uuid.MustParse("69359037-9599-48e7-b8f2-48393c019135")
		j := &jobsdb.JobT{
			UUID:         uuid,
			JobID:        gjson.GetBytes(lineByte, "job_id").Int(),
			UserID:       gjson.GetBytes(lineByte, "user_id").String(),
			CustomVal:    gjson.GetBytes(lineByte, "custom_val").String(),
			Parameters:   []byte(gjson.GetBytes(lineByte, "parameters").String()),
			EventCount:   int(gjson.GetBytes(lineByte, "event_count").Int()),
			WorkspaceId:  gjson.GetBytes(lineByte, "workspace_id").String(),
			EventPayload: []byte(gjson.GetBytes(lineByte, "event_payload").String()),
		}
		jobs = append(jobs, j)
	}
	return jobs, nil
}
