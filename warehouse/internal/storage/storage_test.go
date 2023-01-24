package storage_test

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"testing"

	"github.com/ory/dockertest/v3"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/internal/storage"
	"github.com/stretchr/testify/require"
)

// storageConfig["bucketName"] = config.GetString("RUDDER_WAREHOUSE_BUCKET", "rudder-warehouse-storage")
// storageConfig["accessKeyID"] = config.GetString("RUDDER_AWS_S3_COPY_USER_ACCESS_KEY_ID", "")
// storageConfig["accessKey"] = config.GetString("RUDDER_AWS_S3_COPY_USER_ACCESS_KEY", "")
// storageConfig["enableSSE"] = config.GetBool("RUDDER_WAREHOUSE_BUCKET_SSE", true)

func TestStorage(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	bucket := "filemanager-test-1"
	region := "us-east-1"
	accessKeyId := "MYACCESSKEY"
	secretAccessKey := "MYSECRETKEY"

	// running minio container on docker
	minioResource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "minio/minio",
		Tag:        "latest",
		Cmd:        []string{"server", "/data"},
		Env: []string{
			fmt.Sprintf("MINIO_ACCESS_KEY=%s", accessKeyId),
			fmt.Sprintf("MINIO_SECRET_KEY=%s", secretAccessKey),
			fmt.Sprintf("MINIO_SITE_REGION=%s", region),
		},
	})
	require.NoError(t, err)
	defer func() {
		if err := pool.Purge(minioResource); err != nil {
			log.Printf("Could not purge resource: %s \n", err)
		}
	}()

	minioEndpoint := fmt.Sprintf("localhost:%s", minioResource.GetPort("9000/tcp"))
	t.Setenv("RUDDER_TMPDIR", "./testdata/tmp")

	config := map[string]interface{}{
		"bucketProvider":   "MINIO",
		"bucketName":       bucket,
		"accessKeyID":      accessKeyId,
		"secretAccessKey":  secretAccessKey,
		"useSSL":           false,
		"endPoint":         minioEndpoint,
		"useRudderStorage": false,
	}

	storage := &storage.Storage{
		FileManagerFactory: filemanager.DefaultFileManagerFactory,
		Destination: backendconfig.DestinationT{
			Config: config,
		},
	}

	stagingFile := model.StagingFile{
		Location: "staging_file.json",
	}

	ctx := context.Background()

	ogFile, err := os.Open("./testdata/staging_file.json")
	require.NoError(t, err)
	defer ogFile.Close()

	err = storage.UploadStagingFile(ctx, &stagingFile, ogFile)
	require.NoError(t, err)

	dlFile, err := storage.OpenStagingFile(ctx, stagingFile)
	require.NoError(t, err)
	defer dlFile.Close()

	dlData, err := io.ReadAll(dlFile)
	require.NoError(t, err)

	ogData, err := os.ReadFile(ogFile.Name())
	require.NoError(t, err)

	require.Equal(t, ogData, dlData)

	require.NoError(t, os.Remove(dlFile.Name()))

}
