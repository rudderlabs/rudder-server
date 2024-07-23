package downloader_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"

	"go.uber.org/mock/gomock"

	"github.com/google/uuid"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/minio"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/utils/misc"
	mockuploader "github.com/rudderlabs/rudder-server/warehouse/internal/mocks/utils"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/internal/service/loadfiles/downloader"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestDownloader(t *testing.T) {
	t.Parallel()

	misc.Init()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	var (
		destType      = "POSTGRES"
		provider      = "MINIO"
		workers       = 12
		workspaceID   = "test-workspace-id"
		destinationID = "test-destination-id"
		table         = "test-table"
	)

	ctxCancel, cancel := context.WithCancel(context.Background())
	cancel()

	testCases := []struct {
		name         string
		conf         map[string]interface{}
		numLoadFiles int
		wantError    error
		loadFiles    []warehouseutils.LoadFile
		ctx          context.Context
	}{
		{
			name:         "many load files",
			numLoadFiles: 51,
		},
		{
			name:         "invalid bucket provider",
			numLoadFiles: 1,
			conf: map[string]interface{}{
				"bucketProvider": "INVALID",
			},
			wantError: errors.New("creating filemanager for destination: service provider not supported: INVALID"),
		},
		{
			name:         "invalid load file",
			numLoadFiles: 1,
			loadFiles: []warehouseutils.LoadFile{
				{
					Location: "http://localhost:56524/testbucket/cc0e3daa-1356-4781-8ce1-549ff95a8313/random.csv.gz",
				},
			},
			wantError: errors.New("downloading batch: downloading object: downloading file from object storage: The specified key does not exist."),
		},
		{
			name:         "context cancelled",
			numLoadFiles: 11,
			ctx:          ctxCancel,
			wantError:    errors.New("downloading batch: downloading object: downloading file from object storage"),
		},
	}

	for i, tc := range testCases {
		i := i
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			minioResource, err := minio.Setup(pool, t)
			require.NoError(t, err)

			t.Log("minio:", minioResource.Endpoint)

			conf := map[string]any{
				"bucketName":       minioResource.BucketName,
				"accessKeyID":      minioResource.AccessKeyID,
				"secretAccessKey":  minioResource.AccessKeySecret,
				"endPoint":         minioResource.Endpoint,
				"forcePathStyle":   true,
				"s3ForcePathStyle": true,
				"disableSSL":       true,
				"region":           minioResource.Region,
				"enableSSE":        false,
				"bucketProvider":   provider,
			}

			for k, v := range tc.conf {
				conf[k] = v
			}

			fm, err := filemanager.New(&filemanager.Settings{
				Provider: provider,
				Config:   conf,
			})
			require.NoError(t, err)

			f, err := os.Open("testdata/sample.csv.gz")
			require.NoError(t, err)

			defer func() { _ = f.Close() }()

			var (
				loadFiles []warehouseutils.LoadFile
				ctx       context.Context
			)

			if tc.ctx != nil {
				ctx = tc.ctx
			} else {
				ctx = context.Background()
			}

			for j := 0; j < tc.numLoadFiles; j++ {
				uploadOutput, err := fm.Upload(context.Background(), f, fmt.Sprintf("%d", i), uuid.New().String())
				require.NoError(t, err)

				loadFiles = append(loadFiles, warehouseutils.LoadFile{
					Location: uploadOutput.Location,
				})
			}
			loadFiles = append(loadFiles, tc.loadFiles...)

			lfd := downloader.NewDownloader(
				&model.Warehouse{
					Destination: backendconfig.DestinationT{
						ID:     destinationID,
						Config: conf,
						DestinationDefinition: backendconfig.DestinationDefinitionT{
							Name: destType,
						},
						WorkspaceID: workspaceID,
					},
				},
				newMockUploader(t, loadFiles),
				workers,
			)

			fileNames, err := lfd.Download(ctx, table)

			misc.RemoveFilePaths(fileNames...)

			if tc.wantError != nil {
				require.ErrorContains(t, err, tc.wantError.Error())
				require.Equal(t, len(fileNames), 0)
				return
			}

			require.NoError(t, err)
			require.Equal(t, len(fileNames), len(loadFiles))
		})
	}
}

func newMockUploader(t testing.TB, loadFiles []warehouseutils.LoadFile) *mockuploader.MockUploader {
	ctrl := gomock.NewController(t)
	u := mockuploader.NewMockUploader(ctrl)
	u.EXPECT().GetLoadFilesMetadata(gomock.Any(), gomock.Any()).AnyTimes().Return(loadFiles, nil)
	u.EXPECT().UseRudderStorage().Return(false).AnyTimes()
	return u
}
