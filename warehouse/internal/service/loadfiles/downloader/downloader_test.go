package downloader_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"

	"github.com/rudderlabs/rudder-server/warehouse/internal/service/loadfiles/downloader"

	"github.com/google/uuid"
	"github.com/ory/dockertest/v3"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/testhelper/destination"
	"github.com/rudderlabs/rudder-server/utils/misc"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/stretchr/testify/require"
)

type mockUploader struct {
	loadFiles []warehouseutils.LoadFile
}

func (*mockUploader) GetSchemaInWarehouse() model.Schema               { return model.Schema{} }
func (*mockUploader) GetLocalSchema() model.Schema                     { return model.Schema{} }
func (*mockUploader) UpdateLocalSchema(model.Schema) error             { return nil }
func (*mockUploader) ShouldOnDedupUseNewRecord() bool                  { return false }
func (*mockUploader) GetLoadFileGenStartTIme() time.Time               { return time.Time{} }
func (*mockUploader) GetLoadFileType() string                          { return "JSON" }
func (*mockUploader) GetFirstLastEvent() (time.Time, time.Time)        { return time.Time{}, time.Time{} }
func (*mockUploader) GetSampleLoadFileLocation(string) (string, error) { return "", nil }
func (*mockUploader) UseRudderStorage() bool                           { return false }
func (*mockUploader) GetTableSchemaInWarehouse(string) model.TableSchema {
	return model.TableSchema{}
}

func (*mockUploader) GetSingleLoadFile(string) (warehouseutils.LoadFile, error) {
	return warehouseutils.LoadFile{}, nil
}

func (m *mockUploader) GetTableSchemaInUpload(string) model.TableSchema {
	return model.TableSchema{}
}

func (m *mockUploader) GetLoadFilesMetadata(warehouseutils.GetLoadFilesOptions) []warehouseutils.LoadFile {
	return m.loadFiles
}

func TestDownloader(t *testing.T) {
	misc.Init()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	var (
		minioResource *destination.MINIOResource
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
					Location: "http://localhost:56524/devintegrationtest/cc0e3daa-1356-4781-8ce1-549ff95a8313/random.csv.gz",
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

			minioResource, err = destination.SetupMINIO(pool, t)
			require.NoError(t, err)

			conf := map[string]any{
				"bucketName":       minioResource.BucketName,
				"accessKeyID":      minioResource.AccessKey,
				"secretAccessKey":  minioResource.SecretKey,
				"endPoint":         minioResource.Endpoint,
				"forcePathStyle":   true,
				"s3ForcePathStyle": true,
				"disableSSL":       true,
				"region":           minioResource.SiteRegion,
				"enableSSE":        false,
				"bucketProvider":   provider,
			}

			for k, v := range tc.conf {
				conf[k] = v
			}

			fmFactory := filemanager.FileManagerFactoryT{}
			fm, err := fmFactory.New(&filemanager.SettingsT{
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
				&mockUploader{
					loadFiles: loadFiles,
				},
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
