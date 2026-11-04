package downloader

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/filemanager"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type maxComputeTestUploader struct {
	warehouseutils.Uploader
	useRudderStorage bool
}

func (u maxComputeTestUploader) UseRudderStorage() bool { return u.useRudderStorage }

func TestMaxComputeDownloaderOSSConfig(t *testing.T) {
	stop := errors.New("factory reached")
	for _, useRudderStorage := range []bool{false, true} {
		for _, mode := range []string{"role", "accessKey", "mixed", "missing", "partial"} {
			t.Run(fmt.Sprintf("%s/useRudderStorage=%t", mode, useRudderStorage), func(t *testing.T) {
				conf := map[string]any{"bucketName": "customer-bucket", "region": "cn-hangzhou", "ossRoleArn": "read-role"}
				switch mode {
				case "role":
					conf["iamRoleARN"] = "role"
				case "accessKey":
					conf["accessKeyId"], conf["accessKeySecret"] = "id", "secret"
				case "mixed":
					conf["iamRoleARN"], conf["accessKeyId"], conf["accessKeySecret"] = "role", "id", "secret"
				case "partial":
					conf["accessKeyId"] = "id"
				}
				called := false
				warehouse := &model.Warehouse{Destination: backendconfig.DestinationT{
					DestinationDefinition: backendconfig.DestinationDefinitionT{Name: warehouseutils.MAXCOMPUTE},
					Config:                conf, WorkspaceID: "workspace-id",
				}}
				l := &downloaderImpl{
					warehouse: warehouse,
					uploader:  maxComputeTestUploader{Uploader: warehouseutils.NewNoOpUploader(), useRudderStorage: useRudderStorage},
					fileManagerFactory: func(s *filemanager.Settings) (filemanager.FileManager, error) {
						called = true
						require.Equal(t, warehouseutils.AlibabaOSS, s.Provider)
						require.Equal(t, "customer-bucket", s.Config["bucketName"])
						require.Equal(t, mode == "role", s.Config["roleBasedAuth"])
						if mode == "role" {
							require.Equal(t, "workspace-id", s.Config["externalID"])
						} else {
							require.NotContains(t, s.Config, "externalID")
						}
						return nil, stop
					},
				}
				_, err := l.Download(context.Background(), "test_table")
				if mode == "role" || mode == "accessKey" {
					require.ErrorIs(t, err, stop)
					require.True(t, called)
				} else {
					require.Error(t, err)
					require.False(t, called)
				}
			})
		}
	}
}
