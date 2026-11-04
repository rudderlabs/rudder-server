package validations

import (
	"compress/gzip"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/filemanager"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func maxComputeDestination() *backendconfig.DestinationT {
	return &backendconfig.DestinationT{
		WorkspaceID:           "workspace-id",
		DestinationDefinition: backendconfig.DestinationDefinitionT{Name: warehouseutils.MAXCOMPUTE},
		Config: map[string]any{
			"bucketName": "customer-bucket", "region": "cn-hangzhou",
			"accessKeyId": "id", "accessKeySecret": "secret", "ossRoleArn": "read-role", "project": "MyProject",
			"endpoint": "https://service.cn-hangzhou.maxcompute.aliyun.com/api",
		},
	}
}

func TestMaxComputeValidationFixture(t *testing.T) {
	Init()
	dest := maxComputeDestination()
	require.Equal(t, "MyProject", configuredNamespaceInDestination(dest))
	dest.Config["namespace"] = "MySchema"
	require.Equal(t, "MySchema", configuredNamespaceInDestination(dest))
	require.NotEqual(t, getTable(dest), getTable(dest))
	path, written, err := CreateTempLoadFile(dest)
	require.NoError(t, err)
	require.Positive(t, written)
	t.Cleanup(func() { _ = os.Remove(path) })
	require.True(t, strings.HasSuffix(path, ".json.gz"))
	require.Equal(t, connectionTestingFolder, filepath.Base(filepath.Dir(path)))
	f, err := os.Open(path)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()
	gz, err := gzip.NewReader(f)
	require.NoError(t, err)
	defer func() { _ = gz.Close() }()
	contents, err := io.ReadAll(gz)
	require.NoError(t, err)
	require.JSONEq(t, `{"id":1,"val":"RudderStack","uuid_ts":"2024-01-02 00:00:00"}`, string(contents))
	require.Equal(t, "datetime", validationTableSchema(true)["uuid_ts"])
	require.NotContains(t, validationTableSchema(false), "uuid_ts")
	require.NotContains(t, tableSchemaMap, "uuid_ts")
	require.NotContains(t, payloadMap, "uuid_ts")
	require.NotContains(t, validationTableSchema(true), "_partitiontime")
}

func TestMaxComputeOSSValidationRejectsInvalidAuthBeforeManager(t *testing.T) {
	dest := maxComputeDestination()
	dest.Config["iamRoleARN"] = "role"
	_, err := NewValidator(context.Background(), model.VerifyingConnections, dest)
	require.ErrorContains(t, err, "cannot be combined")
}

func TestMaxComputeFileManagerRejectsMissingCredentials(t *testing.T) {
	Init()
	dest := maxComputeDestination()
	dest.Config["accessKeySecret"] = ""
	_, err := createFileManager(dest)
	require.ErrorContains(t, err, "complete pair")
}

func TestMaxComputeOSSManagerSettings(t *testing.T) {
	Init()
	dest := maxComputeDestination()
	dest.Config["iamRoleARN"] = "role"
	delete(dest.Config, "accessKeyId")
	delete(dest.Config, "accessKeySecret")
	dest.Config["externalID"] = "untrusted"
	dest.Config["useRudderStorage"] = true
	var settings *filemanager.Settings
	originalFactory := fileManagerFactory
	fileManagerFactory = func(s *filemanager.Settings) (filemanager.FileManager, error) {
		settings = s
		return &validationFileManager{}, nil
	}
	t.Cleanup(func() { fileManagerFactory = originalFactory })
	_, err := createFileManager(dest)
	require.NoError(t, err)
	require.Equal(t, warehouseutils.AlibabaOSS, settings.Provider)
	require.Equal(t, dest.WorkspaceID, settings.Config["externalID"])
	require.Equal(t, "untrusted", dest.Config["externalID"])
	require.Equal(t, "customer-bucket", settings.Config["bucketName"])
	require.NotContains(t, settings.Config, "useRudderStorage")
	require.NotContains(t, settings.Config, "accessKeyID")
	require.NotContains(t, settings.Config, "accessKey")
	delete(dest.Config, "iamRoleARN")
	dest.Config["accessKeyId"] = "id"
	dest.Config["accessKeySecret"] = "secret"
	_, err = createFileManager(dest)
	require.NoError(t, err)
	require.Equal(t, "id", settings.Config["accessKeyID"])
	require.Equal(t, "secret", settings.Config["accessKey"])
	require.Equal(t, "id", dest.Config["accessKeyId"])
	require.NotContains(t, settings.Config, "externalID")
}

// Only SetTimeout is reached when exercising the converted settings.
type validationFileManager struct{ filemanager.FileManager }

func (*validationFileManager) SetTimeout(time.Duration) {}
