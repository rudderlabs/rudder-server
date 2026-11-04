package validations

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
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
	path, expectedHash, err := createTempLoadFile(dest)
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.Remove(path) })
	compressed, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, sha256.Sum256(compressed), expectedHash)
	require.True(t, strings.HasSuffix(path, ".json.gz"))
	require.Regexp(t, `^validation-[^/]+\.json\.gz$`, filepath.Base(path))
	require.Equal(t, connectionTestingFolder, filepath.Base(filepath.Dir(path)))
	f, err := os.Open(path)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()
	gz, err := gzip.NewReader(f)
	require.NoError(t, err)
	defer func() { _ = gz.Close() }()
	contents, err := io.ReadAll(gz)
	require.NoError(t, err)
	require.JSONEq(t, `{"id":1,"val":"RudderStack","_partitiontime":"2024-01-02 03:04:05"}`, string(contents))
	require.Equal(t, "datetime", maxComputeValidationSchema()["_partitiontime"])
	require.Equal(t, maxComputeValidationPartitionTime, maxComputeValidationPayload()["_partitiontime"])
	require.NotContains(t, tableSchemaMap, "_partitiontime")
	require.NotContains(t, payloadMap, "_partitiontime")
	originalConfig := dest.Config
	originalConfig["partitionMode"] = "dynamic"
	originalConfig["partitionModes"] = map[string]any{"customer_table": "dynamic"}
	converted := maxComputeValidationWarehouse(createDummyWarehouse(dest))
	require.Equal(t, "fixed", model.PartitionModeForTable(converted.Destination.Config, getTable(dest)))
	require.NotContains(t, converted.Destination.Config, "partitionModes")
	require.Equal(t, "dynamic", originalConfig["partitionMode"])
	require.Contains(t, originalConfig, "partitionModes")
	require.Equal(t, "datetime", validationTableSchema(true)["_partitiontime"])
	require.NotContains(t, validationTableSchema(false), "_partitiontime")
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

func TestMaxComputeReadbackHash(t *testing.T) {
	path := t.TempDir() + "/fixture"
	require.NoError(t, os.WriteFile(path, []byte("fixture"), 0o600))
	f, err := os.Open(path)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()
	hash, err := hashValidationFile(f)
	require.NoError(t, err)
	require.Equal(t, sha256.Sum256([]byte("fixture")), hash)
	different := sha256.Sum256([]byte("different"))
	require.False(t, bytes.Equal(different[:], hash[:]))
}

func TestMaxComputeReadbackBound(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "readback-")
	require.NoError(t, err)
	defer func() { _ = f.Close() }()
	writer := &boundedValidationWriter{file: f, maxBytes: 7}
	n, err := writer.WriteAt([]byte("fixture"), 0)
	require.NoError(t, err)
	require.Equal(t, 7, n)
	_, err = writer.WriteAt([]byte("!"), 7)
	require.ErrorContains(t, err, "readback exceeds")
	_, err = writer.WriteAt([]byte("!"), -1)
	require.ErrorContains(t, err, "readback exceeds")
	info, err := f.Stat()
	require.NoError(t, err)
	require.EqualValues(t, 7, info.Size())
}
