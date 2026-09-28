package warehouseutils

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMicrosoftFabricRegistration(t *testing.T) {
	require.Contains(t, WarehouseDestinations, MicrosoftFabric)
	require.Equal(t, "microsoft_fabric", WHDestNameMap[MicrosoftFabric])
	require.Equal(t, OneLake, ObjectStorageMap[MicrosoftFabric])
	require.Equal(t, OneLake, ObjectStorageType(MicrosoftFabric, map[string]any{"bucketProvider": S3}, false))
	require.Equal(t, OneLake, ObjectStorageType(MicrosoftFabric, map[string]any{"bucketProvider": S3}, true))
	require.Equal(t, LoadFileTypeParquet, GetLoadFileType(MicrosoftFabric))
	require.Contains(t, ReservedKeywords[MicrosoftFabric], "SELECT")
}
