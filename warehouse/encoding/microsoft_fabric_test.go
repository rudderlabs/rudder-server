package encoding

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestMicrosoftFabricParquetSchema(t *testing.T) {
	schema, err := parquetSchema(model.TableSchema{
		"active": "boolean", "count": "int", "price": "float", "received_at": "datetime", "payload": "json", "name": "string",
	}, warehouseutils.MicrosoftFabric)
	require.NoError(t, err)
	require.Len(t, schema, 6)
	require.Contains(t, schema, "name=payload, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL")
	require.Contains(t, schema, "name=received_at, type=INT64, convertedtype=TIMESTAMP_MICROS, repetitiontype=OPTIONAL")
}
