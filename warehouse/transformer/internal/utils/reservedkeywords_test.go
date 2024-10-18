package utils

import (
	"testing"

	"github.com/stretchr/testify/require"

	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestReservedKeywordsMapping(t *testing.T) {
	for _, destType := range whutils.WarehouseDestinations {
		require.NotNilf(t, reservedKeywordsForColumnsTables[destType], "Reserved keywords not found for destination type %s", destType)
		require.NotNilf(t, reservedKeywordsForNamespaces[destType], "Reserved keywords not found for destination type %s", destType)
	}
}
