package validations

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestGetTableUsesUniqueNamesForDeltalake(t *testing.T) {
	destination := &backendconfig.DestinationT{
		DestinationDefinition: backendconfig.DestinationDefinitionT{
			Name: warehouseutils.DELTALAKE,
		},
		Config: map[string]any{},
	}

	firstTable := getTable(destination)
	secondTable := getTable(destination)

	require.NotEqual(t, firstTable, secondTable)
	require.True(t, strings.HasPrefix(firstTable, table+"_"))
	require.True(t, strings.HasPrefix(secondTable, table+"_"))
}

func TestGetTableUsesFixedNameForNonDeltalake(t *testing.T) {
	destination := &backendconfig.DestinationT{
		DestinationDefinition: backendconfig.DestinationDefinitionT{
			Name: warehouseutils.POSTGRES,
		},
		Config: map[string]any{},
	}

	require.Equal(t, table, getTable(destination))
}
