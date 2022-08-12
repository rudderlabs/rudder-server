package warehouseutils

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReservedKeywords(t *testing.T) {
	for _, destinationType := range WarehouseDestinations {
		require.NotNil(t, ReservedKeywords[destinationType], fmt.Sprintf("Reserved Keywords coming to be nil for destinationType %s", destinationType))
	}
}
