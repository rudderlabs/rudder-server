package warehouseutils

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestReservedKeywords(t *testing.T) {
	for _, destinationType := range WarehouseDestinations {
		require.NotNil(t, ReservedKeywords[destinationType], fmt.Sprintf("Reserved Keywords coming to be nil for destinationType %s", destinationType))
	}
}
