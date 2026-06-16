package warehouseutils

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReservedKeywords(t *testing.T) {
	for destinationType := range PseudoWarehouseDestinationMap {
		require.NotNil(t, ReservedKeywords[destinationType], fmt.Sprintf("Reserved Keywords coming to be nil for destinationType %s", destinationType))
	}
}
