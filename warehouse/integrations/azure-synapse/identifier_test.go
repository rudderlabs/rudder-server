package azuresynapse

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestDeleteByRemainsNotImplemented(t *testing.T) {
	err := (&AzureSynapse{}).DeleteBy(context.Background(), []string{`events`}, warehouseutils.DeleteByParams{})
	require.EqualError(t, err, warehouseutils.NotImplementedErrorCode)
}
