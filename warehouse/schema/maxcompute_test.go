package schema

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestMaxComputeDiscardsSchema(t *testing.T) {
	schema := enhanceDiscardsSchema(model.Schema{
		"tracks": {"id": "string"},
	}, whutils.MAXCOMPUTE)
	require.Equal(t, "datetime", schema["rudder_discards"]["loaded_at"])
}
