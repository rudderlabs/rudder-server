package replay

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseFileKey(t *testing.T) {
	path := "1680533788.prod-ue1-hinge-v1-rudderstack-8.50113030-50313029.4065231c-26e2-459f-9b56-66cf48dca3d9.json.gz"
	df, err := ParseFileKey(path)
	require.NoError(t, err)

	require.Equal(t, df.InstanceID, 8)
	require.Equal(t, df.MinJobID, 50113030)
	require.Equal(t, df.MaxJobID, 50313029)
}
