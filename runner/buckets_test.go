package runner

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDeliveryCompressionBuckets(t *testing.T) {
	require.Equal(t, []float64{
		64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 65536, 262144, 1048576, 4194304, 16777216, 52428800,
	}, customBuckets["router_delivery_compressed_payload_size_bytes"])
	require.Equal(t, []float64{
		0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10,
	}, customBuckets["router_delivery_compression_duration_seconds"])
}
