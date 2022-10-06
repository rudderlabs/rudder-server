package warehouse

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPickupStagingFileBucket(t *testing.T) {
	inputs := []struct {
		job      *Payload
		expected bool
	}{
		{
			job:      &Payload{},
			expected: false,
		},
		{
			job: &Payload{
				StagingDestinationRevisionID: "1liYatjkkCEVkEMYUmSWOE9eZ4n",
				DestinationRevisionID:        "1liYatjkkCEVkEMYUmSWOE9eZ4n",
			},
			expected: false,
		},
		{
			job: &Payload{
				StagingDestinationRevisionID: "1liYatjkkCEVkEMYUmSWOE9eZ4n",
				DestinationRevisionID:        "2liYatjkkCEVkEMYUmSWOE9eZ4n",
			},
			expected: false,
		},
		{
			job: &Payload{
				StagingDestinationRevisionID: "1liYatjkkCEVkEMYUmSWOE9eZ4n",
				DestinationRevisionID:        "2liYatjkkCEVkEMYUmSWOE9eZ4n",
				StagingDestinationConfig:     map[string]string{},
			},
			expected: true,
		},
	}
	for _, input := range inputs {
		got := PickupStagingConfiguration(input.job)
		require.Equal(t, got, input.expected)
	}
}
