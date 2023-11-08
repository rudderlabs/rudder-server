package types_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/router/types"
)

func TestDehydrateAndHydrate(t *testing.T) {
	msg := types.TransformMessageT{
		Data: []types.RouterJobT{
			{JobMetadata: types.JobMetadataT{JobID: 1, JobT: &jobsdb.JobT{JobID: 1}}},
			{JobMetadata: types.JobMetadataT{JobID: 2, JobT: &jobsdb.JobT{JobID: 2}}},
			{JobMetadata: types.JobMetadataT{JobID: 3, JobT: &jobsdb.JobT{JobID: 3}}},
		},
	}

	msgCopy, jobs := msg.Dehydrate()

	require.Equal(t, len(msg.Data), len(jobs))
	require.Equal(t, len(msg.Data), len(msgCopy.Data))
	for i := range msg.Data {
		require.NotNil(t, msg.Data[i].JobMetadata.JobT, "JobT should not be nil")
		require.Nil(t, msgCopy.Data[i].JobMetadata.JobT, "JobT should be nil")
	}

	destJobs := types.DestinationJobs{
		types.DestinationJobT{JobMetadataArray: []types.JobMetadataT{{JobID: 1}}},
		types.DestinationJobT{JobMetadataArray: []types.JobMetadataT{{JobID: 2}}},
		types.DestinationJobT{JobMetadataArray: []types.JobMetadataT{{JobID: 3}}},
	}
	destJobs.Hydrate(jobs)

	for i := range destJobs {
		require.NotNil(t, destJobs[i].JobMetadataArray[0].JobT, "JobT should not be nil")
	}
}
