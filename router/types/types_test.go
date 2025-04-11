package types_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
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

	msgCopy, preservedData := msg.Dehydrate()

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
	destJobs.Hydrate(preservedData)

	for i := range destJobs {
		require.NotNil(t, destJobs[i].JobMetadataArray[0].JobT, "JobT should not be nil")
	}
}

func TestCompactedTransformMessage(t *testing.T) {
	msg := types.TransformMessageT{
		Data: []types.RouterJobT{
			{
				Message:     []byte(`{"key": "msg1"}`),
				Destination: backendconfig.DestinationT{ID: "d1"},
				Connection:  backendconfig.Connection{SourceID: "s1", DestinationID: "d1"},
				JobMetadata: types.JobMetadataT{DestinationID: "d1", SourceID: "s1"},
			},
			{
				Message:     []byte(`{"key": "msg2"}`),
				Destination: backendconfig.DestinationT{ID: "d2"},
				Connection:  backendconfig.Connection{SourceID: "s1", DestinationID: "d2"},
				JobMetadata: types.JobMetadataT{DestinationID: "d2", SourceID: "s1"},
			},
			{
				Message:     []byte(`{"key": "msg3"}`),
				Destination: backendconfig.DestinationT{ID: "d2"},
				Connection:  backendconfig.Connection{SourceID: "s2", DestinationID: "d2"},
				JobMetadata: types.JobMetadataT{DestinationID: "d2", SourceID: "s2"},
			},
		},
	}

	compactedMsg := msg.Compacted()
	require.Len(t, compactedMsg.Connections, 3)
	require.Len(t, compactedMsg.Destinations, 2)
	require.Len(t, compactedMsg.Data, 3)
	for i := range msg.Data {
		require.Equal(t, msg.Data[i].Message, compactedMsg.Data[i].Message)
		require.Equal(t, msg.Data[i].JobMetadata, compactedMsg.Data[i].JobMetadata)
		require.Equal(t, msg.Data[i].Destination.ID, compactedMsg.Destinations[msg.Data[i].JobMetadata.DestinationID].ID)
		require.Equal(t, msg.Data[i].Connection.SourceID, compactedMsg.Connections[msg.Data[i].JobMetadata.SourceID+":"+msg.Data[i].JobMetadata.DestinationID].SourceID)
		require.Equal(t, msg.Data[i].Connection.DestinationID, compactedMsg.Connections[msg.Data[i].JobMetadata.SourceID+":"+msg.Data[i].JobMetadata.DestinationID].DestinationID)
	}
}
