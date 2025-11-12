package warehouse

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/ory/dockertest/v3"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/minio"

	"github.com/rudderlabs/rudder-server/processor/types"
)

func TestTransformer_CompareResponsesAndUpload(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	minioResource, err := minio.Setup(pool, t)
	require.NoError(t, err)

	ctx := context.Background()
	maxLoggedEvents := 10

	c := config.New()
	c.Set("Warehouse.Transformer.Sampling.maxLoggedEvents", maxLoggedEvents)
	c.Set("Warehouse.Transformer.Sampling.Bucket", minioResource.BucketName)
	c.Set("Warehouse.Transformer.Sampling.Endpoint", fmt.Sprintf("http://%s", minioResource.Endpoint))
	c.Set("Warehouse.Transformer.Sampling.AccessKey", minioResource.AccessKeyID)
	c.Set("Warehouse.Transformer.Sampling.SecretAccessKey", minioResource.AccessKeySecret)
	c.Set("Warehouse.Transformer.Sampling.S3ForcePathStyle", true)
	c.Set("Warehouse.Transformer.Sampling.DisableSsl", true)

	statsStore, err := memstats.New()
	require.NoError(t, err)

	trans := New(c, logger.NOP, statsStore)

	eventsByMessageID := make(map[string]types.SingularEventWithReceivedAt, 50)
	for index := 0; index < 50; index++ {
		eventsByMessageID[strconv.Itoa(index)] = types.SingularEventWithReceivedAt{
			SingularEvent: map[string]any{
				"event": "track" + strconv.Itoa(index),
			},
		}
	}

	events := []types.TransformerEvent{
		{
			Message: types.SingularEventT{
				"event":      "track",
				"context":    "context",
				"properties": "properties",
			},
		},
	}

	for i := 0; i < maxLoggedEvents; i++ {
		legacyResponse := types.Response{
			Events: []types.TransformerResponse{
				{
					Output: types.SingularEventT{
						"event": "track",
					},
					Metadata: types.Metadata{
						MessageID:       "messageID",
						SourceID:        "sourceID",
						DestinationID:   "destinationID",
						SourceType:      "sourceType",
						DestinationType: "destinationType",
					},
				},
			},
		}
		trans.compareResponsesAndUpload(ctx, events, legacyResponse)
	}

	minioContents, err := minioResource.Contents(ctx, "")
	require.NoError(t, err)
	require.Len(t, minioContents, maxLoggedEvents)

	differingEvents := lo.Map(minioContents, func(item minio.File, index int) string {
		return item.Content
	})
	differingEvents = lo.Filter(differingEvents, func(item string, index int) bool {
		return strings.Contains(item, "message") // Filtering raw events as the file contains sample diff as well
	})
	require.Len(t, differingEvents, maxLoggedEvents)

	for i := 0; i < maxLoggedEvents; i++ {
		require.Contains(t, differingEvents[i], "track")
	}
	require.EqualValues(t, []float64{1, 1, 1, 1, 1, 1, 1, 1, 1, 1}, statsStore.Get("warehouse_dest_transform_mismatched_events", stats.Tags{}).Values())
}
