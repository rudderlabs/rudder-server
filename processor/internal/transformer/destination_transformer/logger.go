package destination_transformer

import (
	"bytes"
	"context"
	"fmt"
	"path"

	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	"github.com/rudderlabs/rudder-server/jsonrs"
	"github.com/rudderlabs/rudder-server/processor/types"
)

func (c *Client) CompareAndLog(
	ctx context.Context,
	embeddedResponse, legacyResponse types.Response,
) {
	if c.samplingFileManager == nil { // Cannot upload, we should just report the issue with no diff
		c.log.Warnn("DestinationTransformer sanity check failed")
		return
	}

	if c.loggedEvents.Load() >= int64(c.config.maxLoggedEvents.Load()) {
		return
	}

	c.stats.comparisonTime.RecordDuration()()

	differingResponse, sampleDiff := c.differingEvents(embeddedResponse, legacyResponse)
	if len(differingResponse) == 0 && sampleDiff == "" {
		return
	}

	objName := path.Join("embedded-dt-samples", config.GetKubeNamespace(), uuid.New().String())
	differingResponseJSON, err := jsonrs.Marshal(differingResponse)
	if err != nil {
		c.log.Errorn("DestinationTransformer sanity check failed (cannot encode differingResponse)", obskit.Error(err))
		return
	}

	// upload sample diff and differing response to s3
	file, err := c.samplingFileManager.UploadReader(ctx, objName, bytes.NewReader(append([]byte(sampleDiff), differingResponseJSON...)))
	if err != nil {
		c.log.Errorn("Error uploading DestinationTransformer sanity check diff file", obskit.Error(err))
		return
	}

	c.log.Warnn("DestinationTransformer sanity check failed",
		logger.NewStringField("location", file.Location),
		logger.NewStringField("objectName", file.ObjectName),
	)
	c.loggedEvents.Add(int64(len(differingResponse)))
}

func (c *Client) differingEvents(
	embeddedResponse, legacyResponse types.Response,
) ([]types.TransformerResponse, string) {
	if len(embeddedResponse.Events) != len(legacyResponse.Events) || len(embeddedResponse.FailedEvents) != len(legacyResponse.FailedEvents) {
		c.stats.mismatchedEvents.Count(len(embeddedResponse.Events) + len(embeddedResponse.FailedEvents))
		return []types.TransformerResponse{}, fmt.Sprintf("Event counts mismatch: Successful events (%d vs %d), Failed events (%d vs %d)",
			len(embeddedResponse.Events),
			len(legacyResponse.Events),
			len(embeddedResponse.FailedEvents),
			len(legacyResponse.FailedEvents))
	}

	var (
		differedSampleEvents []types.TransformerResponse
		differedEventsCount  int
		sampleDiff           string
	)

	for i := range legacyResponse.Events {
		diff := cmp.Diff(legacyResponse.Events[i], embeddedResponse.Events[i])
		if len(diff) == 0 {
			continue
		}

		if differedEventsCount == 0 {
			// Collect the mismatched event response and break (sample only)
			differedSampleEvents = append(differedSampleEvents, embeddedResponse.Events[i])
			sampleDiff = diff
		}
		differedEventsCount++
	}

	for i := range legacyResponse.FailedEvents {
		diff := cmp.Diff(legacyResponse.FailedEvents[i], embeddedResponse.FailedEvents[i])
		if len(diff) == 0 {
			continue
		}

		if differedEventsCount == 0 {
			// Collect the mismatched event response and break (sample only)
			differedSampleEvents = append(differedSampleEvents, embeddedResponse.FailedEvents[i])
			sampleDiff = diff
		}
		differedEventsCount++
	}

	c.stats.matchedEvents.Count(len(legacyResponse.Events) + len(legacyResponse.FailedEvents) - differedEventsCount)
	c.stats.mismatchedEvents.Count(differedEventsCount)
	return differedSampleEvents, sampleDiff
}
