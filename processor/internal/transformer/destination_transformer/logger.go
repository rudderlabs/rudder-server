package destination_transformer

import (
	"fmt"

	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
	"github.com/samber/lo"

	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	"github.com/rudderlabs/rudder-go-kit/stringify"

	"github.com/rudderlabs/rudder-server/processor/types"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

func (c *Client) CompareAndLog(
	embeddedResponse, legacyResponse types.Response,
) {
	c.loggedEventsMu.Lock()
	defer c.loggedEventsMu.Unlock()

	if c.loggedEvents >= int64(c.config.maxLoggedEvents.Load()) {
		return
	}

	c.stats.comparisonTime.RecordDuration()()

	differingResponse, sampleDiff := c.differingEvents(embeddedResponse, legacyResponse)
	if len(differingResponse) == 0 && sampleDiff == "" {
		c.log.Infof("Embedded and legacy responses are matches")
		return
	}

	logEntries := lo.Map(differingResponse, func(item types.TransformerResponse, index int) string {
		return stringify.Any(item)
	})
	if err := c.write(append([]string{sampleDiff}, logEntries...)); err != nil {
		c.log.Warnn("Error logging events", obskit.Error(err))
		return
	}

	c.log.Infof("Successfully logged events: %d", len(logEntries))
	c.loggedEvents += int64(len(logEntries))
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

func (c *Client) write(data []string) error {
	writer, err := misc.CreateGZ(c.loggedFileName)
	if err != nil {
		return fmt.Errorf("creating buffered writer: %w", err)
	}
	defer func() { _ = writer.Close() }()

	for _, entry := range data {
		if _, err := writer.Write([]byte(entry + "\n")); err != nil {
			return fmt.Errorf("writing log entry: %w", err)
		}
	}
	return nil
}

func generateLogFileName() string {
	return fmt.Sprintf("destination_transformations_debug_%s.log.gz", uuid.NewString())
}
