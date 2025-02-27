package transformer

import (
	"fmt"

	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
	"github.com/samber/lo"

	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stringify"

	"github.com/rudderlabs/rudder-server/processor/types"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

func (t *Transformer) CompareAndLog(
	events []types.TransformerEvent,
	pResponse, wResponse types.Response,
	metadata *types.Metadata,
	eventsByMessageID map[string]types.SingularEventWithReceivedAt,
) {
	if len(events) == 0 {
		return
	}

	t.loggedEventsMu.Lock()
	defer t.loggedEventsMu.Unlock()

	if t.loggedEvents >= int64(t.config.maxLoggedEvents.Load()) {
		return
	}

	t.stats.comparisionTime.RecordDuration()()

	differingEvents, sampleDiff := t.differingEvents(events, pResponse, wResponse, eventsByMessageID)
	if len(differingEvents) == 0 {
		return
	}

	logEntries := lo.Map(differingEvents, func(item types.SingularEventT, index int) string {
		return stringify.Any(types.TransformerEvent{
			Message:  item,
			Metadata: *metadata,
		})
	})
	if err := t.write(append([]string{sampleDiff}, logEntries...)); err != nil {
		t.logger.Warnn("Error logging events", obskit.Error(err))
		return
	}

	t.logger.Infon("Successfully logged events", logger.NewIntField("event_count", int64(len(logEntries))))
	t.loggedEvents += int64(len(logEntries))
}

func (t *Transformer) differingEvents(
	eventsToTransform []types.TransformerEvent,
	pResponse, wResponse types.Response,
	eventsByMessageID map[string]types.SingularEventWithReceivedAt,
) ([]types.SingularEventT, string) {
	// If the event counts differ, return all events in the transformation
	if len(pResponse.Events) != len(wResponse.Events) || len(pResponse.FailedEvents) != len(wResponse.FailedEvents) {
		events := lo.Map(eventsToTransform, func(e types.TransformerEvent, _ int) types.SingularEventT {
			return eventsByMessageID[e.Metadata.MessageID].SingularEvent
		})
		t.stats.mismatchedEvents.Observe(float64(len(events)))
		return events, ""
	}

	var (
		differedSampleEvents []types.SingularEventT
		differedEventsCount  int
		sampleDiff           string
	)

	for i := range pResponse.Events {
		diff := cmp.Diff(wResponse.Events[i], pResponse.Events[i])
		if len(diff) == 0 {
			continue
		}

		if differedEventsCount == 0 {
			// Collect the mismatched messages and break (sample only)
			differedSampleEvents = append(differedSampleEvents, lo.Map(pResponse.Events[i].Metadata.GetMessagesIDs(), func(msgID string, _ int) types.SingularEventT {
				return eventsByMessageID[msgID].SingularEvent
			})...)
			sampleDiff = diff
		}
		differedEventsCount++
	}
	t.stats.matchedEvents.Observe(float64(len(pResponse.Events) - differedEventsCount))
	t.stats.mismatchedEvents.Observe(float64(differedEventsCount))
	return differedSampleEvents, sampleDiff
}

func (t *Transformer) write(data []string) error {
	writer, err := misc.CreateGZ(t.loggedFileName)
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
	return fmt.Sprintf("warehouse_transformations_debug_%s.log.gz", uuid.NewString())
}
