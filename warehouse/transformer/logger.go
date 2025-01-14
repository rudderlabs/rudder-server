package transformer

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stringify"

	ptrans "github.com/rudderlabs/rudder-server/processor/transformer"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/types"
)

func (t *Transformer) Log(events []types.SingularEventT, metadata *ptrans.Metadata) error {
	if len(events) == 0 {
		return nil
	}
	t.loggedEventsMu.Lock()
	defer t.loggedEventsMu.Unlock()

	if t.loggedEvents >= int64(t.config.maxLoggedEvents.Load()) {
		return nil
	}

	logEntries := lo.Map(events, func(item types.SingularEventT, index int) string {
		return stringify.Any(ptrans.TransformerEvent{
			Message:  item,
			Metadata: *metadata,
		})
	})
	if err := t.writeLogEntries(logEntries); err != nil {
		return fmt.Errorf("logging events: %w", err)
	}

	t.logger.Infon("Successfully logged events", logger.NewIntField("event_count", int64(len(logEntries))))
	t.loggedEvents += int64(len(logEntries))
	return nil
}

func (t *Transformer) writeLogEntries(entries []string) error {
	writer, err := misc.CreateBufferedWriter(t.loggedFileName)
	if err != nil {
		return fmt.Errorf("creating buffered writer: %w", err)
	}
	defer func() { _ = writer.Close() }()

	for _, entry := range entries {
		if _, err := writer.Write([]byte(entry + "\n")); err != nil {
			return fmt.Errorf("writing log entry: %w", err)
		}
	}
	return nil
}

func generateLogFileName() string {
	return fmt.Sprintf("warehouse_transformations_debug_%s.log", uuid.NewString())
}
