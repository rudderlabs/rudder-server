package transformer

import (
	"fmt"
	"sync"

	"github.com/google/uuid"
	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stringify"

	ptrans "github.com/rudderlabs/rudder-server/processor/transformer"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/types"
)

type DebugLogger struct {
	logger             logger.Logger
	maxLoggedEvents    config.ValueLoader[int]
	eventLogMutex      sync.Mutex
	currentLogFileName string
	loggedEvents       int64
}

func NewDebugLogger(conf *config.Config, logger logger.Logger) *DebugLogger {
	logFileName := generateLogFileName()

	return &DebugLogger{
		logger:             logger.Child("debugLogger").With("currentLogFileName", logFileName),
		maxLoggedEvents:    conf.GetReloadableIntVar(10000, 1, "Processor.maxLoggedEvents"),
		currentLogFileName: logFileName,
	}
}

func generateLogFileName() string {
	return fmt.Sprintf("warehouse_transformations_debug_%s.log", uuid.NewString())
}

func (d *DebugLogger) LogEvents(events []types.SingularEventT, commonMedata *ptrans.Metadata) error {
	if len(events) == 0 {
		return nil
	}
	d.eventLogMutex.Lock()
	defer d.eventLogMutex.Unlock()

	if d.loggedEvents >= int64(d.maxLoggedEvents.Load()) {
		return nil
	}

	logEntries := lo.Map(events, func(item types.SingularEventT, index int) string {
		return stringify.Any(ptrans.TransformerEvent{
			Message:  item,
			Metadata: *commonMedata,
		})
	})

	if err := d.writeLogEntries(logEntries); err != nil {
		return fmt.Errorf("logging events: %w", err)
	}

	d.logger.Infon("Successfully logged events", logger.NewIntField("event_count", int64(len(logEntries))))
	d.loggedEvents += int64(len(logEntries))
	return nil
}

func (d *DebugLogger) writeLogEntries(entries []string) error {
	writer, err := misc.CreateBufferedWriter(d.currentLogFileName)
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
