package observability

import (
	"context"

	"github.com/rudderlabs/rudder-server/utils/shared"
	. "github.com/rudderlabs/rudder-server/utils/tx"
	rt "github.com/rudderlabs/rudder-server/utils/types"
)

// This is an interface for Observability SDK. We plan to use Observability SDK
// 1. To collect all reporting metrics like Default Event Metrics, DataMapper, MTU, ErrorDetails Metrics etc. via single interface
// 2. To provide observability features like Live Events in the future
type Observer interface {
	// ObserveInputEvents records input events at the beginning of a stage processing.
	// These events represent the data entering the current stage.
	ObserveInputEvents(events ...*shared.InputEvent) error

	// ObserveOutputEvents records output events with their processing results at the end of a stage.
	// The output events contain the processing outcome, including mappings and status.
	ObserveOutputEvents(events ...*shared.OutputEvent) error

	// Enter a new stage in the observability collection process. The previous stage will be marked as complete.
	// Successful events of the previous stage will be treated as input events for the new stage.
	NextStage(stage rt.StageDetails) error

	// End the current stage in the observability collection process and mark the collector as complete (readonly).
	End() error

	// Merge another Observer into this instance.
	// Since we are going to have MetricsStore at SubJob/SubBatch level, we need to merge metrics from all Subjobs/SubBatches of a batch before writing to db.
	Merge(other Observer) error

	// Flush the metrics to the database. This will finalize the observability collection and persist the data.
	// The collector must be marked as complete (readonly) before committing, otherwise an error will be returned.
	Flush(ctx context.Context, tx *Tx) error
}
