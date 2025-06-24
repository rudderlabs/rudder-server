package collector

import (
	"context"

	. "github.com/rudderlabs/rudder-server/utils/tx"
	reportingtypes "github.com/rudderlabs/rudder-server/utils/types"
)

// MetricsCollector interface defines the contract for collecting metrics
type MetricsCollector interface {
	// CurrentStage returns the current stage of the metrics collection.
	CurrentStage() string

	// Collect output event metrics for the current stage. An error will be returned if the stage is not active.
	Collect(events ...*reportingtypes.OutMetricEvent) error

	// Enter a new stage in the metrics collection process. The previous stage will be marked as complete.
	// Successful events of the previous stage will be treated as input events for the new stage.
	NextStage(stageDetails reportingtypes.StageDetails) error

	// End the current stage in the metrics collection process and mark the metrics collector as complete (readonly).
	End() error

	// Merge this MetricsCollector with another one and return a new MetricsCollector.
	Merge(other MetricsCollector) (MetricsCollector, error)

	// Flush the metrics to the database. This will finalize the metrics collection and persist the data.
	// The collector must be marked as complete (readonly) before committing, otherwise an error will be returned.
	Flush(ctx context.Context, tx *Tx) error
}
