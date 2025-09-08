package stages

import (
	"context"

	"github.com/rudderlabs/rudder-server/utils/shared"
)

// ProcessorStage interface defines a stage in the processing pipeline
type ProcessorStage interface {
	// Process takes input events and returns output events
	Process(ctx context.Context, events []*shared.InputEvent) ([]*shared.OutputEvent, error)
}
