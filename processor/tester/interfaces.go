package tester

import (
	"encoding/json"

	"github.com/rudderlabs/rudder-server/utils/shared"
)

// ProcessorStageTester interface defines a tester for processor stages
type ProcessorStageTester interface {
	// Test takes input events and configuration, returns processed events
	Test(events []*shared.EventWithMetadata, config json.RawMessage) ([]*shared.EventWithMetadata, error)
}
