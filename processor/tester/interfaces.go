package tester

import (
	"encoding/json"

	"github.com/rudderlabs/rudder-server/utils/shared"
)

// ProcessorStageTester interface defines a tester for processor stages
type ProcessorStageTester interface {
	// Test takes input events and configuration, returns processed events
	Test(events []*shared.InputEvent, config json.RawMessage) ([]*shared.OutputEvent, error)
}
