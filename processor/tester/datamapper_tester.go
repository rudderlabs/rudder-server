package tester

import (
	"context"
	"encoding/json"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/processor/stages"
	"github.com/rudderlabs/rudder-server/utils/shared"
)

// DataMapperTester implements ProcessorStageTester for testing data mapper functionality
type DataMapperTester struct{}

// NewDataMapperTester creates a new DataMapperTester instance
func NewDataMapperTester() *DataMapperTester {
	return &DataMapperTester{}
}

// Test implements the ProcessorStageTester interface for data mapper testing
// The config parameter should contain JSON representation of DataMappings
func (t *DataMapperTester) Test(events []*shared.InputEvent, config json.RawMessage) ([]*shared.OutputEvent, error) {
	// Parse the configuration
	var dataMappings backendconfig.DataMappings
	if err := json.Unmarshal(config, &dataMappings); err != nil {
		return nil, err
	}

	// Create data mapper stage with the parsed configuration
	stage := stages.NewDataMapperStage(dataMappings)

	// Process events using the stage
	ctx := context.Background()
	return stage.Process(ctx, events)
}
