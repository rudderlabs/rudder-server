package stages

import (
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
)

// StageFactory provides functionality to create processor stage instances
type StageFactory struct{}

// NewStageFactory creates a new StageFactory instance
func NewStageFactory() *StageFactory {
	return &StageFactory{}
}

// GetStageInstance creates and returns a processor stage instance based on the stage name and workspace configuration
func (f *StageFactory) GetStageInstance(stage string, workspaceConfig *backendconfig.ConfigT) ProcessorStage {
	if workspaceConfig == nil {
		return nil
	}

	switch stage {
	case "datamapper":
		return NewDataMapperStage(workspaceConfig.Settings.DataMappings)
	default:
		return nil
	}
}
