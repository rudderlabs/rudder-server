package model

import backendconfig "github.com/rudderlabs/rudder-server/backend-config"

type Warehouse struct {
	WorkspaceID string
	Source      backendconfig.SourceT
	Destination backendconfig.DestinationT
	Namespace   string
	Type        string
	Identifier  string
}

func (w *Warehouse) GetBoolDestinationConfig(key string) bool {
	destConfig := w.Destination.Config
	if destConfig[key] != nil {
		if val, ok := destConfig[key].(bool); ok {
			return val
		}
	}
	return false
}
