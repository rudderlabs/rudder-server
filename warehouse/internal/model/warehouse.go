package model

import backendconfig "github.com/rudderlabs/rudder-server/backend-config"

type DestinationConfigSetting interface{ string() string }

type destConfSetting string

func (s destConfSetting) string() string { return string(s) }

const (
	EnableMergeSetting      destConfSetting = "enableMerge"
	UseRudderStorageSetting destConfSetting = "useRudderStorage"
)

type Warehouse struct {
	WorkspaceID string
	Source      backendconfig.SourceT
	Destination backendconfig.DestinationT
	Namespace   string
	Type        string
	Identifier  string
}

func (w *Warehouse) GetBoolDestinationConfig(key DestinationConfigSetting) bool {
	destConfig := w.Destination.Config
	if destConfig[key.string()] != nil {
		if val, ok := destConfig[key.string()].(bool); ok {
			return val
		}
	}
	return false
}
