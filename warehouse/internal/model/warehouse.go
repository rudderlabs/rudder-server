package model

import backendconfig "github.com/rudderlabs/rudder-server/backend-config"

type DestinationConfigSetting interface{ string() string }

type destConfSetting string

func (s destConfSetting) string() string { return string(s) }

const (
	PreferAppendSetting     destConfSetting = "preferAppend"
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

func (w *Warehouse) GetPreferAppendSetting() bool {
	destConfig := w.Destination.Config
	// defaulting to false if not defined for backwards compatibility with previous behaviour
	value, ok := destConfig[PreferAppendSetting.string()].(bool)
	if !ok {
		if w.Type == "BQ" {
			return true // defaulting to true for BQ
		}
		return false
	}
	return value
}
