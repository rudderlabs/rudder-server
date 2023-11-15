package model

import (
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
)

type DestinationConfigSetting interface {
	String() string
	protected()
}

type destConfSetting string

func (destConfSetting) protected()       {}
func (s destConfSetting) String() string { return string(s) }

var (
	PreferAppendSetting     DestinationConfigSetting = destConfSetting("preferAppend")
	UseRudderStorageSetting DestinationConfigSetting = destConfSetting("useRudderStorage")
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
	if destConfig[key.String()] != nil {
		if val, ok := destConfig[key.String()].(bool); ok {
			return val
		}
	}
	return false
}

func (w *Warehouse) GetPreferAppendSetting() bool {
	destConfig := w.Destination.Config
	value, ok := destConfig[PreferAppendSetting.String()].(bool)
	if !ok {
		// when the value is not defined it is important we choose the right default
		// in order to maintain backward compatibility for existing destinations
		return w.Type == "BQ" // defaulting to true for BQ, false for other destination types
	}
	return value
}
