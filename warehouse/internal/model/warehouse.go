package model

import (
	"fmt"

	"github.com/rudderlabs/rudder-go-kit/config"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
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

func (w *Warehouse) GetStringDestinationConfig(conf *config.Config, key DestinationConfigSetting) string {
	configKey := fmt.Sprintf("Warehouse.pipeline.%s.%s.%s", w.Source.ID, w.Destination.ID, key)
	if conf.IsSet(configKey) {
		return conf.GetString(configKey, "")
	}

	destConfig := w.Destination.Config
	if destConfig[key.String()] != nil {
		if val, ok := destConfig[key.String()].(string); ok {
			return val
		}
	}
	return ""
}

func (w *Warehouse) GetMapDestinationConfig(key DestinationConfigSetting) map[string]interface{} {
	destConfig := w.Destination.Config
	if destConfig[key.String()] != nil {
		if val, ok := destConfig[key.String()].(map[string]interface{}); ok {
			return val
		}
	}
	return map[string]interface{}{}
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

func (w *Warehouse) IsEnabled() bool {
	return w.Source.Enabled && w.Destination.Enabled
}
