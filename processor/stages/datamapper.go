package stages

import (
	"context"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/utils/shared"
)

// AppliedMapping represents a mapping that was actually applied during transformation
type AppliedMapping struct {
	ID   string `json:"id"`
	From string `json:"from"`
	To   string `json:"to"`
}

// AppliedMappingsMetadata contains information about which mappings were applied
type AppliedMappingsMetadata struct {
	Event      *AppliedMapping  `json:"event,omitempty"`
	Properties []AppliedMapping `json:"properties,omitempty"`
}

// DataMapperStage implements ProcessorStage interface for data mapping functionality
type DataMapperStage struct {
	dataMappings backendconfig.DataMappings
}

// NewDataMapperStage creates a new DataMapperStage with the given data mappings
func NewDataMapperStage(dataMappings backendconfig.DataMappings) *DataMapperStage {
	return &DataMapperStage{
		dataMappings: dataMappings,
	}
}

// Process implements the ProcessorStage interface
// It transforms event names and properties based on the configured mappings
func (d *DataMapperStage) Process(ctx context.Context, events []*shared.EventWithMetadata) ([]*shared.EventWithMetadata, error) {
	if len(events) == 0 {
		return events, nil
	}

	// Create mapping lookups for efficient transformation
	eventMappings := make(map[string]backendconfig.Mapping)
	propertyMappings := make(map[string]backendconfig.Mapping)

	// Build event name mappings
	for _, mapping := range d.dataMappings.Events {
		if mapping.Enabled {
			eventMappings[mapping.From] = mapping
		}
	}

	// Build property name mappings
	for _, mapping := range d.dataMappings.Properties {
		if mapping.Enabled {
			propertyMappings[mapping.From] = mapping
		}
	}

	// Transform events and track applied mappings
	for _, event := range events {
		if event == nil || event.Event == nil {
			continue
		}

		appliedMappings := AppliedMappingsMetadata{}

		// Transform event name
		if eventName, exists := event.Event["event"].(string); exists {
			if mapping, mappingExists := eventMappings[eventName]; mappingExists {
				event.Event["event"] = mapping.To
				appliedMappings.Event = &AppliedMapping{
					ID:   mapping.ID,
					From: mapping.From,
					To:   mapping.To,
				}
			}
		}

		// Transform properties in the Event map and track applied mappings
		propertyMappingsApplied := d.transformPropertiesWithTracking(event.Event, propertyMappings)
		appliedMappings.Properties = propertyMappingsApplied

		// Inject applied mappings into event context if any were applied
		if appliedMappings.Event != nil || len(appliedMappings.Properties) > 0 {
			if event.Event["context"] == nil {
				event.Event["context"] = make(map[string]interface{})
			}
			context := event.Event["context"].(map[string]interface{})
			context["dataMappings"] = appliedMappings
		}
	}

	return events, nil
}

// transformPropertiesWithTracking recursively transforms property names in a map and tracks applied mappings
func (d *DataMapperStage) transformPropertiesWithTracking(eventMap map[string]interface{}, mappings map[string]backendconfig.Mapping) []AppliedMapping {
	if eventMap == nil {
		return nil
	}

	var appliedMappings []AppliedMapping

	// Handle properties object specifically
	if properties, ok := eventMap["properties"].(map[string]interface{}); ok {
		applied := d.transformMapKeysWithTracking(properties, mappings)
		appliedMappings = append(appliedMappings, applied...)
	}

	// Handle traits object for identify events
	if traits, ok := eventMap["traits"].(map[string]interface{}); ok {
		applied := d.transformMapKeysWithTracking(traits, mappings)
		appliedMappings = append(appliedMappings, applied...)
	}

	// Handle context properties
	if context, ok := eventMap["context"].(map[string]interface{}); ok {
		if contextTraits, ok := context["traits"].(map[string]interface{}); ok {
			applied := d.transformMapKeysWithTracking(contextTraits, mappings)
			appliedMappings = append(appliedMappings, applied...)
		}
	}

	return appliedMappings
}

// transformMapKeysWithTracking transforms the keys of a map based on the provided mappings and tracks applied mappings
func (d *DataMapperStage) transformMapKeysWithTracking(m map[string]interface{}, mappings map[string]backendconfig.Mapping) []AppliedMapping {
	// Collect keys to transform to avoid modifying map while iterating
	keysToTransform := make(map[string]backendconfig.Mapping)
	for key := range m {
		if mapping, exists := mappings[key]; exists {
			keysToTransform[key] = mapping
		}
	}

	var appliedMappings []AppliedMapping

	// Apply transformations and track applied mappings
	for oldKey, mapping := range keysToTransform {
		if value, exists := m[oldKey]; exists {
			m[mapping.To] = value
			delete(m, oldKey)
			appliedMappings = append(appliedMappings, AppliedMapping{
				ID:   mapping.ID,
				From: mapping.From,
				To:   mapping.To,
			})
		}
	}

	return appliedMappings
}

// transformProperties recursively transforms property names in a map
func (d *DataMapperStage) transformProperties(eventMap map[string]interface{}, mappings map[string]string) {
	if eventMap == nil {
		return
	}

	// Handle properties object specifically
	if properties, ok := eventMap["properties"].(map[string]interface{}); ok {
		d.transformMapKeys(properties, mappings)
	}

	// Handle traits object for identify events
	if traits, ok := eventMap["traits"].(map[string]interface{}); ok {
		d.transformMapKeys(traits, mappings)
	}

	// Handle context properties
	if context, ok := eventMap["context"].(map[string]interface{}); ok {
		if contextTraits, ok := context["traits"].(map[string]interface{}); ok {
			d.transformMapKeys(contextTraits, mappings)
		}
	}
}

// transformMapKeys transforms the keys of a map based on the provided mappings
func (d *DataMapperStage) transformMapKeys(m map[string]interface{}, mappings map[string]string) {
	// Collect keys to transform to avoid modifying map while iterating
	keysToTransform := make(map[string]string)
	for key := range m {
		if newKey, exists := mappings[key]; exists {
			keysToTransform[key] = newKey
		}
	}

	// Apply transformations
	for oldKey, newKey := range keysToTransform {
		if value, exists := m[oldKey]; exists {
			m[newKey] = value
			delete(m, oldKey)
		}
	}
}
