package backendconfig

import (
	"github.com/samber/lo"
)

type EventReplayConfigs map[string]*EventReplayConfig

// ApplyReplaySources reads the event replay configuration and adds replay sources to the config
// A replay source is a copy of the original source with a different ID and source definition
// This replay source contains as destinations replay destinations which are copies of the original destinations but with a different ID
func (c *ConfigT) ApplyReplaySources() {
	if len(c.EventReplays) == 0 {
		return
	}
	originalSources := c.SourcesMap()
	originalDestinations := c.destinationsBySourceType()
	for _, replay := range c.EventReplays {
		sources := lo.OmitByValues(lo.MapValues(replay.Sources, func(value EventReplaySource, id string) *SourceT {
			s, ok := originalSources[value.OriginalSourceID]
			if !ok {
				return nil
			}
			newSource := *s
			newSource.ID = id
			newSource.OriginalID = s.ID
			newSource.WriteKey = id
			// no event uploads for replay sources
			if config, err := jsonparser.DeleteKey(s.Config, "eventUpload"); err == nil { // the only error here is an empty config, which leaves the copy as it is
				newSource.Config = config
			}
			newSource.Destinations = nil // destinations are added later
			return &newSource
		}), []*SourceT{nil})
		// add destinations to sources. The instance to copy depends on the source it is being
		// attached to, so it is resolved per connection rather than once per replay destination
		for _, connection := range replay.Connections {
			source, ok := sources[connection.SourceID]
			if !ok {
				continue
			}
			replayDestination, ok := replay.Destinations[connection.DestinationID]
			if !ok {
				continue
			}
			original, ok := destinationForSourceType(
				originalDestinations[replayDestination.OriginalDestinationID],
				sourceTypeOf(source.SourceDefinition.Name, source.SourceDefinition.Category),
			)
			if !ok {
				continue
			}
			newDestination := *original
			newDestination.ID = connection.DestinationID
			newDestination.OriginalID = original.ID
			newDestination.IsProcessorEnabled = true // processor is always enabled for replay destinations
			source.Destinations = append(source.Destinations, newDestination)
		}

		// add replay sources to config, only the ones that have destinations
		c.Sources = append(c.Sources, lo.FilterMap(lo.Values(sources), func(source *SourceT, _ int) (SourceT, bool) {
			return *source, len(source.Destinations) > 0
		})...)
	}
}

type EventReplayConfig struct {
	Sources      map[string]EventReplaySource      `json:"sources"`
	Destinations map[string]EventReplayDestination `json:"destinations"`
	Connections  []EventReplayConnection           `json:"connections"`
}

type EventReplaySource struct {
	OriginalSourceID string `json:"originalId"`
}

type EventReplayDestination struct {
	OriginalDestinationID string `json:"originalId"`
}

type EventReplayConnection struct {
	SourceID      string `json:"sourceId"`
	DestinationID string `json:"destinationId"`
}

// destinationsBySourceType indexes every destination instance by its id and by the source type of
// the source it is nested under.
//
// A destination connected to several sources appears once per source, and those copies are not
// interchangeable: the control plane filters a destination's config against the keys its definition
// declares for the source type it is connected to. Instances sharing a source type are identical,
// which is why one per type is enough.
func (c *ConfigT) destinationsBySourceType() map[string]map[string]*DestinationT {
	byID := make(map[string]map[string]*DestinationT)
	for i := range c.Sources {
		source := c.Sources[i]
		sourceType := sourceTypeOf(source.SourceDefinition.Name, source.SourceDefinition.Category)
		for j := range source.Destinations {
			destination := source.Destinations[j]
			byType, ok := byID[destination.ID]
			if !ok {
				byType = make(map[string]*DestinationT)
				byID[destination.ID] = byType
			}
			byType[sourceType] = &destination
		}
	}
	return byID
}

// destinationForSourceType picks the instance a replay source of the given type should copy.
//
// A replay does not have to name a source and a destination that were ever connected, so the
// instance for that exact source may not exist. In that case the first source type in ascending
// order is taken: an arbitrary choice, but a fixed one, where reading the destination out of a map
// keyed by id alone would return whichever instance the map happened to hold. Since that instance
// was filtered for a different source type, some source-type-specific config keys are stripped from
// a copy, so that we can operate against a "safe" config.
func destinationForSourceType(byType map[string]*DestinationT, sourceType string) (*DestinationT, bool) {
	if destination, ok := byType[sourceType]; ok {
		return destination, true
	}
	var (
		fallbackType string
		fallback     *DestinationT
	)
	for candidate, destination := range byType {
		if fallback == nil || candidate < fallbackType {
			fallbackType, fallback = candidate, destination
		}
	}
	if fallback == nil {
		return nil, false
	}
	stripped := *fallback
	stripped.Config = lo.OmitByKeys(fallback.Config, sourceTypeSpecificConfigKeys)
	return &stripped, true
}

// sourceTypeSpecificConfigKeys are config keys whose values are specific to the source type the
// destination instance was filtered for, so they are stripped from a fallback instance of a
// different source type: when the replay is for a connection that never existed, we assume no
// consent management filtering or event filtering based on connection mode should happen.
var sourceTypeSpecificConfigKeys = []string{
	"connectionMode",
	"consentManagement",
	"oneTrustCookieCategories",
	"ketchConsentPurposes",
}
