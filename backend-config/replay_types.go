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
	originalDestinations := c.DestinationsMap()
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
			newSource.Config = lo.OmitByKeys(newSource.Config, []string{"eventUpload"}) // no event uploads for replay sources for now
			newSource.Destinations = nil                                                // destinations are added later
			return &newSource
		}), []*SourceT{nil})
		destinations := lo.OmitByValues(lo.MapValues(replay.Destinations, func(value EventReplayDestination, id string) *DestinationT {
			d, ok := originalDestinations[value.OriginalDestinationID]
			if !ok {
				return nil
			}
			newDestination := *d
			newDestination.ID = id
			newDestination.IsProcessorEnabled = true // processor is always enabled for replay destinations
			return &newDestination
		}), []*DestinationT{nil})

		// add destinations to sources
		for _, connection := range replay.Connections {
			source, ok := sources[connection.SourceID]
			if !ok {
				continue
			}
			destination, ok := destinations[connection.DestinationID]
			if !ok {
				continue
			}
			source.Destinations = append(source.Destinations, *destination)
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
