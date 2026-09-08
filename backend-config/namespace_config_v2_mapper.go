package backendconfig

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
)

var _ mapper = &configMapper{}

// errUnknownVersion is raised when a destination is pinned to a major its definition cannot
// resolve. It skips the destination rather than failing the workspace, mirroring the control
// plane: failing would drop delivery for every destination in the workspace.
var errUnknownVersion = errors.New("unknown destination definition version")

// defaultDefinitionMajor is the major a destination with no version of its own resolves to.
const defaultDefinitionMajor = 1

// configMapper turns one workspace's v2 body into the v1 ConfigT.
// It is a port of rudder-config-backend's transformer, which builds the same
// document server side, and follows it even where it is surprising: divergences would surface as
// shadow-mode noise or, worse, as behaviour changes downstream.
type configMapper struct {
	logger logger.Logger
	// destinations whose version could not be resolved are reported once per process rather than
	// once per poll: the poll loop would otherwise log the same destination every few seconds.
	// Only ever touched from the poll goroutine.
	reportedUnresolvable map[string]struct{}
}

func newConfigMapper(log logger.Logger) *configMapper {
	return &configMapper{logger: log, reportedUnresolvable: make(map[string]struct{})}
}

func (m *configMapper) Map(workspaceID string, raw json.RawMessage, catalogues v2Catalogues) (ConfigT, error) {
	var workspace v2WorkspaceBody
	if err := jsonrs.Unmarshal(raw, &workspace); err != nil {
		return ConfigT{}, fmt.Errorf("unmarshalling workspace: %w", err)
	}

	// v1 nests destinations under sources, v2 joins them through connections (A7 in design doc).
	// Ranged in id order, here and over the sources below, for a repeatable output: configUpdate
	// compares each poll's result with the previous one.
	connectionsBySource := make(map[string][]Connection, len(workspace.Sources))
	for _, connectionID := range slices.Sorted(maps.Keys(workspace.Connections)) {
		connection := workspace.Connections[connectionID]
		connectionsBySource[connection.SourceID] = append(connectionsBySource[connection.SourceID], connection)
	}

	skippedDestinations := make(map[string]struct{}) // if a destination needs to be skipped we are filtering out its connection below
	sources := make([]SourceT, 0, len(workspace.Sources))
	for _, sourceID := range slices.Sorted(maps.Keys(workspace.Sources)) {
		mapped, err := m.mapSource(workspaceID, sourceID, workspace.Sources[sourceID], workspace, catalogues, connectionsBySource[sourceID], skippedDestinations)
		if err != nil {
			return ConfigT{}, err
		}
		sources = append(sources, mapped)
	}

	// connections are emitted as they are, minus the ones whose destination was skipped (B7 in design doc)
	connections := lo.OmitBy(workspace.Connections, func(_ string, c Connection) bool {
		_, skipped := skippedDestinations[c.DestinationID]
		return skipped
	})

	// pass-throughs (A11 in design doc). The account definition catalogue is kept whole
	// (A12 in design doc) - v1 prunes it to what the workspace references, purely to keep its
	// per-workspace copy small
	accountDefinitions := lo.MapValues(catalogues.AccountDefinitions,
		func(definition v2AccountDefinition, name string) AccountDefinition { return definition.toV1(name) })

	return ConfigT{
		// WorkspaceID is deliberately not set: v1's namespace payload has no per workspace
		// workspaceId either, so a ConfigT built from it carries an empty one. Nothing below
		// backend-config should be able to tell which endpoint produced its config
		Sources:      sources,
		Libraries:    workspace.Libraries,
		Settings:     lo.FromPtrOr(workspace.Settings, defaultSettings()),
		UpdatedAt:    workspace.UpdatedAt,
		EventReplays: workspace.EventReplays,
		Credentials:  workspace.Credentials,
		Connections:  connections,
		// Account.ID stays empty here, as it does on the v1 path: processAccountAssociations
		// stamps it on the copy it attaches to a destination
		Accounts:           workspace.Accounts,
		AccountDefinitions: accountDefinitions,
	}, nil
}

// defaultSettings is what the control plane serves for a workspace with no settings row.
func defaultSettings() Settings {
	return Settings{
		DataRetention: DataRetention{
			UseSelfStorage: true,
		},
	}
}

func (m *configMapper) mapSource(
	workspaceID, sourceID string,
	source v2Source,
	workspace v2WorkspaceBody,
	catalogues v2Catalogues,
	connections []Connection,
	skippedDestinations map[string]struct{},
) (SourceT, error) {
	// definitions are catalogued by name and carry none of their own (A3 in design doc)
	definition, ok := catalogues.SourceDefinitions[source.SourceDefinitionName]
	if !ok {
		return SourceT{}, fmt.Errorf("source %q references unknown source definition %q", sourceID, source.SourceDefinitionName)
	}
	sourceDefinition := definition.toV1(source.SourceDefinitionName)

	destinations, err := m.mapDestinations(workspaceID, sourceID, sourceDefinition, workspace, catalogues, connections, skippedDestinations)
	if err != nil {
		return SourceT{}, err
	}

	// the flat config and liveEventsConfig are merged into one object (A4 in design doc)
	config, err := jsonrs.Marshal(lo.Assign(source.Config, source.LiveEventsConfig))
	if err != nil {
		return SourceT{}, fmt.Errorf("marshalling config of source %q: %w", sourceID, err)
	}

	mapped := SourceT{
		ID:               sourceID,    // the map key is the id (A2 in design doc)
		WorkspaceID:      workspaceID, // and the workspace's key is the workspace id (A2 in design doc)
		Name:             source.Name,
		SourceDefinition: sourceDefinition,
		Config:           config,
		Enabled:          source.Enabled,
		Destinations:     destinations,
		WriteKey:         source.WriteKey,
		Transient:        source.Transient,
		InternalSecret:   source.InternalSecret, // load bearing: the hydrate path forwards it (A14 in design doc)
	}
	// truthy, not nullish - an absent geoEnrichment is disabled (A5 in design doc)
	mapped.GeoEnrichment.Enabled = lo.FromPtr(source.GeoEnrichment).Enabled

	// the tracking plan is attached only to a source that has one, enabled and not deleted, and
	// only when the plan it names exists in this workspace (A6 in design doc)
	if plan := source.DgSourceTrackingPlanConfig; plan != nil {
		mapped.DgSourceTrackingPlanConfig = DgSourceTrackingPlanConfigT{
			SourceId:            sourceID,
			SourceConfigVersion: plan.Version,
			Config:              plan.Config,
			Deleted:             plan.Deleted,
		}
		trackingPlan, ok := workspace.TrackingPlans[plan.TrackingPlanID]
		if !plan.Deleted && plan.Enabled && plan.TrackingPlanID != "" && ok {
			mapped.DgSourceTrackingPlanConfig.TrackingPlan = TrackingPlanT{
				Id:      plan.TrackingPlanID,
				Version: trackingPlan.Version,
			}
		}
	}

	return mapped, nil
}

func (m *configMapper) mapDestinations(
	workspaceID, sourceID string,
	sourceDefinition SourceDefinitionT,
	workspace v2WorkspaceBody,
	catalogues v2Catalogues,
	connections []Connection,
	skippedDestinations map[string]struct{},
) ([]DestinationT, error) {
	destinations := make([]DestinationT, 0, len(connections))
	for _, connection := range connections {
		destination, ok := workspace.Destinations[connection.DestinationID]
		if !ok {
			return nil, fmt.Errorf("source %q is connected to unknown destination %q", sourceID, connection.DestinationID)
		}
		definition, ok := catalogues.DestinationDefinitions[destination.DestinationDefinitionName]
		if !ok {
			return nil, fmt.Errorf("destination %q references unknown destination definition %q",
				connection.DestinationID, destination.DestinationDefinitionName)
		}

		// the definition this destination is delivered with carries the config of the major it is
		// pinned to, which may be an archived one (B2 in design doc)
		definitionV1, major, err := definition.toV1(destination.DestinationDefinitionName, destination.Version)
		if errors.Is(err, errUnknownVersion) {
			// skip the row rather than fail the workspace, and let the connection pruning drop its
			// connection with it (B7 in design doc)
			skippedDestinations[connection.DestinationID] = struct{}{}
			m.reportUnresolvable(workspaceID, connection.DestinationID, destination, major)
			continue
		} else if err != nil {
			return nil, err
		}

		// the config is rebuilt from the keys the definition declares for this source's type
		// (B3 in design doc)
		config, err := filterDestinationConfig(destination.Config, destination.LiveEventsConfig,
			definitionV1.Config, sourceTypeOf(sourceDefinition.Name, sourceDefinition.Category))
		if err != nil {
			return nil, fmt.Errorf("filtering config of destination %q: %w", connection.DestinationID, err)
		}
		// the legacy consent keys are backfilled last, from the config just rebuilt (B4 in design doc)
		config = backfillLegacyConsents(config)

		mapped := DestinationT{
			ID:                 connection.DestinationID,
			Name:               destination.Name,
			Config:             config,
			Enabled:            destination.Enabled,
			WorkspaceID:        workspaceID,
			IsProcessorEnabled: connection.ProcessorEnabled,
			RevisionID:         destination.RevisionID,
			// the major its definition was resolved for (A10 in design doc)
			Version:               major,
			DestinationDefinition: definitionV1,
		}

		// the slice is always there, empty included, as v1 serializes it (A8 in design doc)
		mapped.Transformations, err = lo.MapErr(destination.TransformationIDs,
			func(transformationID string, _ int) (TransformationT, error) {
				transformation, ok := workspace.Transformations[transformationID]
				if !ok {
					return TransformationT{}, fmt.Errorf("destination %q references unknown transformation %q",
						connection.DestinationID, transformationID)
				}
				return TransformationT{
					ID:        transformationID,
					VersionID: transformation.VersionID,
					Language:  transformation.Language,
					Config:    transformation.LiveEventsConfig,
				}, nil
			})
		if err != nil {
			return nil, err
		}

		destinations = append(destinations, mapped)
	}
	return destinations, nil
}

func (m *configMapper) reportUnresolvable(workspaceID, destinationID string, destination v2Destination, major int) {
	if destination.Deleted { // a soft deleted row is skipped silently
		return
	}
	if _, reported := m.reportedUnresolvable[destinationID]; reported {
		return
	}
	m.reportedUnresolvable[destinationID] = struct{}{}
	m.logger.Errorn("Excluding destination from workspace config: its version cannot be resolved",
		obskit.WorkspaceID(workspaceID),
		obskit.DestinationID(destinationID),
		logger.NewIntField("version", int64(major)),
		logger.NewStringField("destinationDefinition", destination.DestinationDefinitionName),
	)
}

// resolveDefinitionMajor mirrors the control plane's nullish coalesce: absent and null mean the
// default major, an explicit 0 does not and resolves to no config at all.
func resolveDefinitionMajor(version *int) int {
	if version == nil {
		return defaultDefinitionMajor
	}
	return *version
}

// currentMajor is the major served by the definition's own config. The wire carries a dotted
// version ("1.0"), so the major is the leading component; anything unparseable or below the first
// major resolves to the default, so callers never see a maximum of zero.
func (d v2DestinationDefinition) currentMajor() int {
	major, err := strconv.Atoi(strings.SplitN(d.Version, ".", 2)[0])
	if err != nil || major < defaultDefinitionMajor {
		return defaultDefinitionMajor
	}
	return major
}

// configFor returns the config a destination pinned to the given major should be given: the
// definition's own config for the current major, the archive's for a superseded one.
//
// A major it cannot resolve degrades to the definition's own config when there is no archive at
// all - that config is then the only one there is, and serving it beats dropping the destination.
// A definition that does carry an archive raises errUnknownVersion instead: that is a genuine
// unknown major rather than an unversioned definition.
func (d v2DestinationDefinition) configFor(major int) (map[string]any, error) {
	current := d.currentMajor()
	switch {
	case major == current:
		return d.Config, nil
	case major >= defaultDefinitionMajor && major < current:
		if entry, ok := d.Versions[strconv.Itoa(major)]; ok {
			return entry.Config, nil
		}
	}
	if len(d.Versions) == 0 {
		return d.Config, nil
	}
	return nil, fmt.Errorf("%w: %d, the newest known is %d", errUnknownVersion, major, current)
}
