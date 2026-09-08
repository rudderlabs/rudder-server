package backendconfig

import (
	"encoding/json"
	"maps"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
)

// destConfig builds the shape a destination definition declares its keys in.
func destConfig(defaultConfig []any, perSourceType map[string]any) map[string]any {
	dest := map[string]any{"defaultConfig": defaultConfig}
	maps.Copy(dest, perSourceType)
	return map[string]any{"destConfig": dest}
}

// testCatalogues is a namespace catalogue with one source and one destination definition, both
// named after what the fixtures reference.
func testCatalogues(definitionConfig map[string]any) v2Catalogues {
	return v2Catalogues{
		SourceDefinitions: v2SourceDefinitions{
			"webhook":    {SourceDefinitionT: SourceDefinitionT{ID: "sd-1", Type: "cloud", Category: ""}},
			"javascript": {SourceDefinitionT: SourceDefinitionT{ID: "sd-2", Type: "web", Category: ""}},
		},
		DestinationDefinitions: v2DestinationDefinitions{
			"WEBHOOK": {DestinationDefinitionT: DestinationDefinitionT{
				ID: "dd-1", DisplayName: "Webhook", Config: definitionConfig,
			}},
		},
		AccountDefinitions: v2AccountDefinitions{
			"DESTINATION_SALESFORCE_OAUTH": {AccountDefinition: AccountDefinition{
				AuthenticationType: "oauth",
				Config:             map[string]any{"refreshOAuthToken": true},
			}},
		},
	}
}

func mapWorkspace(t *testing.T, body string, catalogues v2Catalogues) ConfigT {
	t.Helper()
	config, err := newConfigMapper(logger.NOP).Map("ws-1", json.RawMessage(body), catalogues)
	require.NoError(t, err)
	return config
}

// sourceIDs and destinationIDs read the order the mapper emitted, which is part of its contract.
func sourceIDs(config ConfigT) []string {
	ids := make([]string, 0, len(config.Sources))
	for _, source := range config.Sources {
		ids = append(ids, source.ID)
	}
	return ids
}

func destinationIDs(source SourceT) []string {
	ids := make([]string, 0, len(source.Destinations))
	for _, destination := range source.Destinations {
		ids = append(ids, destination.ID)
	}
	return ids
}

func TestV2Mapper(t *testing.T) {
	simpleDefinition := destConfig([]any{"webhookUrl"}, nil)

	t.Run("sources", func(t *testing.T) {
		body := `{
			"updatedAt": "2026-07-24T17:01:55.777Z",
			"sources": {
				"src-1": {
					"name": "My Source",
					"writeKey": "wk-1",
					"enabled": true,
					"transient": true,
					"sourceDefinitionName": "webhook",
					"config": {"foo": "bar"},
					"liveEventsConfig": {"eventUpload": true, "eventUploadTS": 1784909557360},
					"internalSecret": {"token": "s3cret"},
					"geoEnrichment": {"enabled": true}
				}
			}
		}`
		config := mapWorkspace(t, body, testCatalogues(simpleDefinition))

		require.Empty(t, config.WorkspaceID, "v1's namespace payload carries none either")
		require.Len(t, config.Sources, 1)
		source := config.Sources[0]

		// A2: the map key is the id, the workspace's key is the workspace id
		require.Equal(t, "src-1", source.ID)
		require.Equal(t, "ws-1", source.WorkspaceID)
		require.Equal(t, "My Source", source.Name)
		require.Equal(t, "wk-1", source.WriteKey)
		require.True(t, source.Enabled)
		require.True(t, source.Transient)

		// A3: the definition is looked up by name, and named by the key it is catalogued under
		require.Equal(t, SourceDefinitionT{ID: "sd-1", Name: "webhook", Type: "cloud"}, source.SourceDefinition)

		// A4: config and liveEventsConfig are merged into one object
		require.JSONEq(t, `{"foo":"bar","eventUpload":true,"eventUploadTS":1784909557360}`, string(source.Config))

		// A5, A14
		require.True(t, source.GeoEnrichment.Enabled)
		require.JSONEq(t, `{"token":"s3cret"}`, string(source.InternalSecret))
	})

	t.Run("a source with nothing on it", func(t *testing.T) {
		body := `{"sources": {"src-1": {"sourceDefinitionName": "webhook"}}}`
		config := mapWorkspace(t, body, testCatalogues(simpleDefinition))

		source := config.Sources[0]
		require.JSONEq(t, `{}`, string(source.Config), "an object, never null")
		require.False(t, source.GeoEnrichment.Enabled, "A5 is truthy, not nullish")
		require.Empty(t, source.Destinations)
		require.Empty(t, source.InternalSecret)
	})

	t.Run("a destination connected to two sources is emitted under both", func(t *testing.T) {
		body := `{
			"sources": {
				"src-1": {"sourceDefinitionName": "webhook"},
				"src-2": {"sourceDefinitionName": "webhook"}
			},
			"destinations": {
				"dst-1": {"name": "Webhook", "enabled": true, "destinationDefinitionName": "WEBHOOK",
					"config": {"webhookUrl": "https://example.com/hook"}}
			},
			"connections": {
				"conn-1": {"sourceId": "src-1", "destinationId": "dst-1", "processorEnabled": true},
				"conn-2": {"sourceId": "src-2", "destinationId": "dst-1", "processorEnabled": false}
			}
		}`
		config := mapWorkspace(t, body, testCatalogues(simpleDefinition))

		bySource := config.SourcesMap()
		require.Len(t, bySource["src-1"].Destinations, 1)
		require.Len(t, bySource["src-2"].Destinations, 1)
		first, second := bySource["src-1"].Destinations[0], bySource["src-2"].Destinations[0]
		require.Equal(t, "dst-1", first.ID)
		require.Equal(t, "dst-1", second.ID)
		require.True(t, first.IsProcessorEnabled, "taken from the connection, not the destination")
		require.False(t, second.IsProcessorEnabled)

		// v1 serializes a destination once per source, so the two copies never share a config map
		first.Config["webhookUrl"] = "https://example.com/mutated"
		require.Equal(t, "https://example.com/hook", second.Config["webhookUrl"])

		// B7: connections are emitted as they arrived
		require.Len(t, config.Connections, 2)
		require.Equal(t, Connection{SourceID: "src-1", DestinationID: "dst-1", ProcessorEnabled: true},
			config.Connections["conn-1"])
	})

	t.Run("the same body maps to the same document, every time", func(t *testing.T) {
		// ranging the maps in map order would permute a source's destinations on every remap,
		// which configUpdate reads as a config change and republishes the namespace over
		body := `{
			"sources": {
				"src-2": {"sourceDefinitionName": "webhook"},
				"src-1": {"sourceDefinitionName": "webhook"}
			},
			"destinations": {
				"dst-1": {"destinationDefinitionName": "WEBHOOK"},
				"dst-2": {"destinationDefinitionName": "WEBHOOK"},
				"dst-3": {"destinationDefinitionName": "WEBHOOK"},
				"dst-4": {"destinationDefinitionName": "WEBHOOK"}
			},
			"connections": {
				"conn-4": {"sourceId": "src-1", "destinationId": "dst-4"},
				"conn-3": {"sourceId": "src-1", "destinationId": "dst-3"},
				"conn-2": {"sourceId": "src-1", "destinationId": "dst-2"},
				"conn-1": {"sourceId": "src-1", "destinationId": "dst-1"}
			}
		}`
		first := mapWorkspace(t, body, testCatalogues(simpleDefinition))
		require.Equal(t, []string{"src-1", "src-2"}, sourceIDs(first), "sources come out in id order")
		require.Equal(t, []string{"dst-1", "dst-2", "dst-3", "dst-4"}, destinationIDs(first.Sources[0]),
			"and a source's destinations in the id order of the connections that carry them")

		// require.Equal is reflect.DeepEqual for a struct, which is what configUpdate compares with
		for i := range 50 {
			require.Equal(t, first, mapWorkspace(t, body, testCatalogues(simpleDefinition)),
				"remap %d differs from the first", i)
		}
	})

	t.Run("destinations", func(t *testing.T) {
		body := `{
			"sources": {"src-1": {"sourceDefinitionName": "webhook"}},
			"destinations": {
				"dst-1": {
					"name": "Webhook", "enabled": true, "revisionId": "rev-1",
					"destinationDefinitionName": "WEBHOOK",
					"config": {"webhookUrl": "https://example.com/hook"},
					"liveEventsConfig": {"eventDelivery": true, "eventDeliveryTS": 1784909557360},
					"transformationIds": ["tr-1"],
					"regions": [{"region": "eu", "config": {"webhookUrl": "https://eu.example.com", "apiKey": "leaked"}}]
				}
			},
			"transformations": {
				"tr-1": {"versionId": "v-1", "language": "javascript", "liveEventsConfig": {"eventTransform": true}}
			},
			"connections": {"conn-1": {"sourceId": "src-1", "destinationId": "dst-1", "processorEnabled": true}}
		}`
		config := mapWorkspace(t, body, testCatalogues(simpleDefinition))
		destination := config.Sources[0].Destinations[0]

		require.Equal(t, "dst-1", destination.ID)
		require.Equal(t, "Webhook", destination.Name)
		require.Equal(t, "ws-1", destination.WorkspaceID)
		require.Equal(t, "rev-1", destination.RevisionID)
		require.True(t, destination.Enabled)
		require.Equal(t, 1, destination.Version, "A10: absent means the first major")

		// A3/B2: the definition is named by its catalogue key
		require.Equal(t, "WEBHOOK", destination.DestinationDefinition.Name)
		require.Equal(t, "Webhook", destination.DestinationDefinition.DisplayName)

		// A8
		require.Equal(t, []TransformationT{{
			ID: "tr-1", VersionID: "v-1", Language: "javascript",
			Config: map[string]any{"eventTransform": true},
		}}, destination.Transformations)

		// A9 + B3(c): the live events flags survive the config rebuild
		require.Equal(t, map[string]any{
			"webhookUrl":      "https://example.com/hook",
			"eventDelivery":   true,
			"eventDeliveryTS": float64(1784909557360),
		}, destination.Config)

		// A13: regions[] never reaches a ConfigT - under secrets=embed it carries credentials
		// rudder-server has never held
		marshalled, err := jsonrs.Marshal(config)
		require.NoError(t, err)
		require.NotContains(t, string(marshalled), "leaked")
		require.NotContains(t, string(marshalled), "eu.example.com")
	})

	t.Run("pass-throughs", func(t *testing.T) {
		body := `{
			"updatedAt": "2026-07-24T17:01:55.777Z",
			"sources": {},
			"libraries": [{"versionId": "lib-1"}],
			"credentials": {"cred-1": {"key": "authToken", "value": "v", "isSecret": true}},
			"accounts": {"acc-1": {"accountDefinitionName": "DESTINATION_SALESFORCE_OAUTH", "options": {"o": 1}}},
			"eventReplays": {"er-1": {"sources": {"er-s-1": {"originalId": "src-1"}}}},
			"settings": {"eventAuditEnabled": true, "dataRetention": {"useSelfStorage": false}}
		}`
		config := mapWorkspace(t, body, testCatalogues(simpleDefinition))

		require.Equal(t, "2026-07-24T17:01:55.777Z", config.UpdatedAt.Format(updatedAfterTimeFormat))
		require.Equal(t, LibrariesT{{VersionID: "lib-1"}}, config.Libraries)
		// A11: credentials reach the destination transformer, they are not decoration
		require.Equal(t, map[string]Credential{"cred-1": {Key: "authToken", Value: "v", IsSecret: true}}, config.Credentials)
		require.Equal(t, "DESTINATION_SALESFORCE_OAUTH", config.Accounts["acc-1"].AccountDefinitionName)
		require.Empty(t, config.Accounts["acc-1"].ID,
			"v1 leaves the id in the key too, processAccountAssociations stamps it later")
		require.Contains(t, config.EventReplays, "er-1")
		require.True(t, config.Settings.EventAuditEnabled)
		require.False(t, config.Settings.DataRetention.UseSelfStorage)

		// A12: the whole catalogue, not just what this workspace references
		require.Equal(t, map[string]AccountDefinition{
			"DESTINATION_SALESFORCE_OAUTH": {
				Name:               "DESTINATION_SALESFORCE_OAUTH",
				AuthenticationType: "oauth",
				Config:             map[string]any{"refreshOAuthToken": true},
			},
		}, config.AccountDefinitions)
	})

	t.Run("settings default to the control plane's when absent", func(t *testing.T) {
		config := mapWorkspace(t, `{"sources": {}}`, testCatalogues(simpleDefinition))
		require.Equal(t, Settings{DataRetention: DataRetention{UseSelfStorage: true}}, config.Settings)
	})

	t.Run("tracking plans", func(t *testing.T) {
		for _, tc := range []struct {
			name, plan   string
			wantAttached bool
		}{
			{"attached when enabled and the plan exists", `{"trackingPlanId": "tp-1", "version": 3, "enabled": true}`, true},
			{"not attached when disabled", `{"trackingPlanId": "tp-1", "version": 3, "enabled": false}`, false},
			{"not attached when deleted", `{"trackingPlanId": "tp-1", "version": 3, "enabled": true, "deleted": true}`, false},
			{"not attached when the plan is unknown", `{"trackingPlanId": "tp-404", "version": 3, "enabled": true}`, false},
			{"not attached without a plan id", `{"version": 3, "enabled": true}`, false},
		} {
			t.Run(tc.name, func(t *testing.T) {
				body := `{
					"sources": {"src-1": {"sourceDefinitionName": "webhook", "dgSourceTrackingPlanConfig": ` + tc.plan + `}},
					"trackingPlans": {"tp-1": {"version": 7}}
				}`
				config := mapWorkspace(t, body, testCatalogues(simpleDefinition))
				planConfig := config.Sources[0].DgSourceTrackingPlanConfig

				require.Equal(t, "src-1", planConfig.SourceId, "A6 adds the source id")
				require.Equal(t, 3, planConfig.SourceConfigVersion)
				if tc.wantAttached {
					require.Equal(t, TrackingPlanT{Id: "tp-1", Version: 7}, planConfig.TrackingPlan)
				} else {
					require.Empty(t, planConfig.TrackingPlan)
				}
			})
		}
	})

	t.Run("unknown references fail the workspace, as they do in the control plane", func(t *testing.T) {
		for _, tc := range []struct{ name, body, wantErr string }{
			{
				"source definition",
				`{"sources": {"src-1": {"sourceDefinitionName": "nope"}}}`,
				`unknown source definition "nope"`,
			},
			{
				"destination",
				`{"sources": {"src-1": {"sourceDefinitionName": "webhook"}},
				  "connections": {"c-1": {"sourceId": "src-1", "destinationId": "nope"}}}`,
				`unknown destination "nope"`,
			},
			{
				"destination definition",
				`{"sources": {"src-1": {"sourceDefinitionName": "webhook"}},
				  "destinations": {"dst-1": {"destinationDefinitionName": "nope"}},
				  "connections": {"c-1": {"sourceId": "src-1", "destinationId": "dst-1"}}}`,
				`unknown destination definition "nope"`,
			},
			{
				"transformation",
				`{"sources": {"src-1": {"sourceDefinitionName": "webhook"}},
				  "destinations": {"dst-1": {"destinationDefinitionName": "WEBHOOK", "transformationIds": ["nope"]}},
				  "connections": {"c-1": {"sourceId": "src-1", "destinationId": "dst-1"}}}`,
				`unknown transformation "nope"`,
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				_, err := newConfigMapper(logger.NOP).Map("ws-1", json.RawMessage(tc.body), testCatalogues(simpleDefinition))
				require.ErrorContains(t, err, tc.wantErr)
			})
		}
	})

	// The resolver caches the v2 payload and the catalogues and re-maps from them on every generation
	// change, so anything the mapper hands out that still points into that cache is a corruption
	// waiting to happen: one in-place write by any consumer and every later re-derivation inherits it.
	t.Run("does not alias the cache", func(t *testing.T) {
		catalogues := testCatalogues(destConfig([]any{"webhookUrl"}, nil))
		body := json.RawMessage(`{
			"updatedAt": "2026-07-24T17:01:55.777Z",
			"sources": {"src-1": {"name": "My Source", "sourceDefinitionName": "webhook", "config": {"foo": "bar"}}},
			"destinations": {"dst-1": {"name": "Webhook", "destinationDefinitionName": "WEBHOOK",
				"config": {"webhookUrl": "https://example.com/hook"}}},
			"connections": {"conn-1": {"sourceId": "src-1", "destinationId": "dst-1"}},
			"accounts": {"acc-1": {"accountDefinitionName": "DESTINATION_SALESFORCE_OAUTH", "options": {"o": 1}}},
			"credentials": {"cred-1": {"key": "authToken", "value": "v"}},
			"libraries": [{"versionId": "lib-1"}]
		}`)
		mapper := newConfigMapper(logger.NOP)

		first, err := mapper.Map("ws-1", body, catalogues)
		require.NoError(t, err)

		// a consumer holding the mapped config writes all over it
		first.Sources[0].Name = "renamed"
		first.Sources[0].Destinations[0].Name = "renamed"
		first.Sources[0].Destinations[0].Config["webhookUrl"] = "https://example.com/mutated"
		first.Sources[0].Destinations[0].DestinationDefinition.Config["destConfig"] = "clobbered"
		first.Sources[0].Destinations[0].DestinationDefinition.DisplayName = "clobbered"
		first.AccountDefinitions["DESTINATION_SALESFORCE_OAUTH"].Config["clobbered"] = true
		first.Accounts["acc-1"].Options["o"] = "clobbered"
		first.Credentials["cred-1"] = Credential{Key: "clobbered"}
		first.Libraries[0].VersionID = "clobbered"
		delete(first.Connections, "conn-1")

		second, err := mapper.Map("ws-1", body, catalogues)
		require.NoError(t, err)

		require.Equal(t, "My Source", second.Sources[0].Name)
		require.Equal(t, "Webhook", second.Sources[0].Destinations[0].Name)
		require.Equal(t, "https://example.com/hook", second.Sources[0].Destinations[0].Config["webhookUrl"])
		require.Equal(t, "Webhook", second.Sources[0].Destinations[0].DestinationDefinition.DisplayName)
		require.Equal(t, destConfig([]any{"webhookUrl"}, nil)["destConfig"],
			second.Sources[0].Destinations[0].DestinationDefinition.Config["destConfig"])
		require.NotContains(t, second.AccountDefinitions["DESTINATION_SALESFORCE_OAUTH"].Config, "clobbered")
		require.Equal(t, float64(1), second.Accounts["acc-1"].Options["o"])
		require.Equal(t, Credential{Key: "authToken", Value: "v"}, second.Credentials["cred-1"])
		require.Equal(t, LibrariesT{{VersionID: "lib-1"}}, second.Libraries)
		require.Contains(t, second.Connections, "conn-1")

		// and the catalogue everything is mapped against is still what it was
		require.Equal(t, "Webhook", catalogues.DestinationDefinitions["WEBHOOK"].DisplayName)
		require.Equal(t, destConfig([]any{"webhookUrl"}, nil), catalogues.DestinationDefinitions["WEBHOOK"].Config)
		require.Equal(t, map[string]any{"refreshOAuthToken": true},
			catalogues.AccountDefinitions["DESTINATION_SALESFORCE_OAUTH"].Config)
	})

	// ApplyReplaySources copies a destination by value, so a replay destination and the destination it
	// was copied from share one Config map. That is v1's behaviour, and the post-transform steps run on
	// both paths - reproduce it rather than fix it, or shadow mode reports a divergence that isn't one.
	t.Run("replay sources share their destination config", func(t *testing.T) {
		body := json.RawMessage(`{
			"sources": {"src-1": {"sourceDefinitionName": "webhook", "config": {"eventUpload": true}}},
			"destinations": {"dst-1": {"destinationDefinitionName": "WEBHOOK",
				"config": {"webhookUrl": "https://example.com/hook"}}},
			"connections": {"conn-1": {"sourceId": "src-1", "destinationId": "dst-1"}},
			"eventReplays": {"er-1": {
				"sources": {"er-s-1": {"originalId": "src-1"}},
				"destinations": {"er-d-1": {"originalId": "dst-1"}},
				"connections": [{"sourceId": "er-s-1", "destinationId": "er-d-1"}]
			}}
		}`)
		config, err := newConfigMapper(logger.NOP).Map("ws-1", body, testCatalogues(destConfig([]any{"webhookUrl"}, nil)))
		require.NoError(t, err)

		config.ApplyReplaySources()
		require.Len(t, config.Sources, 2)
		sources := config.SourcesMap()
		original, replay := sources["src-1"], sources["er-s-1"]
		require.Equal(t, "src-1", replay.OriginalID)

		// the replay source's own config is a fresh document with eventUpload dropped
		require.JSONEq(t, `{"eventUpload":true}`, string(original.Config))
		require.JSONEq(t, `{}`, string(replay.Config))

		// its destination's config, though, is the very same map
		require.Len(t, replay.Destinations, 1)
		replay.Destinations[0].Config["webhookUrl"] = "https://example.com/mutated"
		require.Equal(t, "https://example.com/mutated", original.Destinations[0].Config["webhookUrl"],
			"shared, as it is on the v1 path")
	})
}
