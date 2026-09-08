package backendconfig

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/logger"
)

func TestV2DestinationConfig(t *testing.T) {
	t.Run("version matched definition slice", func(t *testing.T) {
		// a definition whose current major is 2, with the first major archived
		versioned := v2DestinationDefinition{
			DestinationDefinitionT: DestinationDefinitionT{ID: "dd-1", Config: map[string]any{"major": "two"}},
			Version:                "2",
			Versions: map[string]v2DefinitionArchiveEntry{
				"1": {Config: map[string]any{"major": "one"}},
			},
		}
		unversioned := v2DestinationDefinition{
			DestinationDefinitionT: DestinationDefinitionT{ID: "dd-1", Config: map[string]any{"major": "flat"}},
		}

		for _, tc := range []struct {
			name       string
			definition v2DestinationDefinition
			major      int
			want       string
			wantErr    bool
		}{
			{"the current major is served from the flat config", versioned, 2, "two", false},
			{"an older major from the archive", versioned, 1, "one", false},
			{"a major above the current one is unknown", versioned, 3, "", true},
			{"a major below the first is unknown", versioned, 0, "", true},
			{"an unversioned definition serves its flat config", unversioned, 1, "flat", false},
			{"and keeps serving it for a major it has never heard of", unversioned, 7, "flat", false},
			{"even for a nonsensical one", unversioned, 0, "flat", false},
		} {
			t.Run(tc.name, func(t *testing.T) {
				config, err := tc.definition.configFor(tc.major)
				if tc.wantErr {
					require.ErrorIs(t, err, errUnknownVersion)
					return
				}
				require.NoError(t, err)
				require.Equal(t, tc.want, config["major"])
			})
		}

		t.Run("an unparseable definition version falls back to the first major", func(t *testing.T) {
			definition := v2DestinationDefinition{
				DestinationDefinitionT: DestinationDefinitionT{Config: map[string]any{"major": "flat"}},
				Version:                "not a number",
			}
			require.Equal(t, defaultDefinitionMajor, definition.currentMajor())
		})

		t.Run("a destination's own version is nullish coalesced, so an explicit zero stays zero", func(t *testing.T) {
			zero := 0
			require.Equal(t, defaultDefinitionMajor, resolveDefinitionMajor(nil))
			require.Equal(t, 0, resolveDefinitionMajor(&zero))
		})
	})

	t.Run("a destination pinned to a major its definition cannot resolve", func(t *testing.T) {
		catalogues := testCatalogues(destConfig([]any{"webhookUrl"}, nil))
		definition := catalogues.DestinationDefinitions["WEBHOOK"]
		definition.Version = "2"
		definition.Versions = map[string]v2DefinitionArchiveEntry{"1": {Config: destConfig([]any{"webhookUrl"}, nil)}}
		catalogues.DestinationDefinitions["WEBHOOK"] = definition

		body := `{
			"sources": {"src-1": {"sourceDefinitionName": "webhook"}},
			"destinations": {
				"dst-1": {"destinationDefinitionName": "WEBHOOK", "version": 9, "config": {"webhookUrl": "u"}},
				"dst-2": {"destinationDefinitionName": "WEBHOOK", "version": 1, "config": {"webhookUrl": "u"}}
			},
			"connections": {
				"conn-1": {"sourceId": "src-1", "destinationId": "dst-1"},
				"conn-2": {"sourceId": "src-1", "destinationId": "dst-2"}
			}
		}`
		mapper := newConfigMapper(logger.NOP)
		config, err := mapper.Map("ws-1", json.RawMessage(body), catalogues)
		require.NoError(t, err, "the workspace survives, only the destination is dropped")

		require.Len(t, config.Sources[0].Destinations, 1)
		require.Equal(t, "dst-2", config.Sources[0].Destinations[0].ID)

		// B7: its connection goes with it, or it would point at a destination under no source
		require.Len(t, config.Connections, 1)
		require.Contains(t, config.Connections, "conn-2")

		// reported once per process, not once per poll
		require.Contains(t, mapper.reportedUnresolvable, "dst-1")
		require.NotContains(t, mapper.reportedUnresolvable, "dst-2")
	})

	t.Run("a deleted destination is dropped without a word", func(t *testing.T) {
		catalogues := testCatalogues(destConfig([]any{"webhookUrl"}, nil))
		definition := catalogues.DestinationDefinitions["WEBHOOK"]
		definition.Version = "2"
		definition.Versions = map[string]v2DefinitionArchiveEntry{"1": {Config: destConfig(nil, nil)}}
		catalogues.DestinationDefinitions["WEBHOOK"] = definition

		body := `{
			"sources": {"src-1": {"sourceDefinitionName": "webhook"}},
			"destinations": {"dst-1": {"destinationDefinitionName": "WEBHOOK", "version": 9, "deleted": true}},
			"connections": {"conn-1": {"sourceId": "src-1", "destinationId": "dst-1"}}
		}`
		mapper := newConfigMapper(logger.NOP)
		config, err := mapper.Map("ws-1", json.RawMessage(body), catalogues)
		require.NoError(t, err)
		require.Empty(t, config.Sources[0].Destinations)
		require.Empty(t, mapper.reportedUnresolvable)
	})

	t.Run("per source config filtering", func(t *testing.T) {
		for _, tc := range []struct {
			name             string
			definition       map[string]any
			stored           map[string]any
			liveEventsConfig map[string]any
			sourceType       string
			want             map[string]any
		}{
			{
				name:       "keeps what the definition declares and drops the rest",
				definition: destConfig([]any{"webhookUrl"}, nil),
				stored:     map[string]any{"webhookUrl": "u", "undeclared": "dropped"},
				want:       map[string]any{"webhookUrl": "u"},
			},
			{
				name:       "a declared key the destination does not carry is left out, not nulled",
				definition: destConfig([]any{"webhookUrl", "absent"}, nil),
				stored:     map[string]any{"webhookUrl": "u"},
				want:       map[string]any{"webhookUrl": "u"},
			},
			{
				name:       "a declared key holding null is kept",
				definition: destConfig([]any{"webhookUrl"}, nil),
				stored:     map[string]any{"webhookUrl": nil},
				want:       map[string]any{"webhookUrl": nil},
			},
			{
				name:       "a dotted key is read and written by path",
				definition: destConfig([]any{"auth.token"}, nil),
				stored:     map[string]any{"auth": map[string]any{"token": "t", "other": "dropped"}},
				want:       map[string]any{"auth": map[string]any{"token": "t"}},
			},
			{
				name:       "a source type key is unwrapped from its per source type object",
				definition: destConfig(nil, map[string]any{"web": []any{"connectionMode"}}),
				stored:     map[string]any{"connectionMode": map[string]any{"web": "device", "cloud": "cloud"}},
				sourceType: "web",
				want:       map[string]any{"connectionMode": "device"},
			},
			{
				name:       "unless it holds nothing for this source type",
				definition: destConfig(nil, map[string]any{"web": []any{"connectionMode"}}),
				stored:     map[string]any{"connectionMode": map[string]any{"cloud": "cloud"}},
				sourceType: "web",
				want:       map[string]any{},
			},
			{
				// the reference guards on truthiness and then indexes: a falsy value fails the
				// guard, and a truthy one that is not an object has nothing at the source type
				name:       "a value that is not a per source type object is skipped",
				definition: destConfig(nil, map[string]any{"web": []any{"a", "b", "c", "d", "e"}}),
				stored: map[string]any{
					"a": false, "b": "", "c": nil, // falsy
					"d": "false", "e": float64(5), // truthy in JS, still not indexable
				},
				sourceType: "web",
				want:       map[string]any{},
			},
			{
				name:       "keys declared for another source type are not consulted",
				definition: destConfig(nil, map[string]any{"web": []any{"connectionMode"}}),
				stored:     map[string]any{"connectionMode": map[string]any{"web": "device"}},
				sourceType: "cloud",
				want:       map[string]any{},
			},
			{
				name:             "the live events flags are restored after filtering",
				definition:       destConfig([]any{"webhookUrl"}, nil),
				stored:           map[string]any{"webhookUrl": "u"},
				liveEventsConfig: map[string]any{"eventDelivery": true, "eventDeliveryTS": float64(1), "other": "dropped"},
				want: map[string]any{
					"webhookUrl": "u", "eventDelivery": true, "eventDeliveryTS": float64(1),
				},
			},
			{
				name:             "and only the ones that are there",
				definition:       destConfig([]any{"webhookUrl"}, nil),
				stored:           map[string]any{"webhookUrl": "u"},
				liveEventsConfig: map[string]any{"eventDelivery": true},
				want:             map[string]any{"webhookUrl": "u", "eventDelivery": true},
			},
			{
				name:       "rudderAccountId survives only because the definition declares it",
				definition: destConfig([]any{"rudderAccountId"}, nil),
				stored:     map[string]any{"rudderAccountId": "acc-1"},
				want:       map[string]any{"rudderAccountId": "acc-1"},
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				got, err := filterDestinationConfig(tc.stored, tc.liveEventsConfig, tc.definition, tc.sourceType)
				require.NoError(t, err)
				require.Equal(t, tc.want, got)
			})
		}

		t.Run("a definition with no destConfig is an error, not an empty config", func(t *testing.T) {
			_, err := filterDestinationConfig(map[string]any{"a": 1}, nil, map[string]any{}, "cloud")
			require.ErrorContains(t, err, "no destConfig")
		})
	})

	t.Run("source type", func(t *testing.T) {
		for _, tc := range []struct{ definitionName, category, want string }{
			{"anything", "cloud", "cloudSource"},
			{"anything", "singer", "cloudSource"},
			{"anything", "warehouse", "warehouse"},
			{"javascript", "", "web"},
			{"JavaScript", "", "web"},
			{"android_kotlin", "", "androidKotlin"},
			{"ios_swift", "", "iosSwift"},
			{"Android", "", "android"},
			{"shopify", "", "shopify"},
			{"webhook", "", "cloud"},
			{"HubSpot", "", "cloud"},
		} {
			t.Run(fmt.Sprintf("%s/%s", tc.definitionName, tc.category), func(t *testing.T) {
				require.Equal(t, tc.want, sourceTypeOf(tc.definitionName, tc.category))
			})
		}
	})

	t.Run("consent backfill", func(t *testing.T) {
		modern := func(provider string, consents ...string) []any {
			entries := make([]any, 0, len(consents))
			for _, consent := range consents {
				entries = append(entries, map[string]any{"consent": consent})
			}
			return []any{map[string]any{"provider": provider, "consents": entries}}
		}

		for _, tc := range []struct {
			name   string
			config map[string]any
			want   map[string]any
		}{
			{
				name:   "synthesizes the legacy array when it is absent",
				config: map[string]any{"consentManagement": modern("oneTrust", "C0001", "C0002")},
				want: map[string]any{"oneTrustCookieCategories": []any{
					map[string]any{"oneTrustCookieCategory": "C0001"},
					map[string]any{"oneTrustCookieCategory": "C0002"},
				}},
			},
			{
				name: "and when it holds nothing but empty consents",
				config: map[string]any{
					"consentManagement":    modern("ketch", "purpose-1"),
					"ketchConsentPurposes": []any{map[string]any{"purpose": ""}},
				},
				want: map[string]any{"ketchConsentPurposes": []any{map[string]any{"purpose": "purpose-1"}}},
			},
			{
				name: "leaves a populated legacy array alone",
				config: map[string]any{
					"consentManagement":        modern("oneTrust", "C0002"),
					"oneTrustCookieCategories": []any{map[string]any{"oneTrustCookieCategory": "C0001"}},
				},
				want: map[string]any{"oneTrustCookieCategories": []any{map[string]any{"oneTrustCookieCategory": "C0001"}}},
			},
			{
				name:   "does nothing when no provider matches",
				config: map[string]any{"consentManagement": modern("someoneElse", "C0001")},
				want:   map[string]any{},
			},
			{
				name:   "or when the modern config holds only empty consents",
				config: map[string]any{"consentManagement": modern("oneTrust", "")},
				want:   map[string]any{},
			},
			{
				name:   "nothing to do without a consentManagement key",
				config: map[string]any{"webhookUrl": "u"},
				want:   map[string]any{"webhookUrl": "u"},
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				got := backfillLegacyConsents(tc.config)
				delete(got, "consentManagement") // compared separately, it is passed through untouched
				require.Equal(t, tc.want, got)
			})
		}

		t.Run("both providers are backfilled from one consentManagement list", func(t *testing.T) {
			config := backfillLegacyConsents(map[string]any{"consentManagement": []any{
				map[string]any{"provider": "oneTrust", "consents": []any{map[string]any{"consent": "C0001"}}},
				map[string]any{"provider": "ketch", "consents": []any{map[string]any{"consent": "analytics"}}},
			}})
			require.Equal(t, []any{map[string]any{"oneTrustCookieCategory": "C0001"}}, config["oneTrustCookieCategories"])
			require.Equal(t, []any{map[string]any{"purpose": "analytics"}}, config["ketchConsentPurposes"])
		})
	})
}
