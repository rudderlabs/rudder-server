package processor

import (
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/logger"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/processor/types"
	reportingtypes "github.com/rudderlabs/rudder-server/utils/types"
)

// newClassifyTestHandle builds a bare Handle wired the same way TestFilterDestinations does
// (consent_test.go:1379-1394), plus sourceIdDestinationMap so classifyDestinations can discover
// candidate destinations without a full NewHandle/backend-config subscription.
func newClassifyTestHandle(sourceID string, destinations []backendconfig.DestinationT) *Handle {
	proc := &Handle{}
	proc.config.sourceIdDestinationMap = map[string][]backendconfig.DestinationT{sourceID: destinations}
	proc.config.oneTrustConsentCategoriesMap = make(map[string][]string)
	proc.config.ketchConsentCategoriesMap = make(map[string][]string)
	proc.config.genericConsentManagementMap = make(SourceConsentMap)
	proc.config.genericConsentManagementMap[SourceID(sourceID)] = make(DestConsentMap)
	for _, dest := range destinations {
		proc.config.oneTrustConsentCategoriesMap[dest.ID] = getOneTrustConsentCategories(&dest)
		proc.config.ketchConsentCategoriesMap[dest.ID] = getKetchConsentCategories(&dest)
		proc.config.genericConsentManagementMap[SourceID(sourceID)][DestinationID(dest.ID)], _ = getGenericConsentManagementData(&dest)
	}
	proc.logger = logger.NewLogger().Child("processor")
	return proc
}

func webhookDest(id string, enabled bool) backendconfig.DestinationT {
	return backendconfig.DestinationT{
		ID:      id,
		Name:    id,
		Enabled: enabled,
		DestinationDefinition: backendconfig.DestinationDefinitionT{
			ID:          "webhook-def",
			Name:        "WEBHOOK",
			DisplayName: "Webhook",
		},
	}
}

func amplitudeDest(id string, enabled bool) backendconfig.DestinationT {
	return backendconfig.DestinationT{
		ID:      id,
		Name:    id,
		Enabled: enabled,
		DestinationDefinition: backendconfig.DestinationDefinitionT{
			ID:          "amplitude-def",
			Name:        "AM",
			DisplayName: "Amplitude",
		},
	}
}

// consentDeniedDest returns a destination whose oneTrustCookieCategories will exclude it once the
// event denies "cat-1" (same shape as consent_test.go's TestFilterDestinations fixtures).
func consentDeniedDest(base backendconfig.DestinationT) backendconfig.DestinationT {
	base.Config = map[string]any{
		"oneTrustCookieCategories": []any{
			map[string]any{"oneTrustCookieCategory": "cat-1"},
		},
	}
	return base
}

func deniedConsentEvent() types.SingularEventT {
	return types.SingularEventT{
		"context": map[string]any{
			"consentManagement": map[string]any{
				"deniedConsentIds": []any{"cat-1"},
			},
		},
	}
}

// integrationsExcludingEvent builds an `integrations` object opting out of a destination type,
// keyed by DisplayName — FilterClientIntegrations ranges over destNameIDMap keyed by DisplayName
// (processor.go:1005, integrations.go:52).
func integrationsExcludingEvent(destDisplayName string) types.SingularEventT {
	return types.SingularEventT{
		"integrations": map[string]any{
			destDisplayName: false,
		},
	}
}

func excludedIDs(excluded []excludedDestination) []string {
	return lo.Map(excluded, func(e excludedDestination, _ int) string {
		return e.destination.ID
	})
}

func availableIDs(available []backendconfig.DestinationT) []string {
	return lo.Map(available, func(d backendconfig.DestinationT, _ int) string {
		return d.ID
	})
}

func TestClassifyDestinations(t *testing.T) {
	t.Run("all candidates survive", func(t *testing.T) {
		sourceID := "source-1"
		dests := []backendconfig.DestinationT{webhookDest("dest-1", true), amplitudeDest("dest-2", true)}
		proc := newClassifyTestHandle(sourceID, dests)

		available, excluded := proc.classifyDestinations(types.SingularEventT{}, sourceID, "")

		require.ElementsMatch(t, []string{"dest-1", "dest-2"}, availableIDs(available))
		require.Nil(t, excluded)
	})

	t.Run("an integrations object disabling one destination type excludes every destination of that type", func(t *testing.T) {
		sourceID := "source-1"
		dests := []backendconfig.DestinationT{
			webhookDest("dest-1", true),
			amplitudeDest("dest-2", true),
			amplitudeDest("dest-3", true),
		}
		proc := newClassifyTestHandle(sourceID, dests)

		available, excluded := proc.classifyDestinations(integrationsExcludingEvent("Amplitude"), sourceID, "")

		require.ElementsMatch(t, []string{"dest-1"}, availableIDs(available))
		require.ElementsMatch(t, []string{"dest-2", "dest-3"}, excludedIDs(excluded))
		for _, e := range excluded {
			require.Equal(t, reportingtypes.FilteredIntegrationStatus, e.reason)
			require.Equal(t, reportingtypes.FilterEventCode, e.statusCode)
		}
	})

	t.Run("a consent-denied destination is excluded with reason filtered_consent and code 297", func(t *testing.T) {
		sourceID := "source-1"
		dests := []backendconfig.DestinationT{consentDeniedDest(amplitudeDest("dest-1", true))}
		proc := newClassifyTestHandle(sourceID, dests)

		available, excluded := proc.classifyDestinations(deniedConsentEvent(), sourceID, "")

		require.Empty(t, available)
		require.Len(t, excluded, 1)
		require.Equal(t, "dest-1", excluded[0].destination.ID)
		require.Equal(t, reportingtypes.FilteredConsentStatus, excluded[0].reason)
		require.Equal(t, reportingtypes.ConsentDeniedEventCode, excluded[0].statusCode)
	})

	t.Run("a destination excluded by integrations and also consent-denied yields exactly one entry, with reason filtered_integration", func(t *testing.T) {
		sourceID := "source-1"
		dests := []backendconfig.DestinationT{consentDeniedDest(amplitudeDest("dest-1", true))}
		proc := newClassifyTestHandle(sourceID, dests)

		event := deniedConsentEvent()
		event["integrations"] = map[string]any{"Amplitude": false}

		available, excluded := proc.classifyDestinations(event, sourceID, "")

		require.Empty(t, available)
		require.Len(t, excluded, 1)
		require.Equal(t, "dest-1", excluded[0].destination.ID)
		require.Equal(t, reportingtypes.FilteredIntegrationStatus, excluded[0].reason)
	})

	t.Run("two destinations of the same type with one consent-denied preserve backend-config order in available", func(t *testing.T) {
		sourceID := "source-1"
		dests := []backendconfig.DestinationT{
			consentDeniedDest(amplitudeDest("dest-1", true)),
			amplitudeDest("dest-2", true),
			amplitudeDest("dest-3", true),
		}
		proc := newClassifyTestHandle(sourceID, dests)

		available, excluded := proc.classifyDestinations(deniedConsentEvent(), sourceID, "")

		require.Equal(t, []string{"dest-2", "dest-3"}, availableIDs(available))
		require.Equal(t, []string{"dest-1"}, excludedIDs(excluded))
	})

	t.Run("specificDestID narrows the candidate set to the stamped destination and siblings appear in neither slice", func(t *testing.T) {
		sourceID := "source-1"
		dests := []backendconfig.DestinationT{
			webhookDest("dest-1", true),
			amplitudeDest("dest-2", true),
			amplitudeDest("dest-3", true),
		}
		proc := newClassifyTestHandle(sourceID, dests)

		available, excluded := proc.classifyDestinations(types.SingularEventT{}, sourceID, "dest-2")

		require.Equal(t, []string{"dest-2"}, availableIDs(available))
		require.Empty(t, excluded)
	})

	t.Run("specificDestID set and the stamped destination consent-denied yields empty available and exactly one excluded entry", func(t *testing.T) {
		sourceID := "source-1"
		dests := []backendconfig.DestinationT{
			consentDeniedDest(amplitudeDest("dest-1", true)),
			amplitudeDest("dest-2", true),
		}
		proc := newClassifyTestHandle(sourceID, dests)

		available, excluded := proc.classifyDestinations(deniedConsentEvent(), sourceID, "dest-1")

		require.Empty(t, available)
		require.Len(t, excluded, 1)
		require.Equal(t, "dest-1", excluded[0].destination.ID)
		require.Equal(t, reportingtypes.FilteredConsentStatus, excluded[0].reason)
	})

	t.Run("specificDestID naming a disabled or unconnected destination yields empty available AND empty excluded", func(t *testing.T) {
		sourceID := "source-1"
		dests := []backendconfig.DestinationT{webhookDest("dest-1", true)}
		proc := newClassifyTestHandle(sourceID, dests)

		available, excluded := proc.classifyDestinations(types.SingularEventT{}, sourceID, "dest-not-connected")

		require.Empty(t, available)
		require.Empty(t, excluded)
	})

	t.Run("specificDestID set and the stamped destination's type integration-excluded yields empty available and one filtered_integration entry", func(t *testing.T) {
		sourceID := "source-1"
		dests := []backendconfig.DestinationT{
			webhookDest("dest-1", true),
			amplitudeDest("dest-2", true),
		}
		proc := newClassifyTestHandle(sourceID, dests)

		available, excluded := proc.classifyDestinations(integrationsExcludingEvent("Amplitude"), sourceID, "dest-2")

		require.Empty(t, available)
		require.Len(t, excluded, 1)
		require.Equal(t, "dest-2", excluded[0].destination.ID)
		require.Equal(t, reportingtypes.FilteredIntegrationStatus, excluded[0].reason)
	})

	t.Run("a source with no destinations yields empty available and empty excluded", func(t *testing.T) {
		sourceID := "source-1"
		proc := newClassifyTestHandle(sourceID, nil)

		available, excluded := proc.classifyDestinations(types.SingularEventT{}, sourceID, "")

		require.Empty(t, available)
		require.Empty(t, excluded)
	})

	t.Run("a malformed integrations.All value excludes every candidate as filtered_integration", func(t *testing.T) {
		sourceID := "source-1"
		dests := []backendconfig.DestinationT{webhookDest("dest-1", true), amplitudeDest("dest-2", true)}
		proc := newClassifyTestHandle(sourceID, dests)

		event := types.SingularEventT{
			"integrations": map[string]any{"All": "not-a-bool"},
		}
		available, excluded := proc.classifyDestinations(event, sourceID, "")

		require.Empty(t, available)
		require.ElementsMatch(t, []string{"dest-1", "dest-2"}, excludedIDs(excluded))
		for _, e := range excluded {
			require.Equal(t, reportingtypes.FilteredIntegrationStatus, e.reason)
		}
	})

	t.Run("len(available) > 0 agrees with isDestinationAvailable across the case table", func(t *testing.T) {
		sourceID := "source-1"
		testCases := []struct {
			name           string
			dests          []backendconfig.DestinationT
			event          types.SingularEventT
			specificDestID string
		}{
			{
				name:  "all survive",
				dests: []backendconfig.DestinationT{webhookDest("dest-1", true), amplitudeDest("dest-2", true)},
				event: types.SingularEventT{},
			},
			{
				name:  "integration excluded type",
				dests: []backendconfig.DestinationT{webhookDest("dest-1", true), amplitudeDest("dest-2", true)},
				event: integrationsExcludingEvent("Amplitude"),
			},
			{
				name:  "all excluded by integrations",
				dests: []backendconfig.DestinationT{amplitudeDest("dest-1", true)},
				event: integrationsExcludingEvent("Amplitude"),
			},
			{
				name:  "consent denies the only destination",
				dests: []backendconfig.DestinationT{consentDeniedDest(amplitudeDest("dest-1", true))},
				event: deniedConsentEvent(),
			},
			{
				name:  "consent denies one of two",
				dests: []backendconfig.DestinationT{consentDeniedDest(amplitudeDest("dest-1", true)), amplitudeDest("dest-2", true)},
				event: deniedConsentEvent(),
			},
			{
				name:  "no destinations at all",
				dests: nil,
				event: types.SingularEventT{},
			},
			{
				name:           "RETL narrowed to available sibling",
				dests:          []backendconfig.DestinationT{webhookDest("dest-1", true), amplitudeDest("dest-2", true)},
				event:          types.SingularEventT{},
				specificDestID: "dest-2",
			},
			{
				name:           "RETL narrowed to unavailable/unconnected destination",
				dests:          []backendconfig.DestinationT{webhookDest("dest-1", true)},
				event:          types.SingularEventT{},
				specificDestID: "dest-not-connected",
			},
			{
				name:           "RETL narrowed to consent-denied stamped destination",
				dests:          []backendconfig.DestinationT{consentDeniedDest(amplitudeDest("dest-1", true))},
				event:          deniedConsentEvent(),
				specificDestID: "dest-1",
			},
			{
				name:  "malformed integrations.All",
				dests: []backendconfig.DestinationT{webhookDest("dest-1", true)},
				event: types.SingularEventT{"integrations": map[string]any{"All": "not-a-bool"}},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				proc := newClassifyTestHandle(sourceID, tc.dests)

				available, _ := proc.classifyDestinations(tc.event, sourceID, tc.specificDestID)
				got := len(available) > 0
				want := proc.isDestinationAvailable(tc.event, sourceID, tc.specificDestID)

				require.Equal(t, want, got, "classifyDestinations availability must agree with isDestinationAvailable")
			})
		}
	})
}
