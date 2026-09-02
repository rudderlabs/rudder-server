package backendconfig

import (
	"fmt"
	"strings"

	"github.com/samber/lo"
)

// Per source destination config filtering (B3 in design doc).
//
// The destination's config is not delivered as stored: it is rebuilt from the keys its definition
// declares for the source type it is connected to, which is how a destination wired to a web
// source receives different keys than the same destination wired to a cloud source.
//
// Dot paths are defensive: no destination definition declares a key containing a dot today (none
// of the 949 distinct keys across 247 definitions), so lodashGet and lodashSet below behave as plain
// map access. They are kept because the reference resolves these keys with lodash get and set, and
// if a dotted key ever appears, a flat implementation would quietly hand that destination a
// different config than the control plane does.
//
// The asymmetry in clause (b) is the reference's, not a slip: the read from the unfiltered config is
// a flat key access guarded by truthiness, while the write into the rebuilt config is dot path
// aware. A dotted key would therefore read from the literal `"a.b"` entry and write into a nested
// one.
func filterDestinationConfig(stored, liveEventsConfig, definitionConfig map[string]any, sourceType string) (map[string]any, error) {
	destConfig, ok := definitionConfig["destConfig"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("definition config has no destConfig object")
	}
	defaultConfig, ok := destConfig["defaultConfig"].([]any)
	if !ok {
		return nil, fmt.Errorf("definition destConfig has no defaultConfig list")
	}

	// the stored config and the live events flags are one object before any of this (A9 in design doc)
	unfiltered := lo.Assign(stored, liveEventsConfig)

	filtered := make(map[string]any, len(defaultConfig))
	// (a) every key the definition declares as common, read and written by dot path. A key the
	// destination does not carry is left out rather than written as null
	for _, key := range defaultConfig {
		name, ok := key.(string)
		if !ok {
			continue
		}
		if value, found := lodashGet(unfiltered, name); found {
			lodashSet(filtered, name, value)
		}
	}

	// (b) and the keys declared for this source type, whose values are per source type objects
	if sourceKeys, ok := destConfig[sourceType].([]any); ok {
		for _, key := range sourceKeys {
			name, ok := key.(string)
			if !ok {
				continue
			}
			// flat read, unlike the write. The reference guards on truthiness first, which this
			// subsumes: only an object can carry a value per source type, and every object is
			// truthy, while everything that is not one is skipped either way
			perSourceType, ok := unfiltered[name].(map[string]any)
			if !ok {
				continue
			}
			if value, found := perSourceType[sourceType]; found {
				lodashSet(filtered, name, value)
			}
		}
	}

	// (c) eventDelivery and eventDeliveryTS belong in the config too, but no definition declares
	// them, so the rebuild above dropped them. They come back from liveEventsConfig, where the
	// control plane keeps them and where the reference reads them from; the merged config passed
	// in holds the same values, having been merged from it, but we are mimicking cp-side mapping.
	if liveEventsConfig != nil {
		for _, key := range []string{"eventDelivery", "eventDeliveryTS"} {
			if value, found := lodashGet(liveEventsConfig, key); found {
				lodashSet(filtered, key, value)
			}
		}
	}
	return filtered, nil
}

// lodashGet reads a dot path, as the reference's lodash get does. The bool distinguishes an absent
// key from one holding null: the reference writes the former nowhere and the latter through as
// null. Nothing declares a dotted key today - see filterDestinationConfig.
func lodashGet(m map[string]any, path string) (any, bool) {
	segments := strings.Split(path, ".")
	for i, segment := range segments {
		value, ok := m[segment]
		if !ok {
			return nil, false
		}
		if i == len(segments)-1 {
			return value, true
		}
		if m, ok = value.(map[string]any); !ok {
			return nil, false
		}
	}
	return nil, false
}

// lodashSet writes a dot path, as the reference's lodash set does, creating the objects along the
// way. A segment occupied by something other than an object is overwritten, as lodash does too.
func lodashSet(m map[string]any, path string, value any) {
	segments := strings.Split(path, ".")
	for _, segment := range segments[:len(segments)-1] {
		next, ok := m[segment].(map[string]any)
		if !ok {
			next = make(map[string]any)
			m[segment] = next
		}
		m = next
	}
	m[segments[len(segments)-1]] = value
}

// Consent backfill, modern to legacy (B4 in design doc).
//
// This is what keeps older SDK clients working against a destination configured in the modern UI.
// processor/consent.go picks its branch from the event: an event whose context.consentManagement
// names no provider, or names oneTrust or ketch, is filtered against these legacy keys rather than
// against the generic consent management. Without the backfill there would be nothing there to
// filter against, and the event would reach a destination the customer meant to gate - no error,
// no log.
func backfillLegacyConsents(config map[string]any) map[string]any {
	config = backfillProvider(config, "oneTrust", "oneTrustCookieCategories", "oneTrustCookieCategory")
	return backfillProvider(config, "ketch", "ketchConsentPurposes", "purpose")
}

func backfillProvider(config map[string]any, provider, legacyConfigKey, legacyConsentKey string) map[string]any {
	if config == nil {
		return config
	}
	if consentsConfigured(config[legacyConfigKey], legacyConsentKey) {
		return config // already populated: the modern config does not override it
	}
	consentManagement, ok := config["consentManagement"].([]any)
	if !ok {
		return config
	}
	for _, entry := range consentManagement {
		providerConfig, ok := entry.(map[string]any)
		if !ok || providerConfig["provider"] != provider {
			continue
		}
		if !consentsConfigured(providerConfig["consents"], "consent") {
			continue
		}
		consents, _ := providerConfig["consents"].([]any)
		legacy := make([]any, 0, len(consents))
		for _, consent := range consents {
			consentConfig, _ := consent.(map[string]any)
			legacy = append(legacy, map[string]any{legacyConsentKey: consentConfig["consent"]})
		}
		config[legacyConfigKey] = legacy
		return config
	}
	return config
}

// consentsConfigured reports whether a consent list holds at least one non empty consent.
func consentsConfigured(consentsConfig any, consentKey string) bool {
	consents, ok := consentsConfig.([]any)
	if !ok {
		return false
	}
	for _, consent := range consents {
		consentConfig, ok := consent.(map[string]any)
		if !ok {
			continue
		}
		if value, ok := consentConfig[consentKey].(string); ok && value != "" {
			return true
		}
	}
	return false
}
