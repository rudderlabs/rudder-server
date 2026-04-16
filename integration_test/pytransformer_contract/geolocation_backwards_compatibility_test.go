package pytransformer_contract

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/processor/types"
)

// TestBackwardsCompatibilityGeolocation compares geolocation() behavior between
// the old architecture (rudder-transformer + openfaas-flask-base) and the new
// architecture (rudder-pytransformer).
//
// Both implementations expose a geolocation(ip) function to user transformation
// code that calls an external geolocation service at {base_url}/geoip/{ip}.
//
// This test starts a rudder-geolocation container and configures both
// architectures to use it, then exercises all edge cases.
func TestBackwardsCompatibilityGeolocation(t *testing.T) {
	type subtest struct {
		name      string
		versionID string
		config    configBackendEntry
		run       func(t *testing.T, env *bcTestEnv)
	}

	subtests := []subtest{
		{
			name:      "GeolocationValidIP",
			versionID: "bc-geo-valid-ip-v1",
			config: configBackendEntry{code: `
def transformEvent(event, metadata):
    result = geolocation("1.2.3.4")
    event["geo"] = result
    return event
`},
			run: func(t *testing.T, env *bcTestEnv) {
				const versionID = "bc-geo-valid-ip-v1"

				events := []types.TransformerEvent{
					makeEvent("msg-1", versionID),
				}

				t.Log("Sending request to old architecture...")
				oldResp := env.OldClient.Transform(context.Background(), events)
				t.Logf("Old arch: Events=%d, FailedEvents=%d", len(oldResp.Events), len(oldResp.FailedEvents))

				t.Log("Sending request to new architecture...")
				newResp := env.NewClient.Transform(context.Background(), events)
				t.Logf("New arch: Events=%d, FailedEvents=%d", len(newResp.Events), len(newResp.FailedEvents))

				require.Equal(t, 1, len(oldResp.Events), "old arch: 1 success event expected")
				require.Equal(t, 0, len(oldResp.FailedEvents), "old arch: no failed events expected")
				require.Equal(t, 1, len(newResp.Events), "new arch: 1 success event expected")
				require.Equal(t, 0, len(newResp.FailedEvents), "new arch: no failed events expected")

				// Verify geo data was returned with correct IP
				oldGeo, _ := oldResp.Events[0].Output["geo"].(map[string]any)
				newGeo, _ := newResp.Events[0].Output["geo"].(map[string]any)
				require.Equal(t, "1.2.3.4", oldGeo["ip"], "old arch: geo should contain correct ip")
				require.Equal(t, "1.2.3.4", newGeo["ip"], "new arch: geo should contain correct ip")

				t.Logf("Old arch geo: %v", oldResp.Events[0].Output["geo"])
				t.Logf("New arch geo: %v", newResp.Events[0].Output["geo"])

				diff, equal := oldResp.Equal(&newResp)
				if equal {
					t.Log("Both architectures produce identical responses for valid IP geolocation")
				} else {
					t.Errorf("Responses differ:\n%s", diff)
				}
			},
		},
		{
			name:      "GeolocationNoArgs",
			versionID: "bc-geo-no-args-v1",
			config: configBackendEntry{code: `
def transformEvent(event, metadata):
    try:
        result = geolocation()
        event["geo"] = result
    except Exception as e:
        event["geo_error"] = str(e)
    return event
`},
			run: func(t *testing.T, env *bcTestEnv) {
				const versionID = "bc-geo-no-args-v1"

				events := []types.TransformerEvent{
					makeEvent("msg-1", versionID),
				}

				t.Log("Sending request to old architecture...")
				oldResp := env.OldClient.Transform(context.Background(), events)
				t.Logf("Old arch: Events=%d, FailedEvents=%d", len(oldResp.Events), len(oldResp.FailedEvents))

				t.Log("Sending request to new architecture...")
				newResp := env.NewClient.Transform(context.Background(), events)
				t.Logf("New arch: Events=%d, FailedEvents=%d", len(newResp.Events), len(newResp.FailedEvents))

				// Both should succeed (error caught by try/except)
				require.Equal(t, 1, len(oldResp.Events), "old arch: 1 success event expected")
				require.Equal(t, 1, len(newResp.Events), "new arch: 1 success event expected")

				// Both should have geo_error containing the validation message
				oldError, _ := oldResp.Events[0].Output["geo_error"].(string)
				newError, _ := newResp.Events[0].Output["geo_error"].(string)
				t.Logf("Old arch geo_error: %q", oldError)
				t.Logf("New arch geo_error: %q", newError)

				require.Contains(t, oldError, "single string argument", "old arch: error should mention single string argument")
				require.Contains(t, newError, "single string argument", "new arch: error should mention single string argument")

				diff, equal := oldResp.Equal(&newResp)
				if equal {
					t.Log("Both architectures produce identical responses for geolocation with no args")
				} else {
					t.Errorf("Responses differ:\n%s", diff)
				}
			},
		},
		{
			name:      "GeolocationMultipleArgs",
			versionID: "bc-geo-multi-args-v1",
			config: configBackendEntry{code: `
def transformEvent(event, metadata):
    try:
        result = geolocation("1.2.3.4", "extra")
        event["geo"] = result
    except Exception as e:
        event["geo_error"] = str(e)
    return event
`},
			run: func(t *testing.T, env *bcTestEnv) {
				const versionID = "bc-geo-multi-args-v1"

				events := []types.TransformerEvent{
					makeEvent("msg-1", versionID),
				}

				oldResp := env.OldClient.Transform(context.Background(), events)
				t.Logf("Old arch: Events=%d, FailedEvents=%d", len(oldResp.Events), len(oldResp.FailedEvents))
				newResp := env.NewClient.Transform(context.Background(), events)
				t.Logf("New arch: Events=%d, FailedEvents=%d", len(newResp.Events), len(newResp.FailedEvents))

				require.Equal(t, 1, len(oldResp.Events), "old arch: 1 success event expected")
				require.Equal(t, 1, len(newResp.Events), "new arch: 1 success event expected")

				oldError, _ := oldResp.Events[0].Output["geo_error"].(string)
				newError, _ := newResp.Events[0].Output["geo_error"].(string)
				t.Logf("Old arch geo_error: %q", oldError)
				t.Logf("New arch geo_error: %q", newError)

				require.Contains(t, oldError, "single string argument", "old arch: error should mention single string argument")
				require.Contains(t, newError, "single string argument", "new arch: error should mention single string argument")

				diff, equal := oldResp.Equal(&newResp)
				if equal {
					t.Log("Both architectures produce identical responses for geolocation with multiple args")
				} else {
					t.Errorf("Responses differ:\n%s", diff)
				}
			},
		},
		{
			name:      "GeolocationNonStringArg",
			versionID: "bc-geo-non-string-v1",
			config: configBackendEntry{code: `
def transformEvent(event, metadata):
    try:
        result = geolocation(12345)
        event["geo"] = result
    except Exception as e:
        event["geo_error"] = str(e)
    return event
`},
			run: func(t *testing.T, env *bcTestEnv) {
				const versionID = "bc-geo-non-string-v1"

				events := []types.TransformerEvent{
					makeEvent("msg-1", versionID),
				}

				oldResp := env.OldClient.Transform(context.Background(), events)
				t.Logf("Old arch: Events=%d, FailedEvents=%d", len(oldResp.Events), len(oldResp.FailedEvents))
				newResp := env.NewClient.Transform(context.Background(), events)
				t.Logf("New arch: Events=%d, FailedEvents=%d", len(newResp.Events), len(newResp.FailedEvents))

				require.Equal(t, 1, len(oldResp.Events), "old arch: 1 success event expected")
				require.Equal(t, 1, len(newResp.Events), "new arch: 1 success event expected")

				oldError, _ := oldResp.Events[0].Output["geo_error"].(string)
				newError, _ := newResp.Events[0].Output["geo_error"].(string)
				t.Logf("Old arch geo_error: %q", oldError)
				t.Logf("New arch geo_error: %q", newError)

				require.Contains(t, oldError, "single string argument", "old arch: error should mention single string argument")
				require.Contains(t, newError, "single string argument", "new arch: error should mention single string argument")

				diff, equal := oldResp.Equal(&newResp)
				if equal {
					t.Log("Both architectures produce identical responses for geolocation with non-string arg")
				} else {
					t.Errorf("Responses differ:\n%s", diff)
				}
			},
		},
		{
			name:      "GeolocationNoneArg",
			versionID: "bc-geo-none-arg-v1",
			config: configBackendEntry{code: `
def transformEvent(event, metadata):
    try:
        result = geolocation(None)
        event["geo"] = result
    except Exception as e:
        event["geo_error"] = str(e)
    return event
`},
			run: func(t *testing.T, env *bcTestEnv) {
				const versionID = "bc-geo-none-arg-v1"

				events := []types.TransformerEvent{
					makeEvent("msg-1", versionID),
				}

				oldResp := env.OldClient.Transform(context.Background(), events)
				t.Logf("Old arch: Events=%d, FailedEvents=%d", len(oldResp.Events), len(oldResp.FailedEvents))
				newResp := env.NewClient.Transform(context.Background(), events)
				t.Logf("New arch: Events=%d, FailedEvents=%d", len(newResp.Events), len(newResp.FailedEvents))

				require.Equal(t, 1, len(oldResp.Events), "old arch: 1 success event expected")
				require.Equal(t, 1, len(newResp.Events), "new arch: 1 success event expected")

				oldError, _ := oldResp.Events[0].Output["geo_error"].(string)
				newError, _ := newResp.Events[0].Output["geo_error"].(string)
				t.Logf("Old arch geo_error: %q", oldError)
				t.Logf("New arch geo_error: %q", newError)

				require.Contains(t, oldError, "single string argument", "old arch: error should mention single string argument")
				require.Contains(t, newError, "single string argument", "new arch: error should mention single string argument")

				diff, equal := oldResp.Equal(&newResp)
				if equal {
					t.Log("Both architectures produce identical responses for geolocation with None arg")
				} else {
					t.Errorf("Responses differ:\n%s", diff)
				}
			},
		},
		{
			name:      "GeolocationListArg",
			versionID: "bc-geo-list-arg-v1",
			config: configBackendEntry{code: `
def transformEvent(event, metadata):
    try:
        result = geolocation(["1.2.3.4"])
        event["geo"] = result
    except Exception as e:
        event["geo_error"] = str(e)
    return event
`},
			run: func(t *testing.T, env *bcTestEnv) {
				const versionID = "bc-geo-list-arg-v1"

				events := []types.TransformerEvent{
					makeEvent("msg-1", versionID),
				}

				oldResp := env.OldClient.Transform(context.Background(), events)
				t.Logf("Old arch: Events=%d, FailedEvents=%d", len(oldResp.Events), len(oldResp.FailedEvents))
				newResp := env.NewClient.Transform(context.Background(), events)
				t.Logf("New arch: Events=%d, FailedEvents=%d", len(newResp.Events), len(newResp.FailedEvents))

				require.Equal(t, 1, len(oldResp.Events), "old arch: 1 success event expected")
				require.Equal(t, 1, len(newResp.Events), "new arch: 1 success event expected")

				oldError, _ := oldResp.Events[0].Output["geo_error"].(string)
				newError, _ := newResp.Events[0].Output["geo_error"].(string)
				t.Logf("Old arch geo_error: %q", oldError)
				t.Logf("New arch geo_error: %q", newError)

				require.Contains(t, oldError, "single string argument", "old arch: error should mention single string argument")
				require.Contains(t, newError, "single string argument", "new arch: error should mention single string argument")

				diff, equal := oldResp.Equal(&newResp)
				if equal {
					t.Log("Both architectures produce identical responses for geolocation with list arg")
				} else {
					t.Errorf("Responses differ:\n%s", diff)
				}
			},
		},
		{
			name:      "GeolocationBoolArg",
			versionID: "bc-geo-bool-arg-v1",
			config: configBackendEntry{code: `
def transformEvent(event, metadata):
    try:
        result = geolocation(True)
        event["geo"] = result
    except Exception as e:
        event["geo_error"] = str(e)
    return event
`},
			run: func(t *testing.T, env *bcTestEnv) {
				const versionID = "bc-geo-bool-arg-v1"

				events := []types.TransformerEvent{
					makeEvent("msg-1", versionID),
				}

				oldResp := env.OldClient.Transform(context.Background(), events)
				t.Logf("Old arch: Events=%d, FailedEvents=%d", len(oldResp.Events), len(oldResp.FailedEvents))
				newResp := env.NewClient.Transform(context.Background(), events)
				t.Logf("New arch: Events=%d, FailedEvents=%d", len(newResp.Events), len(newResp.FailedEvents))

				require.Equal(t, 1, len(oldResp.Events), "old arch: 1 success event expected")
				require.Equal(t, 1, len(newResp.Events), "new arch: 1 success event expected")

				oldError, _ := oldResp.Events[0].Output["geo_error"].(string)
				newError, _ := newResp.Events[0].Output["geo_error"].(string)
				t.Logf("Old arch geo_error: %q", oldError)
				t.Logf("New arch geo_error: %q", newError)

				// In Python, isinstance(True, str) is False, so bool should be rejected
				require.Contains(t, oldError, "single string argument", "old arch: error should mention single string argument")
				require.Contains(t, newError, "single string argument", "new arch: error should mention single string argument")

				diff, equal := oldResp.Equal(&newResp)
				if equal {
					t.Log("Both architectures produce identical responses for geolocation with bool arg")
				} else {
					t.Errorf("Responses differ:\n%s", diff)
				}
			},
		},
		{
			name:      "GeolocationDictArg",
			versionID: "bc-geo-dict-arg-v1",
			config: configBackendEntry{code: `
def transformEvent(event, metadata):
    try:
        result = geolocation({"ip": "1.2.3.4"})
        event["geo"] = result
    except Exception as e:
        event["geo_error"] = str(e)
    return event
`},
			run: func(t *testing.T, env *bcTestEnv) {
				const versionID = "bc-geo-dict-arg-v1"

				events := []types.TransformerEvent{
					makeEvent("msg-1", versionID),
				}

				oldResp := env.OldClient.Transform(context.Background(), events)
				t.Logf("Old arch: Events=%d, FailedEvents=%d", len(oldResp.Events), len(oldResp.FailedEvents))
				newResp := env.NewClient.Transform(context.Background(), events)
				t.Logf("New arch: Events=%d, FailedEvents=%d", len(newResp.Events), len(newResp.FailedEvents))

				require.Equal(t, 1, len(oldResp.Events), "old arch: 1 success event expected")
				require.Equal(t, 1, len(newResp.Events), "new arch: 1 success event expected")

				oldError, _ := oldResp.Events[0].Output["geo_error"].(string)
				newError, _ := newResp.Events[0].Output["geo_error"].(string)
				t.Logf("Old arch geo_error: %q", oldError)
				t.Logf("New arch geo_error: %q", newError)

				require.Contains(t, oldError, "single string argument", "old arch: error should mention single string argument")
				require.Contains(t, newError, "single string argument", "new arch: error should mention single string argument")

				diff, equal := oldResp.Equal(&newResp)
				if equal {
					t.Log("Both architectures produce identical responses for geolocation with dict arg")
				} else {
					t.Errorf("Responses differ:\n%s", diff)
				}
			},
		},
		{
			name:      "GeolocationEmptyString",
			versionID: "bc-geo-empty-string-v1",
			config: configBackendEntry{code: `
def transformEvent(event, metadata):
    try:
        result = geolocation("")
        event["geo"] = result
    except Exception as e:
        event["geo_error"] = str(e)
    return event
`},
			run: func(t *testing.T, env *bcTestEnv) {
				const versionID = "bc-geo-empty-string-v1"

				events := []types.TransformerEvent{
					makeEvent("msg-1", versionID),
				}

				oldResp := env.OldClient.Transform(context.Background(), events)
				t.Logf("Old arch: Events=%d, FailedEvents=%d", len(oldResp.Events), len(oldResp.FailedEvents))
				newResp := env.NewClient.Transform(context.Background(), events)
				t.Logf("New arch: Events=%d, FailedEvents=%d", len(newResp.Events), len(newResp.FailedEvents))

				// Empty string passes isinstance(args[0], str) check but the geolocation
				// service will likely return an error for an empty IP.
				require.Equal(t, 1, len(oldResp.Events), "old arch: 1 success event expected")
				require.Equal(t, 1, len(newResp.Events), "new arch: 1 success event expected")

				t.Logf("Old arch output: geo=%v, geo_error=%v",
					oldResp.Events[0].Output["geo"], oldResp.Events[0].Output["geo_error"])
				t.Logf("New arch output: geo=%v, geo_error=%v",
					newResp.Events[0].Output["geo"], newResp.Events[0].Output["geo_error"])

				diff, equal := oldResp.Equal(&newResp)
				if equal {
					t.Log("Both architectures produce identical responses for geolocation with empty string")
				} else {
					t.Errorf("Responses differ:\n%s", diff)
				}
			},
		},
		{
			name:      "GeolocationMultipleCalls",
			versionID: "bc-geo-multi-calls-v1",
			config: configBackendEntry{code: `
def transformEvent(event, metadata):
    try:
        geo1 = geolocation("1.2.3.4")
        geo2 = geolocation("8.8.8.8")
        event["geo1"] = geo1
        event["geo2"] = geo2
    except Exception as e:
        event["geo_error"] = str(e)
    return event
`},
			run: func(t *testing.T, env *bcTestEnv) {
				const versionID = "bc-geo-multi-calls-v1"

				events := []types.TransformerEvent{
					makeEvent("msg-1", versionID),
				}

				oldResp := env.OldClient.Transform(context.Background(), events)
				t.Logf("Old arch: Events=%d, FailedEvents=%d", len(oldResp.Events), len(oldResp.FailedEvents))
				newResp := env.NewClient.Transform(context.Background(), events)
				t.Logf("New arch: Events=%d, FailedEvents=%d", len(newResp.Events), len(newResp.FailedEvents))

				require.Equal(t, 1, len(oldResp.Events), "old arch: 1 success event expected")
				require.Equal(t, 1, len(newResp.Events), "new arch: 1 success event expected")

				// Both should have two different geo results with correct IPs
				oldGeo1, _ := oldResp.Events[0].Output["geo1"].(map[string]any)
				oldGeo2, _ := oldResp.Events[0].Output["geo2"].(map[string]any)
				newGeo1, _ := newResp.Events[0].Output["geo1"].(map[string]any)
				newGeo2, _ := newResp.Events[0].Output["geo2"].(map[string]any)
				require.Equal(t, "1.2.3.4", oldGeo1["ip"], "old arch: geo1 should contain correct ip")
				require.Equal(t, "8.8.8.8", oldGeo2["ip"], "old arch: geo2 should contain correct ip")
				require.Equal(t, "1.2.3.4", newGeo1["ip"], "new arch: geo1 should contain correct ip")
				require.Equal(t, "8.8.8.8", newGeo2["ip"], "new arch: geo2 should contain correct ip")

				t.Logf("Old arch geo1: %v", oldResp.Events[0].Output["geo1"])
				t.Logf("Old arch geo2: %v", oldResp.Events[0].Output["geo2"])
				t.Logf("New arch geo1: %v", newResp.Events[0].Output["geo1"])
				t.Logf("New arch geo2: %v", newResp.Events[0].Output["geo2"])

				diff, equal := oldResp.Equal(&newResp)
				if equal {
					t.Log("Both architectures produce identical responses for multiple geolocation calls")
				} else {
					t.Errorf("Responses differ:\n%s", diff)
				}
			},
		},
		{
			name:      "GeolocationSameIPTwice",
			versionID: "bc-geo-same-ip-twice-v1",
			config: configBackendEntry{code: `
def transformEvent(event, metadata):
    geo1 = geolocation("1.2.3.4")
    geo2 = geolocation("1.2.3.4")
    event["geo1"] = geo1
    event["geo2"] = geo2
    event["same"] = geo1 == geo2
    return event
`},
			run: func(t *testing.T, env *bcTestEnv) {
				const versionID = "bc-geo-same-ip-twice-v1"

				events := []types.TransformerEvent{
					makeEvent("msg-1", versionID),
				}

				oldResp := env.OldClient.Transform(context.Background(), events)
				t.Logf("Old arch: Events=%d, FailedEvents=%d", len(oldResp.Events), len(oldResp.FailedEvents))
				newResp := env.NewClient.Transform(context.Background(), events)
				t.Logf("New arch: Events=%d, FailedEvents=%d", len(newResp.Events), len(newResp.FailedEvents))

				require.Equal(t, 1, len(oldResp.Events), "old arch: 1 success event expected")
				require.Equal(t, 1, len(newResp.Events), "new arch: 1 success event expected")

				// Same IP should produce same result both times
				require.Equal(t, true, oldResp.Events[0].Output["same"], "old arch: same IP should produce same result")
				require.Equal(t, true, newResp.Events[0].Output["same"], "new arch: same IP should produce same result")

				diff, equal := oldResp.Equal(&newResp)
				if equal {
					t.Log("Both architectures produce identical responses for same IP called twice")
				} else {
					t.Errorf("Responses differ:\n%s", diff)
				}
			},
		},
		{
			name:      "GeolocationEnrichEvent",
			versionID: "bc-geo-enrich-v1",
			config: configBackendEntry{code: `
def transformEvent(event, metadata):
    ip = event.get("properties", {}).get("ip", "1.2.3.4")
    try:
        geo = geolocation(ip)
        if "context" not in event:
            event["context"] = {}
        event["context"]["geo"] = geo
    except Exception as e:
        event["geo_error"] = str(e)
    return event
`},
			run: func(t *testing.T, env *bcTestEnv) {
				const versionID = "bc-geo-enrich-v1"

				events := []types.TransformerEvent{
					{
						Message: types.SingularEventT{
							"messageId":  "msg-1",
							"type":       "track",
							"event":      "Test Event",
							"properties": map[string]any{"ip": "8.8.8.8"},
						},
						Metadata: types.Metadata{
							SourceID:      "src-1",
							DestinationID: "dest-1",
							WorkspaceID:   "ws-1",
							MessageID:     "msg-1",
						},
						Destination: backendconfig.DestinationT{
							Transformations: []backendconfig.TransformationT{
								{VersionID: versionID, ID: "transformation-1", Language: "pythonfaas"},
							},
						},
					},
				}

				oldResp := env.OldClient.Transform(context.Background(), events)
				t.Logf("Old arch: Events=%d, FailedEvents=%d", len(oldResp.Events), len(oldResp.FailedEvents))
				newResp := env.NewClient.Transform(context.Background(), events)
				t.Logf("New arch: Events=%d, FailedEvents=%d", len(newResp.Events), len(newResp.FailedEvents))

				require.Equal(t, 1, len(oldResp.Events), "old arch: 1 success event expected")
				require.Equal(t, 1, len(newResp.Events), "new arch: 1 success event expected")

				// Verify geo data was placed in context.geo with correct IP
				oldCtx, _ := oldResp.Events[0].Output["context"].(map[string]any)
				newCtx, _ := newResp.Events[0].Output["context"].(map[string]any)
				oldGeo, _ := oldCtx["geo"].(map[string]any)
				newGeo, _ := newCtx["geo"].(map[string]any)
				require.Equal(t, "8.8.8.8", oldGeo["ip"], "old arch: context.geo should contain correct ip")
				require.Equal(t, "8.8.8.8", newGeo["ip"], "new arch: context.geo should contain correct ip")

				t.Logf("Old arch context.geo: %v", oldCtx["geo"])
				t.Logf("New arch context.geo: %v", newCtx["geo"])

				diff, equal := oldResp.Equal(&newResp)
				if equal {
					t.Log("Both architectures produce identical responses for geolocation enrichment")
				} else {
					t.Errorf("Responses differ:\n%s", diff)
				}
			},
		},
		{
			name:      "GeolocationBatchTransform",
			versionID: "bc-geo-batch-v1",
			config: configBackendEntry{code: `
def transformBatch(events, metadata):
    for event in events:
        try:
            geo = geolocation("1.2.3.4")
            event["geo"] = geo
        except Exception as e:
            event["geo_error"] = str(e)
    return events
`},
			run: func(t *testing.T, env *bcTestEnv) {
				const versionID = "bc-geo-batch-v1"

				events := []types.TransformerEvent{
					makeEvent("msg-1", versionID),
					makeEvent("msg-2", versionID),
					makeEvent("msg-3", versionID),
				}

				t.Log("Sending 3 events to old architecture...")
				oldResp := env.OldClient.Transform(context.Background(), events)
				t.Logf("Old arch: Events=%d, FailedEvents=%d", len(oldResp.Events), len(oldResp.FailedEvents))

				t.Log("Sending 3 events to new architecture...")
				newResp := env.NewClient.Transform(context.Background(), events)
				t.Logf("New arch: Events=%d, FailedEvents=%d", len(newResp.Events), len(newResp.FailedEvents))

				require.Equal(t, 3, len(oldResp.Events), "old arch: 3 success events expected")
				require.Equal(t, 0, len(oldResp.FailedEvents), "old arch: no failed events expected")
				require.Equal(t, 3, len(newResp.Events), "new arch: 3 success events expected")
				require.Equal(t, 0, len(newResp.FailedEvents), "new arch: no failed events expected")

				// All events should have geo data with correct IP
				for i := range oldResp.Events {
					oldGeo, _ := oldResp.Events[i].Output["geo"].(map[string]any)
					newGeo, _ := newResp.Events[i].Output["geo"].(map[string]any)
					require.Equalf(t, "1.2.3.4", oldGeo["ip"], "old arch: event %d geo should contain correct ip", i)
					require.Equalf(t, "1.2.3.4", newGeo["ip"], "new arch: event %d geo should contain correct ip", i)
				}

				diff, equal := oldResp.Equal(&newResp)
				if equal {
					t.Log("Both architectures produce identical responses for geolocation in batch transform")
				} else {
					t.Errorf("Responses differ:\n%s", diff)
				}
			},
		},
		{
			name:      "GeolocationErrorWithoutTryCatch",
			versionID: "bc-geo-error-no-catch-v1",
			config: configBackendEntry{code: `
def transformEvent(event, metadata):
    # Call with no args without try/catch — should produce a failed event
    result = geolocation()
    event["geo"] = result
    return event
`},
			run: func(t *testing.T, env *bcTestEnv) {
				const versionID = "bc-geo-error-no-catch-v1"

				events := []types.TransformerEvent{
					makeEvent("msg-1", versionID),
				}

				oldResp := env.OldClient.Transform(context.Background(), events)
				t.Logf("Old arch: Events=%d, FailedEvents=%d", len(oldResp.Events), len(oldResp.FailedEvents))
				newResp := env.NewClient.Transform(context.Background(), events)
				t.Logf("New arch: Events=%d, FailedEvents=%d", len(newResp.Events), len(newResp.FailedEvents))

				// Both should fail since the exception propagates
				require.Equal(t, 0, len(oldResp.Events), "old arch: no success events expected")
				require.Equal(t, 1, len(oldResp.FailedEvents), "old arch: 1 failed event expected")
				require.Equal(t, 0, len(newResp.Events), "new arch: no success events expected")
				require.Equal(t, 1, len(newResp.FailedEvents), "new arch: 1 failed event expected")

				oldError := oldResp.FailedEvents[0].Error
				newError := newResp.FailedEvents[0].Error
				t.Logf("Old arch error: %q", oldError)
				t.Logf("New arch error: %q", newError)

				require.Contains(t, oldError, "single string argument", "old arch: error should mention validation")
				require.Contains(t, newError, "single string argument", "new arch: error should mention validation")

				diff, equal := oldResp.Equal(&newResp)
				if equal {
					t.Log("Both architectures produce identical error responses for uncaught geolocation error")
				} else {
					t.Errorf("Responses differ:\n%s", diff)
				}
			},
		},
		{
			name:      "GeolocationPartialErrors",
			versionID: "bc-geo-partial-errors-v1",
			config: configBackendEntry{code: `
def transformEvent(event, metadata):
    msg_id = event.get("messageId", "")
    if msg_id == "msg-2":
        # This event calls geolocation with bad args (no try/catch)
        result = geolocation()
        event["geo"] = result
    else:
        # These events succeed
        result = geolocation("1.2.3.4")
        event["geo"] = result
    return event
`},
			run: func(t *testing.T, env *bcTestEnv) {
				const versionID = "bc-geo-partial-errors-v1"

				events := []types.TransformerEvent{
					makeEvent("msg-1", versionID),
					makeEvent("msg-2", versionID),
					makeEvent("msg-3", versionID),
				}

				t.Log("Sending 3 events to old architecture...")
				oldResp := env.OldClient.Transform(context.Background(), events)
				t.Logf("Old arch: Events=%d, FailedEvents=%d", len(oldResp.Events), len(oldResp.FailedEvents))

				t.Log("Sending 3 events to new architecture...")
				newResp := env.NewClient.Transform(context.Background(), events)
				t.Logf("New arch: Events=%d, FailedEvents=%d", len(newResp.Events), len(newResp.FailedEvents))

				// msg-1 and msg-3 should succeed, msg-2 should fail
				require.Equal(t, 2, len(oldResp.Events), "old arch: 2 success events expected")
				require.Equal(t, 1, len(oldResp.FailedEvents), "old arch: 1 failed event expected")
				require.Equal(t, 2, len(newResp.Events), "new arch: 2 success events expected")
				require.Equal(t, 1, len(newResp.FailedEvents), "new arch: 1 failed event expected")

				diff, equal := oldResp.Equal(&newResp)
				if equal {
					t.Log("Both architectures produce identical responses for partial geolocation errors")
				} else {
					t.Errorf("Responses differ:\n%s", diff)
				}
			},
		},
		{
			name:      "GeolocationResultFieldAccess",
			versionID: "bc-geo-field-access-v1",
			config: configBackendEntry{code: `
def transformEvent(event, metadata):
    try:
        geo = geolocation("1.2.3.4")
        # Access individual fields from the geo response
        event["geo_type"] = str(type(geo))
        event["geo_keys"] = sorted(list(geo.keys())) if isinstance(geo, dict) else None
        event["geo_has_city"] = "city" in geo if isinstance(geo, dict) else False
        event["geo_has_country"] = "country" in geo if isinstance(geo, dict) else False
        event["geo_raw"] = geo
    except Exception as e:
        event["geo_error"] = str(e)
    return event
`},
			run: func(t *testing.T, env *bcTestEnv) {
				const versionID = "bc-geo-field-access-v1"

				events := []types.TransformerEvent{
					makeEvent("msg-1", versionID),
				}

				oldResp := env.OldClient.Transform(context.Background(), events)
				t.Logf("Old arch: Events=%d, FailedEvents=%d", len(oldResp.Events), len(oldResp.FailedEvents))
				newResp := env.NewClient.Transform(context.Background(), events)
				t.Logf("New arch: Events=%d, FailedEvents=%d", len(newResp.Events), len(newResp.FailedEvents))

				require.Equal(t, 1, len(oldResp.Events), "old arch: 1 success event expected")
				require.Equal(t, 1, len(newResp.Events), "new arch: 1 success event expected")

				// Verify the geo result is a dict
				require.Contains(t, oldResp.Events[0].Output["geo_type"], "dict", "old arch: geo should be a dict")
				require.Contains(t, newResp.Events[0].Output["geo_type"], "dict", "new arch: geo should be a dict")

				t.Logf("Old arch: type=%v, keys=%v", oldResp.Events[0].Output["geo_type"], oldResp.Events[0].Output["geo_keys"])
				t.Logf("New arch: type=%v, keys=%v", newResp.Events[0].Output["geo_type"], newResp.Events[0].Output["geo_keys"])

				diff, equal := oldResp.Equal(&newResp)
				if equal {
					t.Log("Both architectures produce identical responses for geolocation field access")
				} else {
					t.Errorf("Responses differ:\n%s", diff)
				}
			},
		},
		{
			name:      "GeolocationIPFromEvent",
			versionID: "bc-geo-ip-from-event-v1",
			config: configBackendEntry{code: `
def transformEvent(event, metadata):
    # Extract IP from the event itself and look it up
    ip = event.get("context", {}).get("ip", "")
    if ip:
        try:
            geo = geolocation(ip)
            event["geo_lookup"] = geo
        except Exception as e:
            event["geo_error"] = str(e)
    else:
        event["geo_error"] = "no IP in event"
    return event
`},
			run: func(t *testing.T, env *bcTestEnv) {
				const versionID = "bc-geo-ip-from-event-v1"

				events := []types.TransformerEvent{
					{
						Message: types.SingularEventT{
							"messageId": "msg-1",
							"type":      "track",
							"event":     "Test Event",
							"context":   map[string]any{"ip": "8.8.8.8"},
						},
						Metadata: types.Metadata{
							SourceID:      "src-1",
							DestinationID: "dest-1",
							WorkspaceID:   "ws-1",
							MessageID:     "msg-1",
						},
						Destination: backendconfig.DestinationT{
							Transformations: []backendconfig.TransformationT{
								{VersionID: versionID, ID: "transformation-1", Language: "pythonfaas"},
							},
						},
					},
				}

				oldResp := env.OldClient.Transform(context.Background(), events)
				t.Logf("Old arch: Events=%d, FailedEvents=%d", len(oldResp.Events), len(oldResp.FailedEvents))
				newResp := env.NewClient.Transform(context.Background(), events)
				t.Logf("New arch: Events=%d, FailedEvents=%d", len(newResp.Events), len(newResp.FailedEvents))

				require.Equal(t, 1, len(oldResp.Events), "old arch: 1 success event expected")
				require.Equal(t, 1, len(newResp.Events), "new arch: 1 success event expected")

				oldGeo, _ := oldResp.Events[0].Output["geo_lookup"].(map[string]any)
				newGeo, _ := newResp.Events[0].Output["geo_lookup"].(map[string]any)
				require.Equal(t, "8.8.8.8", oldGeo["ip"], "old arch: geo_lookup should contain correct ip")
				require.Equal(t, "8.8.8.8", newGeo["ip"], "new arch: geo_lookup should contain correct ip")

				diff, equal := oldResp.Equal(&newResp)
				if equal {
					t.Log("Both architectures produce identical responses for geolocation with IP from event")
				} else {
					t.Errorf("Responses differ:\n%s", diff)
				}
			},
		},
		{
			name:      "GeolocationBatchDifferentIPs",
			versionID: "bc-geo-batch-diff-ips-v1",
			config: configBackendEntry{code: `
def transformBatch(events, metadata):
    ips = ["1.2.3.4", "8.8.8.8", "1.2.3.4"]
    for i, event in enumerate(events):
        try:
            geo = geolocation(ips[i])
            event["geo"] = geo
            event["ip_used"] = ips[i]
        except Exception as e:
            event["geo_error"] = str(e)
    return events
`},
			run: func(t *testing.T, env *bcTestEnv) {
				const versionID = "bc-geo-batch-diff-ips-v1"

				events := []types.TransformerEvent{
					makeEvent("msg-1", versionID),
					makeEvent("msg-2", versionID),
					makeEvent("msg-3", versionID),
				}

				t.Log("Sending 3 events to old architecture...")
				oldResp := env.OldClient.Transform(context.Background(), events)
				t.Logf("Old arch: Events=%d, FailedEvents=%d", len(oldResp.Events), len(oldResp.FailedEvents))

				t.Log("Sending 3 events to new architecture...")
				newResp := env.NewClient.Transform(context.Background(), events)
				t.Logf("New arch: Events=%d, FailedEvents=%d", len(newResp.Events), len(newResp.FailedEvents))

				require.Equal(t, 3, len(oldResp.Events), "old arch: 3 success events expected")
				require.Equal(t, 3, len(newResp.Events), "new arch: 3 success events expected")

				// First and third should have same geo data (same IP)
				// Second should have different geo data
				expectedIPs := []string{"1.2.3.4", "8.8.8.8", "1.2.3.4"}
				for i := range oldResp.Events {
					oldGeo, _ := oldResp.Events[i].Output["geo"].(map[string]any)
					newGeo, _ := newResp.Events[i].Output["geo"].(map[string]any)
					require.Equalf(t, expectedIPs[i], oldGeo["ip"], "old arch: event %d geo should contain correct ip", i)
					require.Equalf(t, expectedIPs[i], newGeo["ip"], "new arch: event %d geo should contain correct ip", i)
				}

				diff, equal := oldResp.Equal(&newResp)
				if equal {
					t.Log("Both architectures produce identical responses for batch geolocation with different IPs")
				} else {
					t.Errorf("Responses differ:\n%s", diff)
				}
			},
		},
		{
			name:      "GeolocationInvalidIP",
			versionID: "bc-geo-fail-invalid-v1",
			config: configBackendEntry{code: `
def transformEvent(event, metadata):
    try:
        result = geolocation("not-an-ip")
        event["geo"] = result
    except Exception as e:
        event["geo_error"] = str(e)
    return event
`},
			run: func(t *testing.T, env *bcTestEnv) {
				const versionID = "bc-geo-fail-invalid-v1"

				events := []types.TransformerEvent{
					makeEvent("msg-1", versionID),
				}

				oldResp := env.OldClient.Transform(context.Background(), events)
				t.Logf("Old arch: Events=%d, FailedEvents=%d", len(oldResp.Events), len(oldResp.FailedEvents))
				newResp := env.NewClient.Transform(context.Background(), events)
				t.Logf("New arch: Events=%d, FailedEvents=%d", len(newResp.Events), len(newResp.FailedEvents))

				require.Equal(t, 1, len(oldResp.Events), "old arch: 1 success event expected")
				require.Equal(t, 1, len(newResp.Events), "new arch: 1 success event expected")

				oldError, _ := oldResp.Events[0].Output["geo_error"].(string)
				newError, _ := newResp.Events[0].Output["geo_error"].(string)
				t.Logf("Old arch geo_error: %q", oldError)
				t.Logf("New arch geo_error: %q", newError)

				require.Contains(t, oldError, "status code: 400", "old arch: error should mention 400")
				require.Contains(t, newError, "status code: 400", "new arch: error should mention 400")

				diff, equal := oldResp.Equal(&newResp)
				if equal {
					t.Log("Both architectures produce identical responses for invalid IP")
				} else {
					t.Errorf("Responses differ:\n%s", diff)
				}
			},
		},
		{
			name:      "GeolocationInvalidIPSpecialChars",
			versionID: "bc-geo-fail-special-v1",
			config: configBackendEntry{code: `
def transformEvent(event, metadata):
    try:
        result = geolocation("hello world")
        event["geo"] = result
    except Exception as e:
        event["geo_error"] = str(e)
    return event
`},
			run: func(t *testing.T, env *bcTestEnv) {
				const versionID = "bc-geo-fail-special-v1"

				events := []types.TransformerEvent{
					makeEvent("msg-1", versionID),
				}

				oldResp := env.OldClient.Transform(context.Background(), events)
				t.Logf("Old arch: Events=%d, FailedEvents=%d", len(oldResp.Events), len(oldResp.FailedEvents))
				newResp := env.NewClient.Transform(context.Background(), events)
				t.Logf("New arch: Events=%d, FailedEvents=%d", len(newResp.Events), len(newResp.FailedEvents))

				require.Equal(t, 1, len(oldResp.Events), "old arch: 1 success event expected")
				require.Equal(t, 1, len(newResp.Events), "new arch: 1 success event expected")

				oldError, _ := oldResp.Events[0].Output["geo_error"].(string)
				newError, _ := newResp.Events[0].Output["geo_error"].(string)
				t.Logf("Old arch geo_error: %q", oldError)
				t.Logf("New arch geo_error: %q", newError)

				require.Contains(t, oldError, "status code: 400", "old arch: error should mention 400")
				require.Contains(t, newError, "status code: 400", "new arch: error should mention 400")

				diff, equal := oldResp.Equal(&newResp)
				if equal {
					t.Log("Both architectures produce identical responses for special chars IP")
				} else {
					t.Errorf("Responses differ:\n%s", diff)
				}
			},
		},
		{
			name:      "GeolocationInvalidIPUncaught",
			versionID: "bc-geo-fail-uncaught-v1",
			config: configBackendEntry{code: `
def transformEvent(event, metadata):
    # Call geolocation with invalid IP without try/catch
    result = geolocation("not-an-ip")
    event["geo"] = result
    return event
`},
			run: func(t *testing.T, env *bcTestEnv) {
				const versionID = "bc-geo-fail-uncaught-v1"

				events := []types.TransformerEvent{
					makeEvent("msg-1", versionID),
				}

				oldResp := env.OldClient.Transform(context.Background(), events)
				t.Logf("Old arch: Events=%d, FailedEvents=%d", len(oldResp.Events), len(oldResp.FailedEvents))
				newResp := env.NewClient.Transform(context.Background(), events)
				t.Logf("New arch: Events=%d, FailedEvents=%d", len(newResp.Events), len(newResp.FailedEvents))

				// Both should produce a failed event since the exception propagates
				require.Equal(t, 0, len(oldResp.Events), "old arch: no success events expected")
				require.Equal(t, 1, len(oldResp.FailedEvents), "old arch: 1 failed event expected")
				require.Equal(t, 0, len(newResp.Events), "new arch: no success events expected")
				require.Equal(t, 1, len(newResp.FailedEvents), "new arch: 1 failed event expected")

				oldError := oldResp.FailedEvents[0].Error
				newError := newResp.FailedEvents[0].Error
				t.Logf("Old arch error: %q", oldError)
				t.Logf("New arch error: %q", newError)

				require.Contains(t, oldError, "status code: 400", "old arch: error should mention 400")
				require.Contains(t, newError, "status code: 400", "new arch: error should mention 400")

				diff, equal := oldResp.Equal(&newResp)
				if equal {
					t.Log("Both architectures produce identical error for uncaught invalid IP")
				} else {
					t.Errorf("Responses differ:\n%s", diff)
				}
			},
		},
		{
			name:      "GeolocationInvalidIPBatch",
			versionID: "bc-geo-fail-batch-v1",
			config: configBackendEntry{code: `
def transformBatch(events, metadata):
    for event in events:
        try:
            geo = geolocation("not-an-ip")
            event["geo"] = geo
        except Exception as e:
            event["geo_error"] = str(e)
    return events
`},
			run: func(t *testing.T, env *bcTestEnv) {
				const versionID = "bc-geo-fail-batch-v1"

				events := []types.TransformerEvent{
					makeEvent("msg-1", versionID),
					makeEvent("msg-2", versionID),
					makeEvent("msg-3", versionID),
				}

				oldResp := env.OldClient.Transform(context.Background(), events)
				t.Logf("Old arch: Events=%d, FailedEvents=%d", len(oldResp.Events), len(oldResp.FailedEvents))
				newResp := env.NewClient.Transform(context.Background(), events)
				t.Logf("New arch: Events=%d, FailedEvents=%d", len(newResp.Events), len(newResp.FailedEvents))

				require.Equal(t, 3, len(oldResp.Events), "old arch: 3 success events expected")
				require.Equal(t, 3, len(newResp.Events), "new arch: 3 success events expected")

				for i := range oldResp.Events {
					oldError, _ := oldResp.Events[i].Output["geo_error"].(string)
					newError, _ := newResp.Events[i].Output["geo_error"].(string)
					require.Containsf(t, oldError, "status code: 400", "old arch: event %d error should mention 400", i)
					require.Containsf(t, newError, "status code: 400", "new arch: event %d error should mention 400", i)
				}

				diff, equal := oldResp.Equal(&newResp)
				if equal {
					t.Log("Both architectures produce identical responses for batch invalid IP")
				} else {
					t.Errorf("Responses differ:\n%s", diff)
				}
			},
		},
		{
			name:      "GeolocationPartialInvalidIP",
			versionID: "bc-geo-fail-partial-v1",
			config: configBackendEntry{code: `
def transformEvent(event, metadata):
    msg_id = event.get("messageId", "")
    if msg_id == "msg-2":
        # This event uses an invalid IP
        ip = "not-an-ip"
    else:
        # These events use a valid IP
        ip = "1.2.3.4"
    try:
        geo = geolocation(ip)
        event["geo"] = geo
    except Exception as e:
        event["geo_error"] = str(e)
    return event
`},
			run: func(t *testing.T, env *bcTestEnv) {
				const versionID = "bc-geo-fail-partial-v1"

				events := []types.TransformerEvent{
					makeEvent("msg-1", versionID),
					makeEvent("msg-2", versionID),
					makeEvent("msg-3", versionID),
				}

				oldResp := env.OldClient.Transform(context.Background(), events)
				t.Logf("Old arch: Events=%d, FailedEvents=%d", len(oldResp.Events), len(oldResp.FailedEvents))
				newResp := env.NewClient.Transform(context.Background(), events)
				t.Logf("New arch: Events=%d, FailedEvents=%d", len(newResp.Events), len(newResp.FailedEvents))

				// All 3 events succeed (errors are caught)
				require.Equal(t, 3, len(oldResp.Events), "old arch: 3 success events expected")
				require.Equal(t, 3, len(newResp.Events), "new arch: 3 success events expected")

				// msg-1 and msg-3 should have geo data, msg-2 should have geo_error
				for _, resp := range []*types.Response{&oldResp, &newResp} {
					for _, ev := range resp.Events {
						msgID, _ := ev.Output["messageId"].(string)
						if msgID == "msg-2" {
							geoErr, _ := ev.Output["geo_error"].(string)
							require.Containsf(t, geoErr, "status code: 400",
								"event %s should have 400 error", msgID)
							require.Nilf(t, ev.Output["geo"],
								"event %s should not have geo data", msgID)
						} else {
							geo, _ := ev.Output["geo"].(map[string]any)
							require.Equalf(t, "1.2.3.4", geo["ip"],
								"event %s geo should contain correct ip", msgID)
							require.Nilf(t, ev.Output["geo_error"],
								"event %s should not have geo_error", msgID)
						}
					}
				}

				diff, equal := oldResp.Equal(&newResp)
				if equal {
					t.Log("Both architectures produce identical responses for partial invalid IP")
				} else {
					t.Errorf("Responses differ:\n%s", diff)
				}
			},
		},
		{
			name:      "GeolocationPrivateIP",
			versionID: "bc-geo-fail-private-v1",
			config: configBackendEntry{code: `
def transformEvent(event, metadata):
    result = geolocation("127.0.0.1")
    event["geo"] = result
    return event
`},
			run: func(t *testing.T, env *bcTestEnv) {
				const versionID = "bc-geo-fail-private-v1"

				events := []types.TransformerEvent{
					makeEvent("msg-1", versionID),
				}

				oldResp := env.OldClient.Transform(context.Background(), events)
				t.Logf("Old arch: Events=%d, FailedEvents=%d", len(oldResp.Events), len(oldResp.FailedEvents))
				newResp := env.NewClient.Transform(context.Background(), events)
				t.Logf("New arch: Events=%d, FailedEvents=%d", len(newResp.Events), len(newResp.FailedEvents))

				// Private IP returns 200 with empty string fields — no error
				require.Equal(t, 1, len(oldResp.Events), "old arch: 1 success event expected")
				require.Equal(t, 0, len(oldResp.FailedEvents), "old arch: no failed events expected")
				require.Equal(t, 1, len(newResp.Events), "new arch: 1 success event expected")
				require.Equal(t, 0, len(newResp.FailedEvents), "new arch: no failed events expected")

				// Geo data should be present with correct IP but empty values
				oldGeo, _ := oldResp.Events[0].Output["geo"].(map[string]any)
				newGeo, _ := newResp.Events[0].Output["geo"].(map[string]any)
				require.Equal(t, "127.0.0.1", oldGeo["ip"], "old arch: geo should contain correct ip")
				require.Equal(t, "127.0.0.1", newGeo["ip"], "new arch: geo should contain correct ip")

				t.Logf("Old arch geo: %v", oldResp.Events[0].Output["geo"])
				t.Logf("New arch geo: %v", newResp.Events[0].Output["geo"])

				diff, equal := oldResp.Equal(&newResp)
				if equal {
					t.Log("Both architectures produce identical responses for private IP")
				} else {
					t.Errorf("Responses differ:\n%s", diff)
				}
			},
		},
		{
			name:      "GeolocationInvalidIPBatchUncaught",
			versionID: "bc-geo-fail-batch-uncaught-v1",
			config: configBackendEntry{code: `
def transformBatch(events, metadata):
    for event in events:
        geo = geolocation("not-an-ip")
        event["geo"] = geo
    return events
`},
			run: func(t *testing.T, env *bcTestEnv) {
				const versionID = "bc-geo-fail-batch-uncaught-v1"

				events := []types.TransformerEvent{
					makeEvent("msg-1", versionID),
					makeEvent("msg-2", versionID),
				}

				oldResp := env.OldClient.Transform(context.Background(), events)
				t.Logf("Old arch: Events=%d, FailedEvents=%d", len(oldResp.Events), len(oldResp.FailedEvents))
				newResp := env.NewClient.Transform(context.Background(), events)
				t.Logf("New arch: Events=%d, FailedEvents=%d", len(newResp.Events), len(newResp.FailedEvents))

				// All events should fail since the exception propagates
				require.Equal(t, 0, len(oldResp.Events), "old arch: no success events expected")
				require.Equal(t, 2, len(oldResp.FailedEvents), "old arch: 2 failed events expected")
				require.Equal(t, 0, len(newResp.Events), "new arch: no success events expected")
				require.Equal(t, 2, len(newResp.FailedEvents), "new arch: 2 failed events expected")

				for i := range oldResp.FailedEvents {
					require.Containsf(t, oldResp.FailedEvents[i].Error, "status code: 400", "old arch: event %d error should mention 400", i)
					require.Containsf(t, newResp.FailedEvents[i].Error, "status code: 400", "new arch: event %d error should mention 400", i)
				}

				diff, equal := oldResp.Equal(&newResp)
				if equal {
					t.Log("Both architectures produce identical responses for batch invalid IP uncaught")
				} else {
					t.Errorf("Responses differ:\n%s", diff)
				}
			},
		},
		{
			name:      "GeolocationBatchNoArgs",
			versionID: "bc-geo-batch-no-args-v1",
			config: configBackendEntry{code: `
def transformBatch(events, metadata):
    for event in events:
        try:
            result = geolocation()
            event["geo"] = result
        except Exception as e:
            event["geo_error"] = str(e)
    return events
`},
			run: func(t *testing.T, env *bcTestEnv) {
				const versionID = "bc-geo-batch-no-args-v1"

				events := []types.TransformerEvent{
					makeEvent("msg-1", versionID),
					makeEvent("msg-2", versionID),
				}

				oldResp := env.OldClient.Transform(context.Background(), events)
				t.Logf("Old arch: Events=%d, FailedEvents=%d", len(oldResp.Events), len(oldResp.FailedEvents))
				newResp := env.NewClient.Transform(context.Background(), events)
				t.Logf("New arch: Events=%d, FailedEvents=%d", len(newResp.Events), len(newResp.FailedEvents))

				require.Equal(t, 2, len(oldResp.Events), "old arch: 2 success events expected")
				require.Equal(t, 2, len(newResp.Events), "new arch: 2 success events expected")

				for i := range oldResp.Events {
					oldError, _ := oldResp.Events[i].Output["geo_error"].(string)
					newError, _ := newResp.Events[i].Output["geo_error"].(string)
					require.Containsf(t, oldError, "single string argument", "old arch: event %d error should mention single string argument", i)
					require.Containsf(t, newError, "single string argument", "new arch: event %d error should mention single string argument", i)
				}

				diff, equal := oldResp.Equal(&newResp)
				if equal {
					t.Log("Both architectures produce identical responses for batch no args")
				} else {
					t.Errorf("Responses differ:\n%s", diff)
				}
			},
		},
		{
			name:      "GeolocationKeywordArg",
			versionID: "bc-geo-keyword-arg-v1",
			config: configBackendEntry{code: `
def transformEvent(event, metadata):
    try:
        result = geolocation(ip="1.2.3.4")
        event["geo"] = result
    except Exception as e:
        event["geo_error"] = str(e)
    return event
`},
			run: func(t *testing.T, env *bcTestEnv) {
				const versionID = "bc-geo-keyword-arg-v1"

				events := []types.TransformerEvent{
					makeEvent("msg-1", versionID),
				}

				oldResp := env.OldClient.Transform(context.Background(), events)
				t.Logf("Old arch: Events=%d, FailedEvents=%d", len(oldResp.Events), len(oldResp.FailedEvents))
				newResp := env.NewClient.Transform(context.Background(), events)
				t.Logf("New arch: Events=%d, FailedEvents=%d", len(newResp.Events), len(newResp.FailedEvents))

				require.Equal(t, 1, len(oldResp.Events), "old arch: 1 success event expected")
				require.Equal(t, 1, len(newResp.Events), "new arch: 1 success event expected")

				oldError, _ := oldResp.Events[0].Output["geo_error"].(string)
				newError, _ := newResp.Events[0].Output["geo_error"].(string)
				t.Logf("Old arch geo_error: %q", oldError)
				t.Logf("New arch geo_error: %q", newError)

				require.NotEmpty(t, oldError, "old arch: should have a geo_error")
				require.NotEmpty(t, newError, "new arch: should have a geo_error")

				diff, equal := oldResp.Equal(&newResp)
				if equal {
					t.Log("Both architectures produce identical responses for keyword arg")
				} else {
					t.Errorf("Responses differ:\n%s", diff)
				}
			},
		},
	}

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	// Start rudder-geolocation container (starts MinIO internally and uploads test MMDB).
	geoContainer, geoURL := startRudderGeolocation(t, pool)
	t.Cleanup(func() {
		if err := pool.Purge(geoContainer); err != nil {
			t.Logf("Failed to purge rudder-geolocation: %v", err)
		}
	})
	waitForGeolocation(t, pool, geoURL)

	// Collect all config backend entries.
	allEntries := make(map[string]configBackendEntry, len(subtests))
	for _, st := range subtests {
		if st.config != (configBackendEntry{}) {
			allEntries[st.versionID] = st.config
		}
	}
	configBackend := newContractConfigBackend(t, allEntries)
	t.Cleanup(configBackend.Close)

	// Mock OpenFaaS gateway with dynamic target.
	var (
		gatewayMu        sync.Mutex
		gatewayTargetURL string
	)
	getGatewayTarget := func() string {
		gatewayMu.Lock()
		defer gatewayMu.Unlock()
		return gatewayTargetURL
	}
	setGatewayTarget := func(url string) {
		gatewayMu.Lock()
		defer gatewayMu.Unlock()
		gatewayTargetURL = url
	}
	mockGateway, _ := newMockOpenFaaSGateway(t, getGatewayTarget)
	t.Cleanup(mockGateway.Close)

	var (
		wg                               sync.WaitGroup
		transformerURL, pyTransformerURL string
	)
	wg.Go(func() {
		transformerURL = startRudderTransformer(t, pool, configBackend.URL, mockGateway.URL)
	})
	wg.Go(func() {
		pyTransformerURL = startRudderPytransformer(t, pool, configBackend.URL, "GEOLOCATION_URL="+geoURL)
	})
	wg.Wait()

	// Run subtests sequentially.
	for _, st := range subtests {
		t.Run(st.name, func(t *testing.T) {
			env := newBCTestEnv(t, transformerURL, pyTransformerURL, withLimitedRetryableHTTPRetries())

			if st.config.code != "" {
				t.Logf("Starting openfaas-flask-base for %s (versionID=%s)...", st.name, st.versionID)
				openFaasURL := startOpenFaasFlask(t, pool, st.versionID, configBackend.URL, "geolocation_url="+geoURL)

				setGatewayTarget(openFaasURL)
			}

			st.run(t, env)
			env.assertRetryCountsMatch(t)
		})
	}
}

// TestBackwardsCompatibilityGeolocationNotConfigured tests geolocation behavior
// when the geolocation service URL is NOT configured in either architecture.
// Both should produce the same "not supported" error.
func TestBackwardsCompatibilityGeolocationNotConfigured(t *testing.T) {
	type subtest struct {
		name      string
		versionID string
		config    configBackendEntry
		run       func(t *testing.T, env *bcTestEnv)
	}

	subtests := []subtest{
		{
			name:      "GeolocationNotConfigured",
			versionID: "bc-geo-not-configured-v1",
			config: configBackendEntry{code: `
def transformEvent(event, metadata):
    try:
        result = geolocation("1.2.3.4")
        event["geo"] = result
    except Exception as e:
        event["geo_error"] = str(e)
    return event
`},
			run: func(t *testing.T, env *bcTestEnv) {
				const versionID = "bc-geo-not-configured-v1"

				events := []types.TransformerEvent{
					makeEvent("msg-1", versionID),
				}

				oldResp := env.OldClient.Transform(context.Background(), events)
				t.Logf("Old arch: Events=%d, FailedEvents=%d", len(oldResp.Events), len(oldResp.FailedEvents))
				newResp := env.NewClient.Transform(context.Background(), events)
				t.Logf("New arch: Events=%d, FailedEvents=%d", len(newResp.Events), len(newResp.FailedEvents))

				// Both should succeed (error caught by try/except)
				require.Equal(t, 1, len(oldResp.Events), "old arch: 1 success event expected")
				require.Equal(t, 1, len(newResp.Events), "new arch: 1 success event expected")

				// Both should report "not supported" error
				oldError, _ := oldResp.Events[0].Output["geo_error"].(string)
				newError, _ := newResp.Events[0].Output["geo_error"].(string)
				t.Logf("Old arch geo_error: %q", oldError)
				t.Logf("New arch geo_error: %q", newError)

				require.Contains(t, oldError, "not supported", "old arch: error should mention not supported")
				require.Contains(t, newError, "not supported", "new arch: error should mention not supported")

				diff, equal := oldResp.Equal(&newResp)
				if equal {
					t.Log("Both architectures produce identical responses when geolocation is not configured")
				} else {
					t.Errorf("Responses differ:\n%s", diff)
				}
			},
		},
		{
			name:      "GeolocationNotConfiguredUncaught",
			versionID: "bc-geo-not-configured-uncaught-v1",
			config: configBackendEntry{code: `
def transformEvent(event, metadata):
    # Call geolocation without try/catch — should produce a failed event
    result = geolocation("1.2.3.4")
    event["geo"] = result
    return event
`},
			run: func(t *testing.T, env *bcTestEnv) {
				const versionID = "bc-geo-not-configured-uncaught-v1"

				events := []types.TransformerEvent{
					makeEvent("msg-1", versionID),
				}

				oldResp := env.OldClient.Transform(context.Background(), events)
				t.Logf("Old arch: Events=%d, FailedEvents=%d", len(oldResp.Events), len(oldResp.FailedEvents))
				newResp := env.NewClient.Transform(context.Background(), events)
				t.Logf("New arch: Events=%d, FailedEvents=%d", len(newResp.Events), len(newResp.FailedEvents))

				// Both should fail since exception propagates
				require.Equal(t, 0, len(oldResp.Events), "old arch: no success events expected")
				require.Equal(t, 1, len(oldResp.FailedEvents), "old arch: 1 failed event expected")
				require.Equal(t, 0, len(newResp.Events), "new arch: no success events expected")
				require.Equal(t, 1, len(newResp.FailedEvents), "new arch: 1 failed event expected")

				oldError := oldResp.FailedEvents[0].Error
				newError := newResp.FailedEvents[0].Error
				t.Logf("Old arch error: %q", oldError)
				t.Logf("New arch error: %q", newError)

				require.Contains(t, oldError, "not supported", "old arch: error should mention not supported")
				require.Contains(t, newError, "not supported", "new arch: error should mention not supported")

				diff, equal := oldResp.Equal(&newResp)
				if equal {
					t.Log("Both architectures produce identical error for uncaught geolocation not configured")
				} else {
					t.Errorf("Responses differ:\n%s", diff)
				}
			},
		},
		{
			name:      "GeolocationNotConfiguredBatch",
			versionID: "bc-geo-not-configured-batch-v1",
			config: configBackendEntry{code: `
def transformBatch(events, metadata):
    for event in events:
        try:
            geo = geolocation("1.2.3.4")
            event["geo"] = geo
        except Exception as e:
            event["geo_error"] = str(e)
    return events
`},
			run: func(t *testing.T, env *bcTestEnv) {
				const versionID = "bc-geo-not-configured-batch-v1"

				events := []types.TransformerEvent{
					makeEvent("msg-1", versionID),
					makeEvent("msg-2", versionID),
				}

				oldResp := env.OldClient.Transform(context.Background(), events)
				t.Logf("Old arch: Events=%d, FailedEvents=%d", len(oldResp.Events), len(oldResp.FailedEvents))
				newResp := env.NewClient.Transform(context.Background(), events)
				t.Logf("New arch: Events=%d, FailedEvents=%d", len(newResp.Events), len(newResp.FailedEvents))

				require.Equal(t, 2, len(oldResp.Events), "old arch: 2 success events expected")
				require.Equal(t, 2, len(newResp.Events), "new arch: 2 success events expected")

				// Both should have geo_error for all events
				for i := range oldResp.Events {
					oldError, _ := oldResp.Events[i].Output["geo_error"].(string)
					newError, _ := newResp.Events[i].Output["geo_error"].(string)
					require.Containsf(t, oldError, "not supported", "old arch: event %d error should mention not supported", i)
					require.Containsf(t, newError, "not supported", "new arch: event %d error should mention not supported", i)
				}

				diff, equal := oldResp.Equal(&newResp)
				if equal {
					t.Log("Both architectures produce identical responses for batch geolocation not configured")
				} else {
					t.Errorf("Responses differ:\n%s", diff)
				}
			},
		},
		{
			name:      "GeolocationNotConfiguredBatchUncaught",
			versionID: "bc-geo-not-configured-batch-uncaught-v1",
			config: configBackendEntry{code: `
def transformBatch(events, metadata):
    for event in events:
        geo = geolocation("1.2.3.4")
        event["geo"] = geo
    return events
`},
			run: func(t *testing.T, env *bcTestEnv) {
				const versionID = "bc-geo-not-configured-batch-uncaught-v1"

				events := []types.TransformerEvent{
					makeEvent("msg-1", versionID),
					makeEvent("msg-2", versionID),
				}

				oldResp := env.OldClient.Transform(context.Background(), events)
				t.Logf("Old arch: Events=%d, FailedEvents=%d", len(oldResp.Events), len(oldResp.FailedEvents))
				newResp := env.NewClient.Transform(context.Background(), events)
				t.Logf("New arch: Events=%d, FailedEvents=%d", len(newResp.Events), len(newResp.FailedEvents))

				// All events should fail since the exception propagates
				require.Equal(t, 0, len(oldResp.Events), "old arch: no success events expected")
				require.Equal(t, 2, len(oldResp.FailedEvents), "old arch: 2 failed events expected")
				require.Equal(t, 0, len(newResp.Events), "new arch: no success events expected")
				require.Equal(t, 2, len(newResp.FailedEvents), "new arch: 2 failed events expected")

				for i := range oldResp.FailedEvents {
					require.Containsf(t, oldResp.FailedEvents[i].Error, "not supported", "old arch: event %d error should mention not supported", i)
					require.Containsf(t, newResp.FailedEvents[i].Error, "not supported", "new arch: event %d error should mention not supported", i)
				}

				diff, equal := oldResp.Equal(&newResp)
				if equal {
					t.Log("Both architectures produce identical responses for batch geolocation not configured uncaught")
				} else {
					t.Errorf("Responses differ:\n%s", diff)
				}
			},
		},
	}

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	// Collect all config backend entries.
	allEntries := make(map[string]configBackendEntry, len(subtests))
	for _, st := range subtests {
		if st.config != (configBackendEntry{}) {
			allEntries[st.versionID] = st.config
		}
	}
	configBackend := newContractConfigBackend(t, allEntries)
	t.Cleanup(configBackend.Close)

	// Mock OpenFaaS gateway with dynamic target.
	var (
		gatewayMu        sync.Mutex
		gatewayTargetURL string
	)
	getGatewayTarget := func() string {
		gatewayMu.Lock()
		defer gatewayMu.Unlock()
		return gatewayTargetURL
	}
	setGatewayTarget := func(url string) {
		gatewayMu.Lock()
		defer gatewayMu.Unlock()
		gatewayTargetURL = url
	}
	mockGateway, _ := newMockOpenFaaSGateway(t, getGatewayTarget)
	t.Cleanup(mockGateway.Close)

	var (
		wg                               sync.WaitGroup
		transformerURL, pyTransformerURL string
	)
	wg.Go(func() {
		transformerURL = startRudderTransformer(t, pool, configBackend.URL, mockGateway.URL)
	})
	wg.Go(func() {
		pyTransformerURL = startRudderPytransformer(t, pool, configBackend.URL)
	})
	wg.Wait()

	for _, st := range subtests {
		t.Run(st.name, func(t *testing.T) {
			env := newBCTestEnv(t, transformerURL, pyTransformerURL, withLimitedRetryableHTTPRetries())

			if st.config.code != "" {
				// Start openfaas WITHOUT geolocation URL (not configured test).
				openFaasURL := startOpenFaasFlask(t, pool, st.versionID, configBackend.URL)

				setGatewayTarget(openFaasURL)
			}

			st.run(t, env)
			env.assertRetryCountsMatch(t)
		})
	}
}

// TestBackwardsCompatibilityGeolocationFailure tests behavior when the
// geolocation service experiences network failures or returns various HTTP
// error status codes. Uses a configurable mock geolocation service to
// simulate different failure scenarios.
// Both architectures raise: "geolocation fetch failed with status code: {code}"
func TestBackwardsCompatibilityGeolocationFailure(t *testing.T) {
	type subtest struct {
		name                string
		versionID           string
		config              configBackendEntry
		setup               func() // called before run to configure mock behavior
		run                 func(t *testing.T, env *bcTestEnv)
		skipRetryCountMatch bool // skip assertRetryCountsMatch when old/new arch retry differently
	}

	mockGeoService, mockGeoCfg := newConfigurableMockGeolocationService(t)
	t.Cleanup(mockGeoService.Close)
	geoURL := mockGeoService.URL

	subtests := []subtest{
		{
			name:      "GeoStatus500",
			versionID: "bc-geo-status-500-v1",
			config: configBackendEntry{code: `
def transformEvent(event, metadata):
    try:
        result = geolocation("1.2.3.4")
        event["geo"] = result
    except Exception as e:
        event["geo_error"] = str(e)
    return event
`},
			setup:               func() { mockGeoCfg.setResponse(500) },
			skipRetryCountMatch: true,
			run: func(t *testing.T, env *bcTestEnv) {
				const versionID = "bc-geo-status-500-v1"

				events := []types.TransformerEvent{
					makeEvent("msg-1", versionID),
				}

				oldResp := env.OldClient.Transform(context.Background(), events)
				t.Logf("Old arch: Events=%d, FailedEvents=%d", len(oldResp.Events), len(oldResp.FailedEvents))

				// Old arch: except Exception catches the error → success event with geo_error
				require.Equal(t, 1, len(oldResp.Events), "old arch: 1 success event expected")
				oldError, _ := oldResp.Events[0].Output["geo_error"].(string)
				t.Logf("Old arch geo_error: %q", oldError)
				require.Contains(t, oldError, "status code: 500", "old arch: error should mention 500")

				// New arch: GeolocationServerError(BaseException) bypasses except Exception
				// → propagates as HTTP 503 with retry → retries exhausted → failed event
				newResp := env.NewClient.Transform(context.Background(), events)
				t.Logf("New arch: Events=%d, FailedEvents=%d", len(newResp.Events), len(newResp.FailedEvents))
				require.Equal(t, 0, len(newResp.Events), "new arch: no success events expected (geo 5xx triggers retries)")
				require.Equal(t, 1, len(newResp.FailedEvents), "new arch: 1 failed event expected")
				t.Logf("New arch error: %q", newResp.FailedEvents[0].Error)
			},
		},
		{
			name:      "GeoStatus502",
			versionID: "bc-geo-status-502-v1",
			config: configBackendEntry{code: `
def transformEvent(event, metadata):
    try:
        result = geolocation("1.2.3.4")
        event["geo"] = result
    except Exception as e:
        event["geo_error"] = str(e)
    return event
`},
			setup:               func() { mockGeoCfg.setResponse(502) },
			skipRetryCountMatch: true,
			run: func(t *testing.T, env *bcTestEnv) {
				const versionID = "bc-geo-status-502-v1"

				events := []types.TransformerEvent{
					makeEvent("msg-1", versionID),
				}

				oldResp := env.OldClient.Transform(context.Background(), events)
				t.Logf("Old arch: Events=%d, FailedEvents=%d", len(oldResp.Events), len(oldResp.FailedEvents))
				require.Equal(t, 1, len(oldResp.Events), "old arch: 1 success event expected")
				oldError, _ := oldResp.Events[0].Output["geo_error"].(string)
				t.Logf("Old arch geo_error: %q", oldError)
				require.Contains(t, oldError, "status code: 502", "old arch: error should mention 502")

				newResp := env.NewClient.Transform(context.Background(), events)
				t.Logf("New arch: Events=%d, FailedEvents=%d", len(newResp.Events), len(newResp.FailedEvents))
				require.Equal(t, 0, len(newResp.Events), "new arch: no success events expected (geo 5xx triggers retries)")
				require.Equal(t, 1, len(newResp.FailedEvents), "new arch: 1 failed event expected")
				t.Logf("New arch error: %q", newResp.FailedEvents[0].Error)
			},
		},
		{
			name:      "GeoStatus503",
			versionID: "bc-geo-status-503-v1",
			config: configBackendEntry{code: `
def transformEvent(event, metadata):
    try:
        result = geolocation("1.2.3.4")
        event["geo"] = result
    except Exception as e:
        event["geo_error"] = str(e)
    return event
`},
			setup:               func() { mockGeoCfg.setResponse(503) },
			skipRetryCountMatch: true,
			run: func(t *testing.T, env *bcTestEnv) {
				const versionID = "bc-geo-status-503-v1"

				events := []types.TransformerEvent{
					makeEvent("msg-1", versionID),
				}

				oldResp := env.OldClient.Transform(context.Background(), events)
				t.Logf("Old arch: Events=%d, FailedEvents=%d", len(oldResp.Events), len(oldResp.FailedEvents))
				require.Equal(t, 1, len(oldResp.Events), "old arch: 1 success event expected")
				oldError, _ := oldResp.Events[0].Output["geo_error"].(string)
				t.Logf("Old arch geo_error: %q", oldError)
				require.Contains(t, oldError, "status code: 503", "old arch: error should mention 503")

				newResp := env.NewClient.Transform(context.Background(), events)
				t.Logf("New arch: Events=%d, FailedEvents=%d", len(newResp.Events), len(newResp.FailedEvents))
				require.Equal(t, 0, len(newResp.Events), "new arch: no success events expected (geo 5xx triggers retries)")
				require.Equal(t, 1, len(newResp.FailedEvents), "new arch: 1 failed event expected")
				t.Logf("New arch error: %q", newResp.FailedEvents[0].Error)
			},
		},
		{
			name:      "GeoStatus429",
			versionID: "bc-geo-status-429-v1",
			config: configBackendEntry{code: `
def transformEvent(event, metadata):
    try:
        result = geolocation("1.2.3.4")
        event["geo"] = result
    except Exception as e:
        event["geo_error"] = str(e)
    return event
`},
			setup: func() { mockGeoCfg.setResponse(429) },
			run: func(t *testing.T, env *bcTestEnv) {
				const versionID = "bc-geo-status-429-v1"

				events := []types.TransformerEvent{
					makeEvent("msg-1", versionID),
				}

				oldResp := env.OldClient.Transform(context.Background(), events)
				t.Logf("Old arch: Events=%d, FailedEvents=%d", len(oldResp.Events), len(oldResp.FailedEvents))
				newResp := env.NewClient.Transform(context.Background(), events)
				t.Logf("New arch: Events=%d, FailedEvents=%d", len(newResp.Events), len(newResp.FailedEvents))

				require.Equal(t, 1, len(oldResp.Events), "old arch: 1 success event expected")
				require.Equal(t, 1, len(newResp.Events), "new arch: 1 success event expected")

				oldError, _ := oldResp.Events[0].Output["geo_error"].(string)
				newError, _ := newResp.Events[0].Output["geo_error"].(string)
				t.Logf("Old arch geo_error: %q", oldError)
				t.Logf("New arch geo_error: %q", newError)

				require.Contains(t, oldError, "status code: 429", "old arch: error should mention 429")
				require.Contains(t, newError, "status code: 429", "new arch: error should mention 429")

				diff, equal := oldResp.Equal(&newResp)
				if equal {
					t.Log("Both architectures produce identical responses for 429")
				} else {
					t.Errorf("Responses differ:\n%s", diff)
				}
			},
		},
		{
			name:      "GeoStatus500Uncaught",
			versionID: "bc-geo-status-500-uncaught-v1",
			config: configBackendEntry{code: `
def transformEvent(event, metadata):
    result = geolocation("1.2.3.4")
    event["geo"] = result
    return event
`},
			setup:               func() { mockGeoCfg.setResponse(500) },
			skipRetryCountMatch: true,
			run: func(t *testing.T, env *bcTestEnv) {
				const versionID = "bc-geo-status-500-uncaught-v1"

				events := []types.TransformerEvent{
					makeEvent("msg-1", versionID),
				}

				oldResp := env.OldClient.Transform(context.Background(), events)
				t.Logf("Old arch: Events=%d, FailedEvents=%d", len(oldResp.Events), len(oldResp.FailedEvents))

				// Both produce failed events, but via different paths:
				// Old arch: exception propagates → failed event with "status code: 500"
				// New arch: GeolocationServerError → 503 retries → exhausted → failed event
				require.Equal(t, 0, len(oldResp.Events), "old arch: no success events expected")
				require.Equal(t, 1, len(oldResp.FailedEvents), "old arch: 1 failed event expected")
				oldError := oldResp.FailedEvents[0].Error
				t.Logf("Old arch error: %q", oldError)
				require.Contains(t, oldError, "status code: 500", "old arch: error should mention 500")

				newResp := env.NewClient.Transform(context.Background(), events)
				t.Logf("New arch: Events=%d, FailedEvents=%d", len(newResp.Events), len(newResp.FailedEvents))
				require.Equal(t, 0, len(newResp.Events), "new arch: no success events expected")
				require.Equal(t, 1, len(newResp.FailedEvents), "new arch: 1 failed event expected")
				t.Logf("New arch error: %q", newResp.FailedEvents[0].Error)
			},
		},
		{
			name:      "GeoStatus500Batch",
			versionID: "bc-geo-status-500-batch-v1",
			config: configBackendEntry{code: `
def transformBatch(events, metadata):
    for event in events:
        try:
            geo = geolocation("1.2.3.4")
            event["geo"] = geo
        except Exception as e:
            event["geo_error"] = str(e)
    return events
`},
			setup:               func() { mockGeoCfg.setResponse(500) },
			skipRetryCountMatch: true,
			run: func(t *testing.T, env *bcTestEnv) {
				const versionID = "bc-geo-status-500-batch-v1"

				events := []types.TransformerEvent{
					makeEvent("msg-1", versionID),
					makeEvent("msg-2", versionID),
					makeEvent("msg-3", versionID),
				}

				oldResp := env.OldClient.Transform(context.Background(), events)
				t.Logf("Old arch: Events=%d, FailedEvents=%d", len(oldResp.Events), len(oldResp.FailedEvents))
				require.Equal(t, 3, len(oldResp.Events), "old arch: 3 success events expected")
				for i := range oldResp.Events {
					oldError, _ := oldResp.Events[i].Output["geo_error"].(string)
					require.Containsf(t, oldError, "status code: 500", "old arch: event %d error should mention 500", i)
				}

				newResp := env.NewClient.Transform(context.Background(), events)
				t.Logf("New arch: Events=%d, FailedEvents=%d", len(newResp.Events), len(newResp.FailedEvents))
				require.Equal(t, 0, len(newResp.Events), "new arch: no success events expected (geo 5xx triggers retries)")
				require.Equal(t, 3, len(newResp.FailedEvents), "new arch: 3 failed events expected")
			},
		},
		{
			name:      "GeoStatus502Batch",
			versionID: "bc-geo-status-502-batch-v1",
			config: configBackendEntry{code: `
def transformBatch(events, metadata):
    for event in events:
        try:
            geo = geolocation("1.2.3.4")
            event["geo"] = geo
        except Exception as e:
            event["geo_error"] = str(e)
    return events
`},
			setup:               func() { mockGeoCfg.setResponse(502) },
			skipRetryCountMatch: true,
			run: func(t *testing.T, env *bcTestEnv) {
				const versionID = "bc-geo-status-502-batch-v1"

				events := []types.TransformerEvent{
					makeEvent("msg-1", versionID),
					makeEvent("msg-2", versionID),
					makeEvent("msg-3", versionID),
				}

				oldResp := env.OldClient.Transform(context.Background(), events)
				t.Logf("Old arch: Events=%d, FailedEvents=%d", len(oldResp.Events), len(oldResp.FailedEvents))
				require.Equal(t, 3, len(oldResp.Events), "old arch: 3 success events expected")
				for i := range oldResp.Events {
					oldError, _ := oldResp.Events[i].Output["geo_error"].(string)
					require.Containsf(t, oldError, "status code: 502", "old arch: event %d error should mention 502", i)
				}

				newResp := env.NewClient.Transform(context.Background(), events)
				t.Logf("New arch: Events=%d, FailedEvents=%d", len(newResp.Events), len(newResp.FailedEvents))
				require.Equal(t, 0, len(newResp.Events), "new arch: no success events expected (geo 5xx triggers retries)")
				require.Equal(t, 3, len(newResp.FailedEvents), "new arch: 3 failed events expected")
			},
		},
		{
			name:      "GeoStatus503Batch",
			versionID: "bc-geo-status-503-batch-v1",
			config: configBackendEntry{code: `
def transformBatch(events, metadata):
    for event in events:
        try:
            geo = geolocation("1.2.3.4")
            event["geo"] = geo
        except Exception as e:
            event["geo_error"] = str(e)
    return events
`},
			setup:               func() { mockGeoCfg.setResponse(503) },
			skipRetryCountMatch: true,
			run: func(t *testing.T, env *bcTestEnv) {
				const versionID = "bc-geo-status-503-batch-v1"

				events := []types.TransformerEvent{
					makeEvent("msg-1", versionID),
					makeEvent("msg-2", versionID),
					makeEvent("msg-3", versionID),
				}

				oldResp := env.OldClient.Transform(context.Background(), events)
				t.Logf("Old arch: Events=%d, FailedEvents=%d", len(oldResp.Events), len(oldResp.FailedEvents))
				require.Equal(t, 3, len(oldResp.Events), "old arch: 3 success events expected")
				for i := range oldResp.Events {
					oldError, _ := oldResp.Events[i].Output["geo_error"].(string)
					require.Containsf(t, oldError, "status code: 503", "old arch: event %d error should mention 503", i)
				}

				newResp := env.NewClient.Transform(context.Background(), events)
				t.Logf("New arch: Events=%d, FailedEvents=%d", len(newResp.Events), len(newResp.FailedEvents))
				require.Equal(t, 0, len(newResp.Events), "new arch: no success events expected (geo 5xx triggers retries)")
				require.Equal(t, 3, len(newResp.FailedEvents), "new arch: 3 failed events expected")
			},
		},
		{
			name:      "GeoStatus429Batch",
			versionID: "bc-geo-status-429-batch-v1",
			config: configBackendEntry{code: `
def transformBatch(events, metadata):
    for event in events:
        try:
            geo = geolocation("1.2.3.4")
            event["geo"] = geo
        except Exception as e:
            event["geo_error"] = str(e)
    return events
`},
			setup: func() { mockGeoCfg.setResponse(429) },
			run: func(t *testing.T, env *bcTestEnv) {
				const versionID = "bc-geo-status-429-batch-v1"

				events := []types.TransformerEvent{
					makeEvent("msg-1", versionID),
					makeEvent("msg-2", versionID),
					makeEvent("msg-3", versionID),
				}

				oldResp := env.OldClient.Transform(context.Background(), events)
				t.Logf("Old arch: Events=%d, FailedEvents=%d", len(oldResp.Events), len(oldResp.FailedEvents))
				newResp := env.NewClient.Transform(context.Background(), events)
				t.Logf("New arch: Events=%d, FailedEvents=%d", len(newResp.Events), len(newResp.FailedEvents))

				require.Equal(t, 3, len(oldResp.Events), "old arch: 3 success events expected")
				require.Equal(t, 3, len(newResp.Events), "new arch: 3 success events expected")

				for i := range oldResp.Events {
					oldError, _ := oldResp.Events[i].Output["geo_error"].(string)
					newError, _ := newResp.Events[i].Output["geo_error"].(string)
					require.Containsf(t, oldError, "status code: 429", "old arch: event %d error should mention 429", i)
					require.Containsf(t, newError, "status code: 429", "new arch: event %d error should mention 429", i)
				}

				diff, equal := oldResp.Equal(&newResp)
				if equal {
					t.Log("Both architectures produce identical responses for batch 429")
				} else {
					t.Errorf("Responses differ:\n%s", diff)
				}
			},
		},
		{
			name:      "GeoConnectionReset",
			versionID: "bc-geo-conn-reset-v1",
			config: configBackendEntry{code: `
def transformEvent(event, metadata):
    try:
        result = geolocation("1.2.3.4")
        event["geo"] = result
    except Exception as e:
        event["geo_error"] = str(e)
    return event
`},
			setup:               func() { mockGeoCfg.setConnectionClose() },
			skipRetryCountMatch: true,
			run: func(t *testing.T, env *bcTestEnv) {
				const versionID = "bc-geo-conn-reset-v1"

				events := []types.TransformerEvent{
					makeEvent("msg-1", versionID),
				}

				// Old arch: connection error raises Exception → caught by except → success with geo_error
				oldResp := env.OldClient.Transform(context.Background(), events)
				t.Logf("Old arch: Events=%d, FailedEvents=%d", len(oldResp.Events), len(oldResp.FailedEvents))
				require.Equal(t, 1, len(oldResp.Events), "old arch: 1 success event expected")
				oldError, _ := oldResp.Events[0].Output["geo_error"].(string)
				t.Logf("Old arch geo_error: %q", oldError)
				require.NotEmpty(t, oldError, "old arch: should have a geo_error")

				// New arch: connection error → GeolocationServerError(BaseException) → 503 retries → failed event
				newResp := env.NewClient.Transform(context.Background(), events)
				t.Logf("New arch: Events=%d, FailedEvents=%d", len(newResp.Events), len(newResp.FailedEvents))
				require.Equal(t, 0, len(newResp.Events), "new arch: no success events expected (geo errors trigger retries)")
				require.Equal(t, 1, len(newResp.FailedEvents), "new arch: 1 failed event expected")
				t.Logf("New arch error: %q", newResp.FailedEvents[0].Error)
			},
		},
		{
			name:      "GeoConnectionResetBatch",
			versionID: "bc-geo-conn-reset-batch-v1",
			config: configBackendEntry{code: `
def transformBatch(events, metadata):
    for event in events:
        try:
            geo = geolocation("1.2.3.4")
            event["geo"] = geo
        except Exception as e:
            event["geo_error"] = str(e)
    return events
`},
			setup:               func() { mockGeoCfg.setConnectionClose() },
			skipRetryCountMatch: true,
			run: func(t *testing.T, env *bcTestEnv) {
				const versionID = "bc-geo-conn-reset-batch-v1"

				events := []types.TransformerEvent{
					makeEvent("msg-1", versionID),
					makeEvent("msg-2", versionID),
					makeEvent("msg-3", versionID),
				}

				oldResp := env.OldClient.Transform(context.Background(), events)
				t.Logf("Old arch: Events=%d, FailedEvents=%d", len(oldResp.Events), len(oldResp.FailedEvents))
				require.Equal(t, 3, len(oldResp.Events), "old arch: 3 success events expected")
				for i := range oldResp.Events {
					oldError, _ := oldResp.Events[i].Output["geo_error"].(string)
					require.NotEmptyf(t, oldError, "old arch: event %d should have a geo_error", i)
				}

				newResp := env.NewClient.Transform(context.Background(), events)
				t.Logf("New arch: Events=%d, FailedEvents=%d", len(newResp.Events), len(newResp.FailedEvents))
				require.Equal(t, 0, len(newResp.Events), "new arch: no success events expected (geo errors trigger retries)")
				require.Equal(t, 3, len(newResp.FailedEvents), "new arch: 3 failed events expected")
			},
		},
		{
			name:      "GeoStatus400",
			versionID: "bc-geo-status-400-v1",
			config: configBackendEntry{code: `
def transformEvent(event, metadata):
    try:
        result = geolocation("1.2.3.4")
        event["geo"] = result
    except Exception as e:
        event["geo_error"] = str(e)
    return event
`},
			setup: func() { mockGeoCfg.setResponse(400) },
			run: func(t *testing.T, env *bcTestEnv) {
				const versionID = "bc-geo-status-400-v1"

				events := []types.TransformerEvent{
					makeEvent("msg-1", versionID),
				}

				oldResp := env.OldClient.Transform(context.Background(), events)
				t.Logf("Old arch: Events=%d, FailedEvents=%d", len(oldResp.Events), len(oldResp.FailedEvents))
				newResp := env.NewClient.Transform(context.Background(), events)
				t.Logf("New arch: Events=%d, FailedEvents=%d", len(newResp.Events), len(newResp.FailedEvents))

				require.Equal(t, 1, len(oldResp.Events), "old arch: 1 success event expected")
				require.Equal(t, 1, len(newResp.Events), "new arch: 1 success event expected")

				oldError, _ := oldResp.Events[0].Output["geo_error"].(string)
				newError, _ := newResp.Events[0].Output["geo_error"].(string)
				t.Logf("Old arch geo_error: %q", oldError)
				t.Logf("New arch geo_error: %q", newError)

				require.Contains(t, oldError, "status code: 400", "old arch: error should mention 400")
				require.Contains(t, newError, "status code: 400", "new arch: error should mention 400")

				diff, equal := oldResp.Equal(&newResp)
				if equal {
					t.Log("Both architectures produce identical responses for 400")
				} else {
					t.Errorf("Responses differ:\n%s", diff)
				}
			},
		},
		{
			name:      "GeoStatus502Uncaught",
			versionID: "bc-geo-status-502-uncaught-v1",
			config: configBackendEntry{code: `
def transformEvent(event, metadata):
    result = geolocation("1.2.3.4")
    event["geo"] = result
    return event
`},
			setup:               func() { mockGeoCfg.setResponse(502) },
			skipRetryCountMatch: true,
			run: func(t *testing.T, env *bcTestEnv) {
				const versionID = "bc-geo-status-502-uncaught-v1"

				events := []types.TransformerEvent{
					makeEvent("msg-1", versionID),
				}

				oldResp := env.OldClient.Transform(context.Background(), events)
				t.Logf("Old arch: Events=%d, FailedEvents=%d", len(oldResp.Events), len(oldResp.FailedEvents))
				require.Equal(t, 0, len(oldResp.Events), "old arch: no success events expected")
				require.Equal(t, 1, len(oldResp.FailedEvents), "old arch: 1 failed event expected")
				t.Logf("Old arch error: %q", oldResp.FailedEvents[0].Error)
				require.Contains(t, oldResp.FailedEvents[0].Error, "status code: 502", "old arch: error should mention 502")

				newResp := env.NewClient.Transform(context.Background(), events)
				t.Logf("New arch: Events=%d, FailedEvents=%d", len(newResp.Events), len(newResp.FailedEvents))
				require.Equal(t, 0, len(newResp.Events), "new arch: no success events expected")
				require.Equal(t, 1, len(newResp.FailedEvents), "new arch: 1 failed event expected")
				t.Logf("New arch error: %q", newResp.FailedEvents[0].Error)
			},
		},
		{
			name:      "GeoStatus503Uncaught",
			versionID: "bc-geo-status-503-uncaught-v1",
			config: configBackendEntry{code: `
def transformEvent(event, metadata):
    result = geolocation("1.2.3.4")
    event["geo"] = result
    return event
`},
			setup:               func() { mockGeoCfg.setResponse(503) },
			skipRetryCountMatch: true,
			run: func(t *testing.T, env *bcTestEnv) {
				const versionID = "bc-geo-status-503-uncaught-v1"

				events := []types.TransformerEvent{
					makeEvent("msg-1", versionID),
				}

				oldResp := env.OldClient.Transform(context.Background(), events)
				t.Logf("Old arch: Events=%d, FailedEvents=%d", len(oldResp.Events), len(oldResp.FailedEvents))
				require.Equal(t, 0, len(oldResp.Events), "old arch: no success events expected")
				require.Equal(t, 1, len(oldResp.FailedEvents), "old arch: 1 failed event expected")
				t.Logf("Old arch error: %q", oldResp.FailedEvents[0].Error)
				require.Contains(t, oldResp.FailedEvents[0].Error, "status code: 503", "old arch: error should mention 503")

				newResp := env.NewClient.Transform(context.Background(), events)
				t.Logf("New arch: Events=%d, FailedEvents=%d", len(newResp.Events), len(newResp.FailedEvents))
				require.Equal(t, 0, len(newResp.Events), "new arch: no success events expected")
				require.Equal(t, 1, len(newResp.FailedEvents), "new arch: 1 failed event expected")
				t.Logf("New arch error: %q", newResp.FailedEvents[0].Error)
			},
		},
		{
			name:      "GeoStatus429Uncaught",
			versionID: "bc-geo-status-429-uncaught-v1",
			config: configBackendEntry{code: `
def transformEvent(event, metadata):
    result = geolocation("1.2.3.4")
    event["geo"] = result
    return event
`},
			setup: func() { mockGeoCfg.setResponse(429) },
			run: func(t *testing.T, env *bcTestEnv) {
				const versionID = "bc-geo-status-429-uncaught-v1"

				events := []types.TransformerEvent{
					makeEvent("msg-1", versionID),
				}

				oldResp := env.OldClient.Transform(context.Background(), events)
				t.Logf("Old arch: Events=%d, FailedEvents=%d", len(oldResp.Events), len(oldResp.FailedEvents))
				newResp := env.NewClient.Transform(context.Background(), events)
				t.Logf("New arch: Events=%d, FailedEvents=%d", len(newResp.Events), len(newResp.FailedEvents))

				require.Equal(t, 0, len(oldResp.Events), "old arch: no success events expected")
				require.Equal(t, 1, len(oldResp.FailedEvents), "old arch: 1 failed event expected")
				require.Equal(t, 0, len(newResp.Events), "new arch: no success events expected")
				require.Equal(t, 1, len(newResp.FailedEvents), "new arch: 1 failed event expected")

				oldError := oldResp.FailedEvents[0].Error
				newError := newResp.FailedEvents[0].Error
				t.Logf("Old arch error: %q", oldError)
				t.Logf("New arch error: %q", newError)

				require.Contains(t, oldError, "status code: 429", "old arch: error should mention 429")
				require.Contains(t, newError, "status code: 429", "new arch: error should mention 429")

				diff, equal := oldResp.Equal(&newResp)
				if equal {
					t.Log("Both architectures produce identical error for uncaught 429")
				} else {
					t.Errorf("Responses differ:\n%s", diff)
				}
			},
		},
		{
			name:      "GeoConnectionResetUncaught",
			versionID: "bc-geo-conn-reset-uncaught-v1",
			config: configBackendEntry{code: `
def transformEvent(event, metadata):
    result = geolocation("1.2.3.4")
    event["geo"] = result
    return event
`},
			setup:               func() { mockGeoCfg.setConnectionClose() },
			skipRetryCountMatch: true,
			run: func(t *testing.T, env *bcTestEnv) {
				const versionID = "bc-geo-conn-reset-uncaught-v1"

				events := []types.TransformerEvent{
					makeEvent("msg-1", versionID),
				}

				oldResp := env.OldClient.Transform(context.Background(), events)
				t.Logf("Old arch: Events=%d, FailedEvents=%d", len(oldResp.Events), len(oldResp.FailedEvents))
				require.Equal(t, 0, len(oldResp.Events), "old arch: no success events expected")
				require.Equal(t, 1, len(oldResp.FailedEvents), "old arch: 1 failed event expected")
				t.Logf("Old arch error: %q", oldResp.FailedEvents[0].Error)
				require.NotEmpty(t, oldResp.FailedEvents[0].Error, "old arch: should have an error")

				newResp := env.NewClient.Transform(context.Background(), events)
				t.Logf("New arch: Events=%d, FailedEvents=%d", len(newResp.Events), len(newResp.FailedEvents))
				require.Equal(t, 0, len(newResp.Events), "new arch: no success events expected")
				require.Equal(t, 1, len(newResp.FailedEvents), "new arch: 1 failed event expected")
				t.Logf("New arch error: %q", newResp.FailedEvents[0].Error)
				require.NotEmpty(t, newResp.FailedEvents[0].Error, "new arch: should have an error")
			},
		},
		{
			name:      "GeoStatus500BatchUncaught",
			versionID: "bc-geo-status-500-batch-uncaught-v1",
			config: configBackendEntry{code: `
def transformBatch(events, metadata):
    for event in events:
        geo = geolocation("1.2.3.4")
        event["geo"] = geo
    return events
`},
			setup:               func() { mockGeoCfg.setResponse(500) },
			skipRetryCountMatch: true,
			run: func(t *testing.T, env *bcTestEnv) {
				const versionID = "bc-geo-status-500-batch-uncaught-v1"

				events := []types.TransformerEvent{
					makeEvent("msg-1", versionID),
					makeEvent("msg-2", versionID),
				}

				oldResp := env.OldClient.Transform(context.Background(), events)
				t.Logf("Old arch: Events=%d, FailedEvents=%d", len(oldResp.Events), len(oldResp.FailedEvents))
				require.Equal(t, 0, len(oldResp.Events), "old arch: no success events expected")
				require.Equal(t, 2, len(oldResp.FailedEvents), "old arch: 2 failed events expected")
				for i := range oldResp.FailedEvents {
					require.Containsf(t, oldResp.FailedEvents[i].Error, "status code: 500", "old arch: event %d error should mention 500", i)
				}

				newResp := env.NewClient.Transform(context.Background(), events)
				t.Logf("New arch: Events=%d, FailedEvents=%d", len(newResp.Events), len(newResp.FailedEvents))
				require.Equal(t, 0, len(newResp.Events), "new arch: no success events expected")
				require.Equal(t, 2, len(newResp.FailedEvents), "new arch: 2 failed events expected")
			},
		},
		{
			name:      "GeoConnectionResetBatchUncaught",
			versionID: "bc-geo-conn-reset-batch-uncaught-v1",
			config: configBackendEntry{code: `
def transformBatch(events, metadata):
    for event in events:
        geo = geolocation("1.2.3.4")
        event["geo"] = geo
    return events
`},
			setup:               func() { mockGeoCfg.setConnectionClose() },
			skipRetryCountMatch: true,
			run: func(t *testing.T, env *bcTestEnv) {
				const versionID = "bc-geo-conn-reset-batch-uncaught-v1"

				events := []types.TransformerEvent{
					makeEvent("msg-1", versionID),
					makeEvent("msg-2", versionID),
				}

				oldResp := env.OldClient.Transform(context.Background(), events)
				t.Logf("Old arch: Events=%d, FailedEvents=%d", len(oldResp.Events), len(oldResp.FailedEvents))
				require.Equal(t, 0, len(oldResp.Events), "old arch: no success events expected")
				require.Equal(t, 2, len(oldResp.FailedEvents), "old arch: 2 failed events expected")
				for i := range oldResp.FailedEvents {
					require.NotEmptyf(t, oldResp.FailedEvents[i].Error, "old arch: event %d should have an error", i)
				}

				newResp := env.NewClient.Transform(context.Background(), events)
				t.Logf("New arch: Events=%d, FailedEvents=%d", len(newResp.Events), len(newResp.FailedEvents))
				require.Equal(t, 0, len(newResp.Events), "new arch: no success events expected")
				require.Equal(t, 2, len(newResp.FailedEvents), "new arch: 2 failed events expected")
				for i := range newResp.FailedEvents {
					require.NotEmptyf(t, newResp.FailedEvents[i].Error, "new arch: event %d should have an error", i)
				}
			},
		},
		{
			// Locks the contract that a slow/hung geolocation backend is
			// distinguished from a slow user HTTP call: it must propagate as
			// a retryable HTTP 503 (GeolocationServerError → retry) rather
			// than a per-event 400. The mock service blocks for longer than
			// GEOLOCATION_TIMEOUT_SECS, so the geolocation session
			// deadline fires and raises GeolocationServerError (BaseException),
			// which bypasses the user-code except-Exception and surfaces as retryable.
			// SANDBOX_HTTP_TIMEOUT_S does NOT apply to internal geolocation traffic — see
			// TestSandboxHTTPTimeoutDoesNotCapGeolocation.
			name:      "GeoTimeout",
			versionID: "bc-geo-timeout-v1",
			config: configBackendEntry{code: `
def transformEvent(event, metadata):
    try:
        result = geolocation("1.2.3.4")
        event["geo"] = result
    except Exception as e:
        # New arch must NOT reach this branch — GeolocationServerError
        # inherits BaseException so it bypasses except-Exception and
        # propagates to the worker as a retryable 503.
        event["geo_error"] = str(e)
    return event
`},
			setup:               func() { mockGeoCfg.setSlow(500 * time.Millisecond) },
			skipRetryCountMatch: true,
			run: func(t *testing.T, env *bcTestEnv) {
				const versionID = "bc-geo-timeout-v1"

				events := []types.TransformerEvent{
					makeEvent("msg-1", versionID),
				}

				// Old arch: openfaas-flask-base catches the timeout via
				// `except Exception` so the user code records geo_error
				// and the event succeeds.

				// New arch: timeout → GeolocationServerError →
				// HTTP 503 + X-Rudder-Should-Retry → retries exhausted →
				// failed event (with the user's `event["geo_error"]`
				// branch never executed).
				newResp := env.NewClient.Transform(context.Background(), events)
				t.Logf("New arch: Events=%d, FailedEvents=%d", len(newResp.Events), len(newResp.FailedEvents))
				require.Len(t, newResp.Events, 0,
					"new arch: slow geolocation must NOT produce a success event "+
						"(GeolocationServerError must bypass user except-Exception)")
				require.Len(t, newResp.FailedEvents, 1,
					"new arch: slow geolocation must surface as a failed event after retries")
				require.Contains(t, newResp.FailedEvents[0].Error,
					"transformer returned status code: 503",
					"new arch: failed event must carry the correct error message")

				retriesCounter := env.NewStats.GetByName("processor_user_transformer_http_retries")
				require.Len(t, retriesCounter, 1)
				require.EqualValues(t, 2, retriesCounter[0].Value)
			},
		},
	}

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	// Collect all config backend entries.
	allEntries := make(map[string]configBackendEntry, len(subtests))
	for _, st := range subtests {
		if st.config != (configBackendEntry{}) {
			allEntries[st.versionID] = st.config
		}
	}
	configBackend := newContractConfigBackend(t, allEntries)
	t.Cleanup(configBackend.Close)

	// Mock OpenFaaS gateway with dynamic target.
	var (
		gatewayMu        sync.Mutex
		gatewayTargetURL string
	)
	getGatewayTarget := func() string {
		gatewayMu.Lock()
		defer gatewayMu.Unlock()
		return gatewayTargetURL
	}
	setGatewayTarget := func(url string) {
		gatewayMu.Lock()
		defer gatewayMu.Unlock()
		gatewayTargetURL = url
	}
	mockGateway, _ := newMockOpenFaaSGateway(t, getGatewayTarget)
	t.Cleanup(mockGateway.Close)

	var (
		wg                               sync.WaitGroup
		transformerURL, pyTransformerURL string
	)
	wg.Go(func() {
		transformerURL = startRudderTransformer(t, pool, configBackend.URL, mockGateway.URL)
	})
	wg.Go(func() {
		// Start shared rudder-pytransformer with configurable mock geolocation URL.
		// GEOLOCATION_TIMEOUT_SECS=0.5 governs the GeoTimeout subtest's 1s mock
		// delay (500 ms < 1 s → retryable 503). SANDBOX_HTTP_TIMEOUT_S is kept
		// low as a guard for any future subtest exercising user HTTP traffic; it
		// does NOT affect geolocation calls — see
		// TestSandboxHTTPTimeoutDoesNotCapGeolocation.
		pyTransformerURL = startRudderPytransformer(t, pool, configBackend.URL,
			"GEOLOCATION_URL="+geoURL,
			"SANDBOX_HTTP_TIMEOUT_S=0.1",
			"GEOLOCATION_TIMEOUT_SECS=0.1",
		)
	})
	wg.Wait()

	// Run subtests sequentially.
	for _, st := range subtests {
		t.Run(st.name, func(t *testing.T) {
			env := newBCTestEnv(t, transformerURL, pyTransformerURL,
				withFailOnError(),
				withLimitedRetryableHTTPRetries(),
			)

			if st.config.code != "" {
				t.Logf("Starting openfaas-flask-base for %s (versionID=%s)...", st.name, st.versionID)
				openFaasURL := startOpenFaasFlask(t, pool, st.versionID, configBackend.URL, "geolocation_url="+geoURL)

				setGatewayTarget(openFaasURL)
			}

			if st.setup != nil {
				st.setup()
			}

			st.run(t, env)
			if !st.skipRetryCountMatch {
				env.assertRetryCountsMatch(t)
			}
		})
	}
}
