package pytransformer_contract

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"
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

				// Verify geo data was returned (both should have a non-nil geo field)
				require.NotNil(t, oldResp.Events[0].Output["geo"], "old arch: geo should be non-nil")
				require.NotNil(t, newResp.Events[0].Output["geo"], "new arch: geo should be non-nil")

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

				// Both should have two different geo results
				require.NotNil(t, oldResp.Events[0].Output["geo1"], "old arch: geo1 should be non-nil")
				require.NotNil(t, oldResp.Events[0].Output["geo2"], "old arch: geo2 should be non-nil")
				require.NotNil(t, newResp.Events[0].Output["geo1"], "new arch: geo1 should be non-nil")
				require.NotNil(t, newResp.Events[0].Output["geo2"], "new arch: geo2 should be non-nil")

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

				// Verify geo data was placed in context.geo
				oldCtx, _ := oldResp.Events[0].Output["context"].(map[string]any)
				newCtx, _ := newResp.Events[0].Output["context"].(map[string]any)
				require.NotNil(t, oldCtx["geo"], "old arch: context.geo should be non-nil")
				require.NotNil(t, newCtx["geo"], "new arch: context.geo should be non-nil")

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

				// All events should have geo data
				for i := range oldResp.Events {
					require.NotNil(t, oldResp.Events[i].Output["geo"], "old arch: event %d geo should be non-nil", i)
					require.NotNil(t, newResp.Events[i].Output["geo"], "new arch: event %d geo should be non-nil", i)
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

				require.NotNil(t, oldResp.Events[0].Output["geo_lookup"], "old arch: geo_lookup should be non-nil")
				require.NotNil(t, newResp.Events[0].Output["geo_lookup"], "new arch: geo_lookup should be non-nil")

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
				for i := range oldResp.Events {
					require.NotNil(t, oldResp.Events[i].Output["geo"], "old arch: event %d geo should be non-nil", i)
					require.NotNil(t, newResp.Events[i].Output["geo"], "new arch: event %d geo should be non-nil", i)
				}

				diff, equal := oldResp.Equal(&newResp)
				if equal {
					t.Log("Both architectures produce identical responses for batch geolocation with different IPs")
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
	waitForGeolocation(t, pool, geoContainer, geoURL)

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

	// Start shared rudder-transformer.
	transformerPort, err := kithelper.GetFreePort()
	require.NoError(t, err)
	transformerURL := fmt.Sprintf("http://localhost:%d", transformerPort)
	transformerContainer := startRudderTransformer(t, pool, transformerPort, configBackend.URL, mockGateway.URL)
	t.Cleanup(func() {
		if err := pool.Purge(transformerContainer); err != nil {
			t.Logf("Failed to purge rudder-transformer: %v", err)
		}
	})

	// Start shared rudder-pytransformer with geolocation URL.
	pyTransformerPort, err := kithelper.GetFreePort()
	require.NoError(t, err)
	pyTransformerURL := fmt.Sprintf("http://localhost:%d", pyTransformerPort)
	pyTransformerContainer := startRudderPytransformer(t, pool, pyTransformerPort, configBackend.URL, "GEOLOCATION_URL="+geoURL)
	t.Cleanup(func() {
		if err := pool.Purge(pyTransformerContainer); err != nil {
			t.Logf("Failed to purge rudder-pytransformer: %v", err)
		}
	})

	// Wait for shared services to be healthy.
	t.Log("Waiting for shared services to be healthy...")
	waitForHealthy(t, pool, transformerURL, "rudder-transformer")
	waitForHealthy(t, pool, pyTransformerURL, "rudder-pytransformer")

	// Run subtests sequentially.
	for _, st := range subtests {
		t.Run(st.name, func(t *testing.T) {
			env := newBCTestEnv(t, transformerURL, pyTransformerURL)

			if st.config.code != "" {
				openFaasPort, err := kithelper.GetFreePort()
				require.NoError(t, err)
				openFaasURL := fmt.Sprintf("http://localhost:%d", openFaasPort)

				t.Logf("Starting openfaas-flask-base for %s (versionID=%s)...", st.name, st.versionID)
				container := startOpenFaasFlask(t, pool, openFaasPort, st.versionID, configBackend.URL, "geolocation_url="+geoURL)
				t.Cleanup(func() {
					if err := pool.Purge(container); err != nil {
						t.Logf("Failed to purge openfaas-flask-base: %v", err)
					}
				})
				waitForOpenFaasFlask(t, pool, openFaasURL)

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
// func TestBackwardsCompatibilityGeolocationNotConfigured(t *testing.T) {
// 	type subtest struct {
// 		name      string
// 		versionID string
// 		config    configBackendEntry
// 		run       func(t *testing.T, env *bcTestEnv)
// 	}

// 	subtests := []subtest{
// 		{
// 			name:      "GeolocationNotConfigured",
// 			versionID: "bc-geo-not-configured-v1",
// 			config: configBackendEntry{code: `
// def transformEvent(event, metadata):
//     try:
//         result = geolocation("1.2.3.4")
//         event["geo"] = result
//     except Exception as e:
//         event["geo_error"] = str(e)
//     return event
// `},
// 			run: func(t *testing.T, env *bcTestEnv) {
// 				const versionID = "bc-geo-not-configured-v1"

// 				events := []types.TransformerEvent{
// 					makeEvent("msg-1", versionID),
// 				}

// 				oldResp := env.OldClient.Transform(context.Background(), events)
// 				t.Logf("Old arch: Events=%d, FailedEvents=%d", len(oldResp.Events), len(oldResp.FailedEvents))
// 				newResp := env.NewClient.Transform(context.Background(), events)
// 				t.Logf("New arch: Events=%d, FailedEvents=%d", len(newResp.Events), len(newResp.FailedEvents))

// 				// Both should succeed (error caught by try/except)
// 				require.Equal(t, 1, len(oldResp.Events), "old arch: 1 success event expected")
// 				require.Equal(t, 1, len(newResp.Events), "new arch: 1 success event expected")

// 				// Both should report "not supported" error
// 				oldError, _ := oldResp.Events[0].Output["geo_error"].(string)
// 				newError, _ := newResp.Events[0].Output["geo_error"].(string)
// 				t.Logf("Old arch geo_error: %q", oldError)
// 				t.Logf("New arch geo_error: %q", newError)

// 				require.Contains(t, oldError, "not supported", "old arch: error should mention not supported")
// 				require.Contains(t, newError, "not supported", "new arch: error should mention not supported")

// 				diff, equal := oldResp.Equal(&newResp)
// 				if equal {
// 					t.Log("Both architectures produce identical responses when geolocation is not configured")
// 				} else {
// 					t.Errorf("Responses differ:\n%s", diff)
// 				}
// 			},
// 		},
// 		{
// 			name:      "GeolocationNotConfiguredUncaught",
// 			versionID: "bc-geo-not-configured-uncaught-v1",
// 			config: configBackendEntry{code: `
// def transformEvent(event, metadata):
//     # Call geolocation without try/catch — should produce a failed event
//     result = geolocation("1.2.3.4")
//     event["geo"] = result
//     return event
// `},
// 			run: func(t *testing.T, env *bcTestEnv) {
// 				const versionID = "bc-geo-not-configured-uncaught-v1"

// 				events := []types.TransformerEvent{
// 					makeEvent("msg-1", versionID),
// 				}

// 				oldResp := env.OldClient.Transform(context.Background(), events)
// 				t.Logf("Old arch: Events=%d, FailedEvents=%d", len(oldResp.Events), len(oldResp.FailedEvents))
// 				newResp := env.NewClient.Transform(context.Background(), events)
// 				t.Logf("New arch: Events=%d, FailedEvents=%d", len(newResp.Events), len(newResp.FailedEvents))

// 				// Both should fail since exception propagates
// 				require.Equal(t, 0, len(oldResp.Events), "old arch: no success events expected")
// 				require.Equal(t, 1, len(oldResp.FailedEvents), "old arch: 1 failed event expected")
// 				require.Equal(t, 0, len(newResp.Events), "new arch: no success events expected")
// 				require.Equal(t, 1, len(newResp.FailedEvents), "new arch: 1 failed event expected")

// 				oldError := oldResp.FailedEvents[0].Error
// 				newError := newResp.FailedEvents[0].Error
// 				t.Logf("Old arch error: %q", oldError)
// 				t.Logf("New arch error: %q", newError)

// 				require.Contains(t, oldError, "not supported", "old arch: error should mention not supported")
// 				require.Contains(t, newError, "not supported", "new arch: error should mention not supported")

// 				diff, equal := oldResp.Equal(&newResp)
// 				if equal {
// 					t.Log("Both architectures produce identical error for uncaught geolocation not configured")
// 				} else {
// 					t.Errorf("Responses differ:\n%s", diff)
// 				}
// 			},
// 		},
// 		{
// 			name:      "GeolocationNotConfiguredBatch",
// 			versionID: "bc-geo-not-configured-batch-v1",
// 			config: configBackendEntry{code: `
// def transformBatch(events, metadata):
//     for event in events:
//         try:
//             geo = geolocation("1.2.3.4")
//             event["geo"] = geo
//         except Exception as e:
//             event["geo_error"] = str(e)
//     return events
// `},
// 			run: func(t *testing.T, env *bcTestEnv) {
// 				const versionID = "bc-geo-not-configured-batch-v1"

// 				events := []types.TransformerEvent{
// 					makeEvent("msg-1", versionID),
// 					makeEvent("msg-2", versionID),
// 				}

// 				oldResp := env.OldClient.Transform(context.Background(), events)
// 				t.Logf("Old arch: Events=%d, FailedEvents=%d", len(oldResp.Events), len(oldResp.FailedEvents))
// 				newResp := env.NewClient.Transform(context.Background(), events)
// 				t.Logf("New arch: Events=%d, FailedEvents=%d", len(newResp.Events), len(newResp.FailedEvents))

// 				require.Equal(t, 2, len(oldResp.Events), "old arch: 2 success events expected")
// 				require.Equal(t, 2, len(newResp.Events), "new arch: 2 success events expected")

// 				// Both should have geo_error for all events
// 				for i := range oldResp.Events {
// 					oldError, _ := oldResp.Events[i].Output["geo_error"].(string)
// 					newError, _ := newResp.Events[i].Output["geo_error"].(string)
// 					require.Contains(t, oldError, "not supported", "old arch: event %d error should mention not supported", i)
// 					require.Contains(t, newError, "not supported", "new arch: event %d error should mention not supported", i)
// 				}

// 				diff, equal := oldResp.Equal(&newResp)
// 				if equal {
// 					t.Log("Both architectures produce identical responses for batch geolocation not configured")
// 				} else {
// 					t.Errorf("Responses differ:\n%s", diff)
// 				}
// 			},
// 		},
// 	}

// 	pool, err := dockertest.NewPool("")
// 	require.NoError(t, err)

// 	// Collect all config backend entries.
// 	allEntries := make(map[string]configBackendEntry, len(subtests))
// 	for _, st := range subtests {
// 		if st.config != (configBackendEntry{}) {
// 			allEntries[st.versionID] = st.config
// 		}
// 	}
// 	configBackend := newContractConfigBackend(t, allEntries)
// 	t.Cleanup(configBackend.Close)

// 	// Mock OpenFaaS gateway with dynamic target.
// 	var (
// 		gatewayMu        sync.Mutex
// 		gatewayTargetURL string
// 	)
// 	getGatewayTarget := func() string {
// 		gatewayMu.Lock()
// 		defer gatewayMu.Unlock()
// 		return gatewayTargetURL
// 	}
// 	setGatewayTarget := func(url string) {
// 		gatewayMu.Lock()
// 		defer gatewayMu.Unlock()
// 		gatewayTargetURL = url
// 	}
// 	mockGateway, _ := newMockOpenFaaSGateway(t, getGatewayTarget)
// 	t.Cleanup(mockGateway.Close)

// 	// Start shared rudder-transformer (WITHOUT geolocation URL).
// 	transformerPort, err := kithelper.GetFreePort()
// 	require.NoError(t, err)
// 	transformerURL := fmt.Sprintf("http://localhost:%d", transformerPort)
// 	transformerContainer := startRudderTransformer(t, pool, transformerPort, configBackend.URL, mockGateway.URL)
// 	t.Cleanup(func() {
// 		if err := pool.Purge(transformerContainer); err != nil {
// 			t.Logf("Failed to purge rudder-transformer: %v", err)
// 		}
// 	})

// 	// Start shared rudder-pytransformer (WITHOUT geolocation URL).
// 	pyTransformerPort, err := kithelper.GetFreePort()
// 	require.NoError(t, err)
// 	pyTransformerURL := fmt.Sprintf("http://localhost:%d", pyTransformerPort)
// 	pyTransformerContainer := startRudderPytransformer(t, pool, pyTransformerPort, configBackend.URL)
// 	t.Cleanup(func() {
// 		if err := pool.Purge(pyTransformerContainer); err != nil {
// 			t.Logf("Failed to purge rudder-pytransformer: %v", err)
// 		}
// 	})

// 	t.Log("Waiting for shared services to be healthy...")
// 	waitForHealthy(t, pool, transformerURL, "rudder-transformer")
// 	waitForHealthy(t, pool, pyTransformerURL, "rudder-pytransformer")

// 	for _, st := range subtests {
// 		t.Run(st.name, func(t *testing.T) {
// 			env := newBCTestEnv(t, transformerURL, pyTransformerURL)

// 			if st.config.code != "" {
// 				openFaasPort, err := kithelper.GetFreePort()
// 				require.NoError(t, err)
// 				openFaasURL := fmt.Sprintf("http://localhost:%d", openFaasPort)

// 				// Start openfaas WITHOUT geolocation URL (not configured test).
// 				container := startOpenFaasFlask(t, pool, openFaasPort, st.versionID, configBackend.URL)
// 				t.Cleanup(func() {
// 					if err := pool.Purge(container); err != nil {
// 						t.Logf("Failed to purge openfaas-flask-base: %v", err)
// 					}
// 				})
// 				waitForOpenFaasFlask(t, pool, openFaasURL)

// 				setGatewayTarget(openFaasURL)
// 			}

// 			st.run(t, env)
// 			env.assertRetryCountsMatch(t)
// 		})
// 	}
// }

// TestBackwardsCompatibilityGeolocationFailures tests behavior when the
// geolocation service returns error responses (non-200 status codes).
// Both architectures raise: "geolocation fetch failed with status code: {code}"
// func TestBackwardsCompatibilityGeolocationFailures(t *testing.T) {
// 	type subtest struct {
// 		name      string
// 		versionID string
// 		config    configBackendEntry
// 		run       func(t *testing.T, env *bcTestEnv)
// 	}

// 	subtests := []subtest{
// 		{
// 			name:      "GeolocationInvalidIP",
// 			versionID: "bc-geo-fail-invalid-v1",
// 			config: configBackendEntry{code: `
// def transformEvent(event, metadata):
//     try:
//         result = geolocation("not-an-ip")
//         event["geo"] = result
//     except Exception as e:
//         event["geo_error"] = str(e)
//     return event
// `},
// 			run: func(t *testing.T, env *bcTestEnv) {
// 				const versionID = "bc-geo-fail-invalid-v1"

// 				events := []types.TransformerEvent{
// 					makeEvent("msg-1", versionID),
// 				}

// 				oldResp := env.OldClient.Transform(context.Background(), events)
// 				t.Logf("Old arch: Events=%d, FailedEvents=%d", len(oldResp.Events), len(oldResp.FailedEvents))
// 				newResp := env.NewClient.Transform(context.Background(), events)
// 				t.Logf("New arch: Events=%d, FailedEvents=%d", len(newResp.Events), len(newResp.FailedEvents))

// 				require.Equal(t, 1, len(oldResp.Events), "old arch: 1 success event expected")
// 				require.Equal(t, 1, len(newResp.Events), "new arch: 1 success event expected")

// 				oldError, _ := oldResp.Events[0].Output["geo_error"].(string)
// 				newError, _ := newResp.Events[0].Output["geo_error"].(string)
// 				t.Logf("Old arch geo_error: %q", oldError)
// 				t.Logf("New arch geo_error: %q", newError)

// 				// Real geolocation service returns 400 for invalid IPs
// 				require.Contains(t, oldError, "status code: 400", "old arch: error should mention 400")
// 				require.Contains(t, newError, "status code: 400", "new arch: error should mention 400")

// 				diff, equal := oldResp.Equal(&newResp)
// 				if equal {
// 					t.Log("Both architectures produce identical responses for invalid IP")
// 				} else {
// 					t.Errorf("Responses differ:\n%s", diff)
// 				}
// 			},
// 		},
// 		{
// 			name:      "GeolocationInvalidIPSpecialChars",
// 			versionID: "bc-geo-fail-special-v1",
// 			config: configBackendEntry{code: `
// def transformEvent(event, metadata):
//     try:
//         result = geolocation("hello world")
//         event["geo"] = result
//     except Exception as e:
//         event["geo_error"] = str(e)
//     return event
// `},
// 			run: func(t *testing.T, env *bcTestEnv) {
// 				const versionID = "bc-geo-fail-special-v1"

// 				events := []types.TransformerEvent{
// 					makeEvent("msg-1", versionID),
// 				}

// 				oldResp := env.OldClient.Transform(context.Background(), events)
// 				t.Logf("Old arch: Events=%d, FailedEvents=%d", len(oldResp.Events), len(oldResp.FailedEvents))
// 				newResp := env.NewClient.Transform(context.Background(), events)
// 				t.Logf("New arch: Events=%d, FailedEvents=%d", len(newResp.Events), len(newResp.FailedEvents))

// 				require.Equal(t, 1, len(oldResp.Events), "old arch: 1 success event expected")
// 				require.Equal(t, 1, len(newResp.Events), "new arch: 1 success event expected")

// 				oldError, _ := oldResp.Events[0].Output["geo_error"].(string)
// 				newError, _ := newResp.Events[0].Output["geo_error"].(string)
// 				t.Logf("Old arch geo_error: %q", oldError)
// 				t.Logf("New arch geo_error: %q", newError)

// 				require.Contains(t, oldError, "status code: 400", "old arch: error should mention 400")
// 				require.Contains(t, newError, "status code: 400", "new arch: error should mention 400")

// 				diff, equal := oldResp.Equal(&newResp)
// 				if equal {
// 					t.Log("Both architectures produce identical responses for special chars IP")
// 				} else {
// 					t.Errorf("Responses differ:\n%s", diff)
// 				}
// 			},
// 		},
// 		{
// 			name:      "GeolocationInvalidIPUncaught",
// 			versionID: "bc-geo-fail-uncaught-v1",
// 			config: configBackendEntry{code: `
// def transformEvent(event, metadata):
//     # Call geolocation with invalid IP without try/catch
//     result = geolocation("not-an-ip")
//     event["geo"] = result
//     return event
// `},
// 			run: func(t *testing.T, env *bcTestEnv) {
// 				const versionID = "bc-geo-fail-uncaught-v1"

// 				events := []types.TransformerEvent{
// 					makeEvent("msg-1", versionID),
// 				}

// 				oldResp := env.OldClient.Transform(context.Background(), events)
// 				t.Logf("Old arch: Events=%d, FailedEvents=%d", len(oldResp.Events), len(oldResp.FailedEvents))
// 				newResp := env.NewClient.Transform(context.Background(), events)
// 				t.Logf("New arch: Events=%d, FailedEvents=%d", len(newResp.Events), len(newResp.FailedEvents))

// 				// Both should produce a failed event since the exception propagates
// 				require.Equal(t, 0, len(oldResp.Events), "old arch: no success events expected")
// 				require.Equal(t, 1, len(oldResp.FailedEvents), "old arch: 1 failed event expected")
// 				require.Equal(t, 0, len(newResp.Events), "new arch: no success events expected")
// 				require.Equal(t, 1, len(newResp.FailedEvents), "new arch: 1 failed event expected")

// 				oldError := oldResp.FailedEvents[0].Error
// 				newError := newResp.FailedEvents[0].Error
// 				t.Logf("Old arch error: %q", oldError)
// 				t.Logf("New arch error: %q", newError)

// 				require.Contains(t, oldError, "status code: 400", "old arch: error should mention 400")
// 				require.Contains(t, newError, "status code: 400", "new arch: error should mention 400")

// 				diff, equal := oldResp.Equal(&newResp)
// 				if equal {
// 					t.Log("Both architectures produce identical error for uncaught invalid IP")
// 				} else {
// 					t.Errorf("Responses differ:\n%s", diff)
// 				}
// 			},
// 		},
// 		{
// 			name:      "GeolocationInvalidIPBatch",
// 			versionID: "bc-geo-fail-batch-v1",
// 			config: configBackendEntry{code: `
// def transformBatch(events, metadata):
//     for event in events:
//         try:
//             geo = geolocation("not-an-ip")
//             event["geo"] = geo
//         except Exception as e:
//             event["geo_error"] = str(e)
//     return events
// `},
// 			run: func(t *testing.T, env *bcTestEnv) {
// 				const versionID = "bc-geo-fail-batch-v1"

// 				events := []types.TransformerEvent{
// 					makeEvent("msg-1", versionID),
// 					makeEvent("msg-2", versionID),
// 					makeEvent("msg-3", versionID),
// 				}

// 				oldResp := env.OldClient.Transform(context.Background(), events)
// 				t.Logf("Old arch: Events=%d, FailedEvents=%d", len(oldResp.Events), len(oldResp.FailedEvents))
// 				newResp := env.NewClient.Transform(context.Background(), events)
// 				t.Logf("New arch: Events=%d, FailedEvents=%d", len(newResp.Events), len(newResp.FailedEvents))

// 				require.Equal(t, 3, len(oldResp.Events), "old arch: 3 success events expected")
// 				require.Equal(t, 3, len(newResp.Events), "new arch: 3 success events expected")

// 				for i := range oldResp.Events {
// 					oldError, _ := oldResp.Events[i].Output["geo_error"].(string)
// 					newError, _ := newResp.Events[i].Output["geo_error"].(string)
// 					require.Contains(t, oldError, "status code: 400", "old arch: event %d error should mention 400", i)
// 					require.Contains(t, newError, "status code: 400", "new arch: event %d error should mention 400", i)
// 				}

// 				diff, equal := oldResp.Equal(&newResp)
// 				if equal {
// 					t.Log("Both architectures produce identical responses for batch invalid IP")
// 				} else {
// 					t.Errorf("Responses differ:\n%s", diff)
// 				}
// 			},
// 		},
// 		{
// 			name:      "GeolocationPartialInvalidIP",
// 			versionID: "bc-geo-fail-partial-v1",
// 			config: configBackendEntry{code: `
// def transformEvent(event, metadata):
//     msg_id = event.get("messageId", "")
//     if msg_id == "msg-2":
//         # This event uses an invalid IP
//         ip = "not-an-ip"
//     else:
//         # These events use a valid IP
//         ip = "1.2.3.4"
//     try:
//         geo = geolocation(ip)
//         event["geo"] = geo
//     except Exception as e:
//         event["geo_error"] = str(e)
//     return event
// `},
// 			run: func(t *testing.T, env *bcTestEnv) {
// 				const versionID = "bc-geo-fail-partial-v1"

// 				events := []types.TransformerEvent{
// 					makeEvent("msg-1", versionID),
// 					makeEvent("msg-2", versionID),
// 					makeEvent("msg-3", versionID),
// 				}

// 				oldResp := env.OldClient.Transform(context.Background(), events)
// 				t.Logf("Old arch: Events=%d, FailedEvents=%d", len(oldResp.Events), len(oldResp.FailedEvents))
// 				newResp := env.NewClient.Transform(context.Background(), events)
// 				t.Logf("New arch: Events=%d, FailedEvents=%d", len(newResp.Events), len(newResp.FailedEvents))

// 				// All 3 events succeed (errors are caught)
// 				require.Equal(t, 3, len(oldResp.Events), "old arch: 3 success events expected")
// 				require.Equal(t, 3, len(newResp.Events), "new arch: 3 success events expected")

// 				// msg-1 and msg-3 should have geo data, msg-2 should have geo_error
// 				for _, resp := range []*types.Response{&oldResp, &newResp} {
// 					for _, ev := range resp.Events {
// 						msgID, _ := ev.Output["messageId"].(string)
// 						if msgID == "msg-2" {
// 							geoErr, _ := ev.Output["geo_error"].(string)
// 							require.Contains(t, geoErr, "status code: 400",
// 								"event %s should have 400 error", msgID)
// 							require.Nil(t, ev.Output["geo"],
// 								"event %s should not have geo data", msgID)
// 						} else {
// 							require.NotNil(t, ev.Output["geo"],
// 								"event %s should have geo data", msgID)
// 							require.Nil(t, ev.Output["geo_error"],
// 								"event %s should not have geo_error", msgID)
// 						}
// 					}
// 				}

// 				diff, equal := oldResp.Equal(&newResp)
// 				if equal {
// 					t.Log("Both architectures produce identical responses for partial invalid IP")
// 				} else {
// 					t.Errorf("Responses differ:\n%s", diff)
// 				}
// 			},
// 		},
// 		{
// 			name:      "GeolocationPrivateIP",
// 			versionID: "bc-geo-fail-private-v1",
// 			config: configBackendEntry{code: `
// def transformEvent(event, metadata):
//     result = geolocation("127.0.0.1")
//     event["geo"] = result
//     return event
// `},
// 			run: func(t *testing.T, env *bcTestEnv) {
// 				const versionID = "bc-geo-fail-private-v1"

// 				events := []types.TransformerEvent{
// 					makeEvent("msg-1", versionID),
// 				}

// 				oldResp := env.OldClient.Transform(context.Background(), events)
// 				t.Logf("Old arch: Events=%d, FailedEvents=%d", len(oldResp.Events), len(oldResp.FailedEvents))
// 				newResp := env.NewClient.Transform(context.Background(), events)
// 				t.Logf("New arch: Events=%d, FailedEvents=%d", len(newResp.Events), len(newResp.FailedEvents))

// 				// Private IP returns 200 with empty string fields — no error
// 				require.Equal(t, 1, len(oldResp.Events), "old arch: 1 success event expected")
// 				require.Equal(t, 0, len(oldResp.FailedEvents), "old arch: no failed events expected")
// 				require.Equal(t, 1, len(newResp.Events), "new arch: 1 success event expected")
// 				require.Equal(t, 0, len(newResp.FailedEvents), "new arch: no failed events expected")

// 				// Geo data should be present but with empty values
// 				require.NotNil(t, oldResp.Events[0].Output["geo"], "old arch: geo should be non-nil")
// 				require.NotNil(t, newResp.Events[0].Output["geo"], "new arch: geo should be non-nil")

// 				t.Logf("Old arch geo: %v", oldResp.Events[0].Output["geo"])
// 				t.Logf("New arch geo: %v", newResp.Events[0].Output["geo"])

// 				diff, equal := oldResp.Equal(&newResp)
// 				if equal {
// 					t.Log("Both architectures produce identical responses for private IP")
// 				} else {
// 					t.Errorf("Responses differ:\n%s", diff)
// 				}
// 			},
// 		},
// 	}

// 	pool, err := dockertest.NewPool("")
// 	require.NoError(t, err)

// 	// Start a mock geolocation service that behaves like the real
// 	// rudder-geolocation: 400 for invalid IPs, 200 with empty fields for
// 	// private IPs, 200 with data for known public IPs.
// 	mockGeoService := newMockGeolocationService(t)
// 	t.Cleanup(mockGeoService.Close)
// 	geoURL := mockGeoService.URL

// 	// Collect all config backend entries.
// 	allEntries := make(map[string]configBackendEntry, len(subtests))
// 	for _, st := range subtests {
// 		if st.config != (configBackendEntry{}) {
// 			allEntries[st.versionID] = st.config
// 		}
// 	}
// 	configBackend := newContractConfigBackend(t, allEntries)
// 	t.Cleanup(configBackend.Close)

// 	// Mock OpenFaaS gateway with dynamic target.
// 	var (
// 		gatewayMu        sync.Mutex
// 		gatewayTargetURL string
// 	)
// 	getGatewayTarget := func() string {
// 		gatewayMu.Lock()
// 		defer gatewayMu.Unlock()
// 		return gatewayTargetURL
// 	}
// 	setGatewayTarget := func(url string) {
// 		gatewayMu.Lock()
// 		defer gatewayMu.Unlock()
// 		gatewayTargetURL = url
// 	}
// 	mockGateway, _ := newMockOpenFaaSGateway(t, getGatewayTarget)
// 	t.Cleanup(mockGateway.Close)

// 	// Start shared rudder-transformer.
// 	transformerPort, err := kithelper.GetFreePort()
// 	require.NoError(t, err)
// 	transformerURL := fmt.Sprintf("http://localhost:%d", transformerPort)
// 	transformerContainer := startRudderTransformer(t, pool, transformerPort, configBackend.URL, mockGateway.URL)
// 	t.Cleanup(func() {
// 		if err := pool.Purge(transformerContainer); err != nil {
// 			t.Logf("Failed to purge rudder-transformer: %v", err)
// 		}
// 	})

// 	// Start shared rudder-pytransformer with mock geolocation URL.
// 	pyTransformerPort, err := kithelper.GetFreePort()
// 	require.NoError(t, err)
// 	pyTransformerURL := fmt.Sprintf("http://localhost:%d", pyTransformerPort)
// 	pyTransformerContainer := startRudderPytransformer(t, pool, pyTransformerPort, configBackend.URL, "GEOLOCATION_URL="+geoURL)
// 	t.Cleanup(func() {
// 		if err := pool.Purge(pyTransformerContainer); err != nil {
// 			t.Logf("Failed to purge rudder-pytransformer: %v", err)
// 		}
// 	})

// 	// Wait for shared services to be healthy.
// 	t.Log("Waiting for shared services to be healthy...")
// 	waitForHealthy(t, pool, transformerURL, "rudder-transformer")
// 	waitForHealthy(t, pool, pyTransformerURL, "rudder-pytransformer")

// 	// Run subtests sequentially.
// 	for _, st := range subtests {
// 		t.Run(st.name, func(t *testing.T) {
// 			env := newBCTestEnv(t, transformerURL, pyTransformerURL)

// 			if st.config.code != "" {
// 				openFaasPort, err := kithelper.GetFreePort()
// 				require.NoError(t, err)
// 				openFaasURL := fmt.Sprintf("http://localhost:%d", openFaasPort)

// 				t.Logf("Starting openfaas-flask-base for %s (versionID=%s)...", st.name, st.versionID)
// 				container := startOpenFaasFlask(t, pool, openFaasPort, st.versionID, configBackend.URL, "geolocation_url="+geoURL)
// 				t.Cleanup(func() {
// 					if err := pool.Purge(container); err != nil {
// 						t.Logf("Failed to purge openfaas-flask-base: %v", err)
// 					}
// 				})
// 				waitForOpenFaasFlask(t, pool, openFaasURL)

// 				setGatewayTarget(openFaasURL)
// 			}

// 			st.run(t, env)
// 			env.assertRetryCountsMatch(t)
// 		})
// 	}
// }
