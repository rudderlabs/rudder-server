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
// the baseline rudder-pytransformer and the candidate one.
//
// Both implementations expose a geolocation(ip) function to user transformation
// code that calls an external geolocation service at {base_url}/geoip/{ip}.
//
// This test starts a rudder-geolocation container and configures both
// versions to use it, then exercises all edge cases.
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

				t.Log("Sending request to baseline...")
				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))

				t.Log("Sending request to candidate...")
				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))

				require.Equal(t, 1, len(baselineResp.Events), "baseline: 1 success event expected")
				require.Equal(t, 0, len(baselineResp.FailedEvents), "baseline: no failed events expected")
				require.Equal(t, 1, len(candidateResp.Events), "candidate: 1 success event expected")
				require.Equal(t, 0, len(candidateResp.FailedEvents), "candidate: no failed events expected")

				// Verify geo data was returned with correct IP
				baselineGeo, _ := baselineResp.Events[0].Output["geo"].(map[string]any)
				candidateGeo, _ := candidateResp.Events[0].Output["geo"].(map[string]any)
				require.Equal(t, "1.2.3.4", baselineGeo["ip"], "baseline: geo should contain correct ip")
				require.Equal(t, "1.2.3.4", candidateGeo["ip"], "candidate: geo should contain correct ip")

				t.Logf("Baseline geo: %v", baselineResp.Events[0].Output["geo"])
				t.Logf("Candidate geo: %v", candidateResp.Events[0].Output["geo"])

				diff, equal := baselineResp.Equal(&candidateResp)
				if equal {
					t.Log("Both versions produce identical responses for valid IP geolocation")
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

				t.Log("Sending request to baseline...")
				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))

				t.Log("Sending request to candidate...")
				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))

				// Both should succeed (error caught by try/except)
				require.Equal(t, 1, len(baselineResp.Events), "baseline: 1 success event expected")
				require.Equal(t, 1, len(candidateResp.Events), "candidate: 1 success event expected")

				// Both should have geo_error containing the validation message
				baselineError, _ := baselineResp.Events[0].Output["geo_error"].(string)
				candidateError, _ := candidateResp.Events[0].Output["geo_error"].(string)
				t.Logf("Baseline geo_error: %q", baselineError)
				t.Logf("Candidate geo_error: %q", candidateError)

				require.Contains(t, baselineError, "single string argument", "baseline: error should mention single string argument")
				require.Contains(t, candidateError, "single string argument", "candidate: error should mention single string argument")

				diff, equal := baselineResp.Equal(&candidateResp)
				if equal {
					t.Log("Both versions produce identical responses for geolocation with no args")
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

				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))
				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))

				require.Equal(t, 1, len(baselineResp.Events), "baseline: 1 success event expected")
				require.Equal(t, 1, len(candidateResp.Events), "candidate: 1 success event expected")

				baselineError, _ := baselineResp.Events[0].Output["geo_error"].(string)
				candidateError, _ := candidateResp.Events[0].Output["geo_error"].(string)
				t.Logf("Baseline geo_error: %q", baselineError)
				t.Logf("Candidate geo_error: %q", candidateError)

				require.Contains(t, baselineError, "single string argument", "baseline: error should mention single string argument")
				require.Contains(t, candidateError, "single string argument", "candidate: error should mention single string argument")

				diff, equal := baselineResp.Equal(&candidateResp)
				if equal {
					t.Log("Both versions produce identical responses for geolocation with multiple args")
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

				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))
				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))

				require.Equal(t, 1, len(baselineResp.Events), "baseline: 1 success event expected")
				require.Equal(t, 1, len(candidateResp.Events), "candidate: 1 success event expected")

				baselineError, _ := baselineResp.Events[0].Output["geo_error"].(string)
				candidateError, _ := candidateResp.Events[0].Output["geo_error"].(string)
				t.Logf("Baseline geo_error: %q", baselineError)
				t.Logf("Candidate geo_error: %q", candidateError)

				require.Contains(t, baselineError, "single string argument", "baseline: error should mention single string argument")
				require.Contains(t, candidateError, "single string argument", "candidate: error should mention single string argument")

				diff, equal := baselineResp.Equal(&candidateResp)
				if equal {
					t.Log("Both versions produce identical responses for geolocation with non-string arg")
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

				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))
				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))

				require.Equal(t, 1, len(baselineResp.Events), "baseline: 1 success event expected")
				require.Equal(t, 1, len(candidateResp.Events), "candidate: 1 success event expected")

				baselineError, _ := baselineResp.Events[0].Output["geo_error"].(string)
				candidateError, _ := candidateResp.Events[0].Output["geo_error"].(string)
				t.Logf("Baseline geo_error: %q", baselineError)
				t.Logf("Candidate geo_error: %q", candidateError)

				require.Contains(t, baselineError, "single string argument", "baseline: error should mention single string argument")
				require.Contains(t, candidateError, "single string argument", "candidate: error should mention single string argument")

				diff, equal := baselineResp.Equal(&candidateResp)
				if equal {
					t.Log("Both versions produce identical responses for geolocation with None arg")
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

				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))
				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))

				require.Equal(t, 1, len(baselineResp.Events), "baseline: 1 success event expected")
				require.Equal(t, 1, len(candidateResp.Events), "candidate: 1 success event expected")

				baselineError, _ := baselineResp.Events[0].Output["geo_error"].(string)
				candidateError, _ := candidateResp.Events[0].Output["geo_error"].(string)
				t.Logf("Baseline geo_error: %q", baselineError)
				t.Logf("Candidate geo_error: %q", candidateError)

				require.Contains(t, baselineError, "single string argument", "baseline: error should mention single string argument")
				require.Contains(t, candidateError, "single string argument", "candidate: error should mention single string argument")

				diff, equal := baselineResp.Equal(&candidateResp)
				if equal {
					t.Log("Both versions produce identical responses for geolocation with list arg")
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

				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))
				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))

				require.Equal(t, 1, len(baselineResp.Events), "baseline: 1 success event expected")
				require.Equal(t, 1, len(candidateResp.Events), "candidate: 1 success event expected")

				baselineError, _ := baselineResp.Events[0].Output["geo_error"].(string)
				candidateError, _ := candidateResp.Events[0].Output["geo_error"].(string)
				t.Logf("Baseline geo_error: %q", baselineError)
				t.Logf("Candidate geo_error: %q", candidateError)

				// In Python, isinstance(True, str) is False, so bool should be rejected
				require.Contains(t, baselineError, "single string argument", "baseline: error should mention single string argument")
				require.Contains(t, candidateError, "single string argument", "candidate: error should mention single string argument")

				diff, equal := baselineResp.Equal(&candidateResp)
				if equal {
					t.Log("Both versions produce identical responses for geolocation with bool arg")
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

				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))
				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))

				require.Equal(t, 1, len(baselineResp.Events), "baseline: 1 success event expected")
				require.Equal(t, 1, len(candidateResp.Events), "candidate: 1 success event expected")

				baselineError, _ := baselineResp.Events[0].Output["geo_error"].(string)
				candidateError, _ := candidateResp.Events[0].Output["geo_error"].(string)
				t.Logf("Baseline geo_error: %q", baselineError)
				t.Logf("Candidate geo_error: %q", candidateError)

				require.Contains(t, baselineError, "single string argument", "baseline: error should mention single string argument")
				require.Contains(t, candidateError, "single string argument", "candidate: error should mention single string argument")

				diff, equal := baselineResp.Equal(&candidateResp)
				if equal {
					t.Log("Both versions produce identical responses for geolocation with dict arg")
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

				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))
				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))

				// Empty string passes isinstance(args[0], str) check but the geolocation
				// service will likely return an error for an empty IP.
				require.Equal(t, 1, len(baselineResp.Events), "baseline: 1 success event expected")
				require.Equal(t, 1, len(candidateResp.Events), "candidate: 1 success event expected")

				t.Logf("Baseline output: geo=%v, geo_error=%v",
					baselineResp.Events[0].Output["geo"], baselineResp.Events[0].Output["geo_error"])
				t.Logf("Candidate output: geo=%v, geo_error=%v",
					candidateResp.Events[0].Output["geo"], candidateResp.Events[0].Output["geo_error"])

				diff, equal := baselineResp.Equal(&candidateResp)
				if equal {
					t.Log("Both versions produce identical responses for geolocation with empty string")
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

				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))
				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))

				require.Equal(t, 1, len(baselineResp.Events), "baseline: 1 success event expected")
				require.Equal(t, 1, len(candidateResp.Events), "candidate: 1 success event expected")

				// Both should have two different geo results with correct IPs
				baselineGeo1, _ := baselineResp.Events[0].Output["geo1"].(map[string]any)
				baselineGeo2, _ := baselineResp.Events[0].Output["geo2"].(map[string]any)
				candidateGeo1, _ := candidateResp.Events[0].Output["geo1"].(map[string]any)
				candidateGeo2, _ := candidateResp.Events[0].Output["geo2"].(map[string]any)
				require.Equal(t, "1.2.3.4", baselineGeo1["ip"], "baseline: geo1 should contain correct ip")
				require.Equal(t, "8.8.8.8", baselineGeo2["ip"], "baseline: geo2 should contain correct ip")
				require.Equal(t, "1.2.3.4", candidateGeo1["ip"], "candidate: geo1 should contain correct ip")
				require.Equal(t, "8.8.8.8", candidateGeo2["ip"], "candidate: geo2 should contain correct ip")

				t.Logf("Baseline geo1: %v", baselineResp.Events[0].Output["geo1"])
				t.Logf("Baseline geo2: %v", baselineResp.Events[0].Output["geo2"])
				t.Logf("Candidate geo1: %v", candidateResp.Events[0].Output["geo1"])
				t.Logf("Candidate geo2: %v", candidateResp.Events[0].Output["geo2"])

				diff, equal := baselineResp.Equal(&candidateResp)
				if equal {
					t.Log("Both versions produce identical responses for multiple geolocation calls")
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

				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))
				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))

				require.Equal(t, 1, len(baselineResp.Events), "baseline: 1 success event expected")
				require.Equal(t, 1, len(candidateResp.Events), "candidate: 1 success event expected")

				// Same IP should produce same result both times
				require.Equal(t, true, baselineResp.Events[0].Output["same"], "baseline: same IP should produce same result")
				require.Equal(t, true, candidateResp.Events[0].Output["same"], "candidate: same IP should produce same result")

				diff, equal := baselineResp.Equal(&candidateResp)
				if equal {
					t.Log("Both versions produce identical responses for same IP called twice")
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

				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))
				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))

				require.Equal(t, 1, len(baselineResp.Events), "baseline: 1 success event expected")
				require.Equal(t, 1, len(candidateResp.Events), "candidate: 1 success event expected")

				// Verify geo data was placed in context.geo with correct IP
				baselineCtx, _ := baselineResp.Events[0].Output["context"].(map[string]any)
				candidateCtx, _ := candidateResp.Events[0].Output["context"].(map[string]any)
				baselineGeo, _ := baselineCtx["geo"].(map[string]any)
				candidateGeo, _ := candidateCtx["geo"].(map[string]any)
				require.Equal(t, "8.8.8.8", baselineGeo["ip"], "baseline: context.geo should contain correct ip")
				require.Equal(t, "8.8.8.8", candidateGeo["ip"], "candidate: context.geo should contain correct ip")

				t.Logf("Baseline context.geo: %v", baselineCtx["geo"])
				t.Logf("Candidate context.geo: %v", candidateCtx["geo"])

				diff, equal := baselineResp.Equal(&candidateResp)
				if equal {
					t.Log("Both versions produce identical responses for geolocation enrichment")
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

				t.Log("Sending 3 events to baseline...")
				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))

				t.Log("Sending 3 events to candidate...")
				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))

				require.Equal(t, 3, len(baselineResp.Events), "baseline: 3 success events expected")
				require.Equal(t, 0, len(baselineResp.FailedEvents), "baseline: no failed events expected")
				require.Equal(t, 3, len(candidateResp.Events), "candidate: 3 success events expected")
				require.Equal(t, 0, len(candidateResp.FailedEvents), "candidate: no failed events expected")

				// All events should have geo data with correct IP
				for i := range baselineResp.Events {
					baselineGeo, _ := baselineResp.Events[i].Output["geo"].(map[string]any)
					candidateGeo, _ := candidateResp.Events[i].Output["geo"].(map[string]any)
					require.Equalf(t, "1.2.3.4", baselineGeo["ip"], "baseline: event %d geo should contain correct ip", i)
					require.Equalf(t, "1.2.3.4", candidateGeo["ip"], "candidate: event %d geo should contain correct ip", i)
				}

				diff, equal := baselineResp.Equal(&candidateResp)
				if equal {
					t.Log("Both versions produce identical responses for geolocation in batch transform")
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

				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))
				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))

				// Both should fail since the exception propagates
				require.Equal(t, 0, len(baselineResp.Events), "baseline: no success events expected")
				require.Equal(t, 1, len(baselineResp.FailedEvents), "baseline: 1 failed event expected")
				require.Equal(t, 0, len(candidateResp.Events), "candidate: no success events expected")
				require.Equal(t, 1, len(candidateResp.FailedEvents), "candidate: 1 failed event expected")

				baselineError := baselineResp.FailedEvents[0].Error
				candidateError := candidateResp.FailedEvents[0].Error
				t.Logf("Baseline error: %q", baselineError)
				t.Logf("Candidate error: %q", candidateError)

				require.Contains(t, baselineError, "single string argument", "baseline: error should mention validation")
				require.Contains(t, candidateError, "single string argument", "candidate: error should mention validation")

				diff, equal := baselineResp.Equal(&candidateResp)
				if equal {
					t.Log("Both versions produce identical error responses for uncaught geolocation error")
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

				t.Log("Sending 3 events to baseline...")
				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))

				t.Log("Sending 3 events to candidate...")
				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))

				// msg-1 and msg-3 should succeed, msg-2 should fail
				require.Equal(t, 2, len(baselineResp.Events), "baseline: 2 success events expected")
				require.Equal(t, 1, len(baselineResp.FailedEvents), "baseline: 1 failed event expected")
				require.Equal(t, 2, len(candidateResp.Events), "candidate: 2 success events expected")
				require.Equal(t, 1, len(candidateResp.FailedEvents), "candidate: 1 failed event expected")

				diff, equal := baselineResp.Equal(&candidateResp)
				if equal {
					t.Log("Both versions produce identical responses for partial geolocation errors")
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

				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))
				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))

				require.Equal(t, 1, len(baselineResp.Events), "baseline: 1 success event expected")
				require.Equal(t, 1, len(candidateResp.Events), "candidate: 1 success event expected")

				// Verify the geo result is a dict
				require.Contains(t, baselineResp.Events[0].Output["geo_type"], "dict", "baseline: geo should be a dict")
				require.Contains(t, candidateResp.Events[0].Output["geo_type"], "dict", "candidate: geo should be a dict")

				t.Logf("Baseline: type=%v, keys=%v", baselineResp.Events[0].Output["geo_type"], baselineResp.Events[0].Output["geo_keys"])
				t.Logf("Candidate: type=%v, keys=%v", candidateResp.Events[0].Output["geo_type"], candidateResp.Events[0].Output["geo_keys"])

				diff, equal := baselineResp.Equal(&candidateResp)
				if equal {
					t.Log("Both versions produce identical responses for geolocation field access")
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

				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))
				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))

				require.Equal(t, 1, len(baselineResp.Events), "baseline: 1 success event expected")
				require.Equal(t, 1, len(candidateResp.Events), "candidate: 1 success event expected")

				baselineGeo, _ := baselineResp.Events[0].Output["geo_lookup"].(map[string]any)
				candidateGeo, _ := candidateResp.Events[0].Output["geo_lookup"].(map[string]any)
				require.Equal(t, "8.8.8.8", baselineGeo["ip"], "baseline: geo_lookup should contain correct ip")
				require.Equal(t, "8.8.8.8", candidateGeo["ip"], "candidate: geo_lookup should contain correct ip")

				diff, equal := baselineResp.Equal(&candidateResp)
				if equal {
					t.Log("Both versions produce identical responses for geolocation with IP from event")
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

				t.Log("Sending 3 events to baseline...")
				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))

				t.Log("Sending 3 events to candidate...")
				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))

				require.Equal(t, 3, len(baselineResp.Events), "baseline: 3 success events expected")
				require.Equal(t, 3, len(candidateResp.Events), "candidate: 3 success events expected")

				// First and third should have same geo data (same IP)
				// Second should have different geo data
				expectedIPs := []string{"1.2.3.4", "8.8.8.8", "1.2.3.4"}
				for i := range baselineResp.Events {
					baselineGeo, _ := baselineResp.Events[i].Output["geo"].(map[string]any)
					candidateGeo, _ := candidateResp.Events[i].Output["geo"].(map[string]any)
					require.Equalf(t, expectedIPs[i], baselineGeo["ip"], "baseline: event %d geo should contain correct ip", i)
					require.Equalf(t, expectedIPs[i], candidateGeo["ip"], "candidate: event %d geo should contain correct ip", i)
				}

				diff, equal := baselineResp.Equal(&candidateResp)
				if equal {
					t.Log("Both versions produce identical responses for batch geolocation with different IPs")
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

				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))
				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))

				require.Equal(t, 1, len(baselineResp.Events), "baseline: 1 success event expected")
				require.Equal(t, 1, len(candidateResp.Events), "candidate: 1 success event expected")

				baselineError, _ := baselineResp.Events[0].Output["geo_error"].(string)
				candidateError, _ := candidateResp.Events[0].Output["geo_error"].(string)
				t.Logf("Baseline geo_error: %q", baselineError)
				t.Logf("Candidate geo_error: %q", candidateError)

				require.Contains(t, baselineError, "status code: 400", "baseline: error should mention 400")
				require.Contains(t, candidateError, "status code: 400", "candidate: error should mention 400")

				diff, equal := baselineResp.Equal(&candidateResp)
				if equal {
					t.Log("Both versions produce identical responses for invalid IP")
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

				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))
				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))

				require.Equal(t, 1, len(baselineResp.Events), "baseline: 1 success event expected")
				require.Equal(t, 1, len(candidateResp.Events), "candidate: 1 success event expected")

				baselineError, _ := baselineResp.Events[0].Output["geo_error"].(string)
				candidateError, _ := candidateResp.Events[0].Output["geo_error"].(string)
				t.Logf("Baseline geo_error: %q", baselineError)
				t.Logf("Candidate geo_error: %q", candidateError)

				require.Contains(t, baselineError, "status code: 400", "baseline: error should mention 400")
				require.Contains(t, candidateError, "status code: 400", "candidate: error should mention 400")

				diff, equal := baselineResp.Equal(&candidateResp)
				if equal {
					t.Log("Both versions produce identical responses for special chars IP")
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

				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))
				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))

				// Both should produce a failed event since the exception propagates
				require.Equal(t, 0, len(baselineResp.Events), "baseline: no success events expected")
				require.Equal(t, 1, len(baselineResp.FailedEvents), "baseline: 1 failed event expected")
				require.Equal(t, 0, len(candidateResp.Events), "candidate: no success events expected")
				require.Equal(t, 1, len(candidateResp.FailedEvents), "candidate: 1 failed event expected")

				baselineError := baselineResp.FailedEvents[0].Error
				candidateError := candidateResp.FailedEvents[0].Error
				t.Logf("Baseline error: %q", baselineError)
				t.Logf("Candidate error: %q", candidateError)

				require.Contains(t, baselineError, "status code: 400", "baseline: error should mention 400")
				require.Contains(t, candidateError, "status code: 400", "candidate: error should mention 400")

				diff, equal := baselineResp.Equal(&candidateResp)
				if equal {
					t.Log("Both versions produce identical error for uncaught invalid IP")
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

				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))
				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))

				require.Equal(t, 3, len(baselineResp.Events), "baseline: 3 success events expected")
				require.Equal(t, 3, len(candidateResp.Events), "candidate: 3 success events expected")

				for i := range baselineResp.Events {
					baselineError, _ := baselineResp.Events[i].Output["geo_error"].(string)
					candidateError, _ := candidateResp.Events[i].Output["geo_error"].(string)
					require.Containsf(t, baselineError, "status code: 400", "baseline: event %d error should mention 400", i)
					require.Containsf(t, candidateError, "status code: 400", "candidate: event %d error should mention 400", i)
				}

				diff, equal := baselineResp.Equal(&candidateResp)
				if equal {
					t.Log("Both versions produce identical responses for batch invalid IP")
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

				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))
				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))

				// All 3 events succeed (errors are caught)
				require.Equal(t, 3, len(baselineResp.Events), "baseline: 3 success events expected")
				require.Equal(t, 3, len(candidateResp.Events), "candidate: 3 success events expected")

				// msg-1 and msg-3 should have geo data, msg-2 should have geo_error
				for _, resp := range []*types.Response{&baselineResp, &candidateResp} {
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

				diff, equal := baselineResp.Equal(&candidateResp)
				if equal {
					t.Log("Both versions produce identical responses for partial invalid IP")
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

				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))
				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))

				// Private IP returns 200 with empty string fields — no error
				require.Equal(t, 1, len(baselineResp.Events), "baseline: 1 success event expected")
				require.Equal(t, 0, len(baselineResp.FailedEvents), "baseline: no failed events expected")
				require.Equal(t, 1, len(candidateResp.Events), "candidate: 1 success event expected")
				require.Equal(t, 0, len(candidateResp.FailedEvents), "candidate: no failed events expected")

				// Geo data should be present with correct IP but empty values
				baselineGeo, _ := baselineResp.Events[0].Output["geo"].(map[string]any)
				candidateGeo, _ := candidateResp.Events[0].Output["geo"].(map[string]any)
				require.Equal(t, "127.0.0.1", baselineGeo["ip"], "baseline: geo should contain correct ip")
				require.Equal(t, "127.0.0.1", candidateGeo["ip"], "candidate: geo should contain correct ip")

				t.Logf("Baseline geo: %v", baselineResp.Events[0].Output["geo"])
				t.Logf("Candidate geo: %v", candidateResp.Events[0].Output["geo"])

				diff, equal := baselineResp.Equal(&candidateResp)
				if equal {
					t.Log("Both versions produce identical responses for private IP")
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

				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))
				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))

				// All events should fail since the exception propagates
				require.Equal(t, 0, len(baselineResp.Events), "baseline: no success events expected")
				require.Equal(t, 2, len(baselineResp.FailedEvents), "baseline: 2 failed events expected")
				require.Equal(t, 0, len(candidateResp.Events), "candidate: no success events expected")
				require.Equal(t, 2, len(candidateResp.FailedEvents), "candidate: 2 failed events expected")

				for i := range baselineResp.FailedEvents {
					require.Containsf(t, baselineResp.FailedEvents[i].Error, "status code: 400", "baseline: event %d error should mention 400", i)
					require.Containsf(t, candidateResp.FailedEvents[i].Error, "status code: 400", "candidate: event %d error should mention 400", i)
				}

				diff, equal := baselineResp.Equal(&candidateResp)
				if equal {
					t.Log("Both versions produce identical responses for batch invalid IP uncaught")
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

				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))
				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))

				require.Equal(t, 2, len(baselineResp.Events), "baseline: 2 success events expected")
				require.Equal(t, 2, len(candidateResp.Events), "candidate: 2 success events expected")

				for i := range baselineResp.Events {
					baselineError, _ := baselineResp.Events[i].Output["geo_error"].(string)
					candidateError, _ := candidateResp.Events[i].Output["geo_error"].(string)
					require.Containsf(t, baselineError, "single string argument", "baseline: event %d error should mention single string argument", i)
					require.Containsf(t, candidateError, "single string argument", "candidate: event %d error should mention single string argument", i)
				}

				diff, equal := baselineResp.Equal(&candidateResp)
				if equal {
					t.Log("Both versions produce identical responses for batch no args")
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

				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))
				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))

				require.Equal(t, 1, len(baselineResp.Events), "baseline: 1 success event expected")
				require.Equal(t, 1, len(candidateResp.Events), "candidate: 1 success event expected")

				baselineError, _ := baselineResp.Events[0].Output["geo_error"].(string)
				candidateError, _ := candidateResp.Events[0].Output["geo_error"].(string)
				t.Logf("Baseline geo_error: %q", baselineError)
				t.Logf("Candidate geo_error: %q", candidateError)

				require.NotEmpty(t, baselineError, "baseline: should have a geo_error")
				require.NotEmpty(t, candidateError, "candidate: should have a geo_error")

				diff, equal := baselineResp.Equal(&candidateResp)
				if equal {
					t.Log("Both versions produce identical responses for keyword arg")
				} else {
					t.Errorf("Responses differ:\n%s", diff)
				}
			},
		},
	}

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = 2 * time.Minute

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
		if !st.config.isZero() {
			_, dup := allEntries[st.versionID]
			require.Falsef(t, dup, "duplicate versionID %q: with shared containers the versionID is the only "+
				"isolation boundary between subtests, so a collision serves one subtest's code to another", st.versionID)
			allEntries[st.versionID] = st.config
		}
	}
	configBackend := newContractConfigBackend(t, allEntries)
	t.Cleanup(configBackend.Close)

	var (
		wg                        sync.WaitGroup
		baselineURL, candidateURL string
	)
	wg.Go(func() {
		baselineURL = startBaselinePytransformer(t, pool, configBackend.URL, "GEOLOCATION_URL="+geoURL)
	})
	wg.Go(func() {
		candidateURL = startCandidatePytransformer(t, pool, configBackend.URL, "GEOLOCATION_URL="+geoURL)
	})
	wg.Wait()

	// Run subtests sequentially.
	for _, st := range subtests {
		t.Run(st.name, func(t *testing.T) {
			env := newBCTestEnv(t, baselineURL, candidateURL, withLimitedRetryableHTTPRetries())

			st.run(t, env)
			env.assertRetryCountsMatch(t)
		})
	}
}

// TestBackwardsCompatibilityGeolocationNotConfigured tests geolocation behavior
// when the geolocation service URL is NOT configured in either version.
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

				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))
				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))

				// Both should succeed (error caught by try/except)
				require.Equal(t, 1, len(baselineResp.Events), "baseline: 1 success event expected")
				require.Equal(t, 1, len(candidateResp.Events), "candidate: 1 success event expected")

				// Both should report "not supported" error
				baselineError, _ := baselineResp.Events[0].Output["geo_error"].(string)
				candidateError, _ := candidateResp.Events[0].Output["geo_error"].(string)
				t.Logf("Baseline geo_error: %q", baselineError)
				t.Logf("Candidate geo_error: %q", candidateError)

				require.Contains(t, baselineError, "not supported", "baseline: error should mention not supported")
				require.Contains(t, candidateError, "not supported", "candidate: error should mention not supported")

				diff, equal := baselineResp.Equal(&candidateResp)
				if equal {
					t.Log("Both versions produce identical responses when geolocation is not configured")
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

				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))
				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))

				// Both should fail since exception propagates
				require.Equal(t, 0, len(baselineResp.Events), "baseline: no success events expected")
				require.Equal(t, 1, len(baselineResp.FailedEvents), "baseline: 1 failed event expected")
				require.Equal(t, 0, len(candidateResp.Events), "candidate: no success events expected")
				require.Equal(t, 1, len(candidateResp.FailedEvents), "candidate: 1 failed event expected")

				baselineError := baselineResp.FailedEvents[0].Error
				candidateError := candidateResp.FailedEvents[0].Error
				t.Logf("Baseline error: %q", baselineError)
				t.Logf("Candidate error: %q", candidateError)

				require.Contains(t, baselineError, "not supported", "baseline: error should mention not supported")
				require.Contains(t, candidateError, "not supported", "candidate: error should mention not supported")

				diff, equal := baselineResp.Equal(&candidateResp)
				if equal {
					t.Log("Both versions produce identical error for uncaught geolocation not configured")
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

				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))
				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))

				require.Equal(t, 2, len(baselineResp.Events), "baseline: 2 success events expected")
				require.Equal(t, 2, len(candidateResp.Events), "candidate: 2 success events expected")

				// Both should have geo_error for all events
				for i := range baselineResp.Events {
					baselineError, _ := baselineResp.Events[i].Output["geo_error"].(string)
					candidateError, _ := candidateResp.Events[i].Output["geo_error"].(string)
					require.Containsf(t, baselineError, "not supported", "baseline: event %d error should mention not supported", i)
					require.Containsf(t, candidateError, "not supported", "candidate: event %d error should mention not supported", i)
				}

				diff, equal := baselineResp.Equal(&candidateResp)
				if equal {
					t.Log("Both versions produce identical responses for batch geolocation not configured")
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

				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))
				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))

				// All events should fail since the exception propagates
				require.Equal(t, 0, len(baselineResp.Events), "baseline: no success events expected")
				require.Equal(t, 2, len(baselineResp.FailedEvents), "baseline: 2 failed events expected")
				require.Equal(t, 0, len(candidateResp.Events), "candidate: no success events expected")
				require.Equal(t, 2, len(candidateResp.FailedEvents), "candidate: 2 failed events expected")

				for i := range baselineResp.FailedEvents {
					require.Containsf(t, baselineResp.FailedEvents[i].Error, "not supported", "baseline: event %d error should mention not supported", i)
					require.Containsf(t, candidateResp.FailedEvents[i].Error, "not supported", "candidate: event %d error should mention not supported", i)
				}

				diff, equal := baselineResp.Equal(&candidateResp)
				if equal {
					t.Log("Both versions produce identical responses for batch geolocation not configured uncaught")
				} else {
					t.Errorf("Responses differ:\n%s", diff)
				}
			},
		},
	}

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = 2 * time.Minute

	// Collect all config backend entries.
	allEntries := make(map[string]configBackendEntry, len(subtests))
	for _, st := range subtests {
		if !st.config.isZero() {
			_, dup := allEntries[st.versionID]
			require.Falsef(t, dup, "duplicate versionID %q: with shared containers the versionID is the only "+
				"isolation boundary between subtests, so a collision serves one subtest's code to another", st.versionID)
			allEntries[st.versionID] = st.config
		}
	}
	configBackend := newContractConfigBackend(t, allEntries)
	t.Cleanup(configBackend.Close)

	// Both containers start WITHOUT a geolocation URL — that is what this test
	// is about.
	var (
		wg                        sync.WaitGroup
		baselineURL, candidateURL string
	)
	wg.Go(func() {
		baselineURL = startBaselinePytransformer(t, pool, configBackend.URL)
	})
	wg.Go(func() {
		candidateURL = startCandidatePytransformer(t, pool, configBackend.URL)
	})
	wg.Wait()

	for _, st := range subtests {
		t.Run(st.name, func(t *testing.T) {
			env := newBCTestEnv(t, baselineURL, candidateURL, withLimitedRetryableHTTPRetries())

			st.run(t, env)
			env.assertRetryCountsMatch(t)
		})
	}
}

// TestBackwardsCompatibilityGeolocationFailure tests behavior when the
// geolocation service experiences network failures or returns various HTTP
// error status codes. Uses a configurable mock geolocation service to
// simulate different failure scenarios.
// Both versions raise: "geolocation fetch failed with status code: {code}"
func TestBackwardsCompatibilityGeolocationFailure(t *testing.T) {
	type subtest struct {
		name      string
		versionID string
		config    configBackendEntry
		setup     func() // called before run to configure mock behavior
		run       func(t *testing.T, env *bcTestEnv)
		// skipRetryCountMatch skips assertRetryCountsMatch. Two releases of the same engine should retry
		// identically, so a divergence is a real regression: set this only where the subtest itself makes the
		// counts incomparable (e.g. it drives the two sides differently), and say why at the call site.
		skipRetryCountMatch bool
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

				// Both versions raise GeolocationServerError(BaseException), which bypasses
				// the user's except Exception and propagates as HTTP 503 with retry →
				// retries exhausted → failed event. The user's geo_error branch never runs.
				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))
				require.Equal(t, 0, len(baselineResp.Events), "baseline: no success events expected (geo 5xx triggers retries)")
				require.Equal(t, 1, len(baselineResp.FailedEvents), "baseline: 1 failed event expected")
				t.Logf("Baseline error: %q", baselineResp.FailedEvents[0].Error)

				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))
				require.Equal(t, 0, len(candidateResp.Events), "candidate: no success events expected (geo 5xx triggers retries)")
				require.Equal(t, 1, len(candidateResp.FailedEvents), "candidate: 1 failed event expected")
				t.Logf("Candidate error: %q", candidateResp.FailedEvents[0].Error)

				diff, equal := baselineResp.Equal(&candidateResp)
				require.Truef(t, equal, "baseline and candidate must agree:\n%s", diff)
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

				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))
				require.Equal(t, 0, len(baselineResp.Events), "baseline: no success events expected (geo 5xx triggers retries)")
				require.Equal(t, 1, len(baselineResp.FailedEvents), "baseline: 1 failed event expected")
				t.Logf("Baseline error: %q", baselineResp.FailedEvents[0].Error)

				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))
				require.Equal(t, 0, len(candidateResp.Events), "candidate: no success events expected (geo 5xx triggers retries)")
				require.Equal(t, 1, len(candidateResp.FailedEvents), "candidate: 1 failed event expected")
				t.Logf("Candidate error: %q", candidateResp.FailedEvents[0].Error)

				diff, equal := baselineResp.Equal(&candidateResp)
				require.Truef(t, equal, "baseline and candidate must agree:\n%s", diff)
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

				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))
				require.Equal(t, 0, len(baselineResp.Events), "baseline: no success events expected (geo 5xx triggers retries)")
				require.Equal(t, 1, len(baselineResp.FailedEvents), "baseline: 1 failed event expected")
				t.Logf("Baseline error: %q", baselineResp.FailedEvents[0].Error)

				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))
				require.Equal(t, 0, len(candidateResp.Events), "candidate: no success events expected (geo 5xx triggers retries)")
				require.Equal(t, 1, len(candidateResp.FailedEvents), "candidate: 1 failed event expected")
				t.Logf("Candidate error: %q", candidateResp.FailedEvents[0].Error)

				diff, equal := baselineResp.Equal(&candidateResp)
				require.Truef(t, equal, "baseline and candidate must agree:\n%s", diff)
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

				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))
				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))

				require.Equal(t, 1, len(baselineResp.Events), "baseline: 1 success event expected")
				require.Equal(t, 1, len(candidateResp.Events), "candidate: 1 success event expected")

				baselineError, _ := baselineResp.Events[0].Output["geo_error"].(string)
				candidateError, _ := candidateResp.Events[0].Output["geo_error"].(string)
				t.Logf("Baseline geo_error: %q", baselineError)
				t.Logf("Candidate geo_error: %q", candidateError)

				require.Contains(t, baselineError, "status code: 429", "baseline: error should mention 429")
				require.Contains(t, candidateError, "status code: 429", "candidate: error should mention 429")

				diff, equal := baselineResp.Equal(&candidateResp)
				require.Truef(t, equal, "baseline and candidate must agree:\n%s", diff)
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

				// No try/except here, so the GeolocationServerError propagates on both versions:
				// 503 with retry → retries exhausted → failed event.
				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))
				require.Equal(t, 0, len(baselineResp.Events), "baseline: no success events expected")
				require.Equal(t, 1, len(baselineResp.FailedEvents), "baseline: 1 failed event expected")
				t.Logf("Baseline error: %q", baselineResp.FailedEvents[0].Error)

				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))
				require.Equal(t, 0, len(candidateResp.Events), "candidate: no success events expected")
				require.Equal(t, 1, len(candidateResp.FailedEvents), "candidate: 1 failed event expected")
				t.Logf("Candidate error: %q", candidateResp.FailedEvents[0].Error)

				diff, equal := baselineResp.Equal(&candidateResp)
				require.Truef(t, equal, "baseline and candidate must agree:\n%s", diff)
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

				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))
				require.Equal(t, 0, len(baselineResp.Events), "baseline: no success events expected (geo 5xx triggers retries)")
				require.Equal(t, 3, len(baselineResp.FailedEvents), "baseline: 3 failed events expected")

				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))
				require.Equal(t, 0, len(candidateResp.Events), "candidate: no success events expected (geo 5xx triggers retries)")
				require.Equal(t, 3, len(candidateResp.FailedEvents), "candidate: 3 failed events expected")

				diff, equal := baselineResp.Equal(&candidateResp)
				require.Truef(t, equal, "baseline and candidate must agree:\n%s", diff)
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

				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))
				require.Equal(t, 0, len(baselineResp.Events), "baseline: no success events expected (geo 5xx triggers retries)")
				require.Equal(t, 3, len(baselineResp.FailedEvents), "baseline: 3 failed events expected")

				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))
				require.Equal(t, 0, len(candidateResp.Events), "candidate: no success events expected (geo 5xx triggers retries)")
				require.Equal(t, 3, len(candidateResp.FailedEvents), "candidate: 3 failed events expected")

				diff, equal := baselineResp.Equal(&candidateResp)
				require.Truef(t, equal, "baseline and candidate must agree:\n%s", diff)
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

				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))
				require.Equal(t, 0, len(baselineResp.Events), "baseline: no success events expected (geo 5xx triggers retries)")
				require.Equal(t, 3, len(baselineResp.FailedEvents), "baseline: 3 failed events expected")

				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))
				require.Equal(t, 0, len(candidateResp.Events), "candidate: no success events expected (geo 5xx triggers retries)")
				require.Equal(t, 3, len(candidateResp.FailedEvents), "candidate: 3 failed events expected")

				diff, equal := baselineResp.Equal(&candidateResp)
				require.Truef(t, equal, "baseline and candidate must agree:\n%s", diff)
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

				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))
				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))

				require.Equal(t, 3, len(baselineResp.Events), "baseline: 3 success events expected")
				require.Equal(t, 3, len(candidateResp.Events), "candidate: 3 success events expected")

				for i := range baselineResp.Events {
					baselineError, _ := baselineResp.Events[i].Output["geo_error"].(string)
					candidateError, _ := candidateResp.Events[i].Output["geo_error"].(string)
					require.Containsf(t, baselineError, "status code: 429", "baseline: event %d error should mention 429", i)
					require.Containsf(t, candidateError, "status code: 429", "candidate: event %d error should mention 429", i)
				}

				diff, equal := baselineResp.Equal(&candidateResp)
				require.Truef(t, equal, "baseline and candidate must agree:\n%s", diff)
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

				// On both versions the connection error becomes a GeolocationServerError(BaseException), which bypasses
				// the user's except Exception → 503 retries → failed event.
				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))
				require.Equal(t, 0, len(baselineResp.Events), "baseline: no success events expected (geo errors trigger retries)")
				require.Equal(t, 1, len(baselineResp.FailedEvents), "baseline: 1 failed event expected")
				t.Logf("Baseline error: %q", baselineResp.FailedEvents[0].Error)

				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))
				require.Equal(t, 0, len(candidateResp.Events), "candidate: no success events expected (geo errors trigger retries)")
				require.Equal(t, 1, len(candidateResp.FailedEvents), "candidate: 1 failed event expected")
				t.Logf("Candidate error: %q", candidateResp.FailedEvents[0].Error)

				diff, equal := baselineResp.Equal(&candidateResp)
				require.Truef(t, equal, "baseline and candidate must agree:\n%s", diff)
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

				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))

				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))
				require.Equal(t, 0, len(candidateResp.Events), "candidate: no success events expected (geo errors trigger retries)")
				require.Equal(t, 3, len(candidateResp.FailedEvents), "candidate: 3 failed events expected")

				diff, equal := baselineResp.Equal(&candidateResp)
				require.Truef(t, equal, "baseline and candidate must agree:\n%s", diff)
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

				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))
				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))

				require.Equal(t, 1, len(baselineResp.Events), "baseline: 1 success event expected")
				require.Equal(t, 1, len(candidateResp.Events), "candidate: 1 success event expected")

				baselineError, _ := baselineResp.Events[0].Output["geo_error"].(string)
				candidateError, _ := candidateResp.Events[0].Output["geo_error"].(string)
				t.Logf("Baseline geo_error: %q", baselineError)
				t.Logf("Candidate geo_error: %q", candidateError)

				require.Contains(t, baselineError, "status code: 400", "baseline: error should mention 400")
				require.Contains(t, candidateError, "status code: 400", "candidate: error should mention 400")

				diff, equal := baselineResp.Equal(&candidateResp)
				require.Truef(t, equal, "baseline and candidate must agree:\n%s", diff)
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

				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))
				require.Equal(t, 0, len(baselineResp.Events), "baseline: no success events expected")
				require.Equal(t, 1, len(baselineResp.FailedEvents), "baseline: 1 failed event expected")
				t.Logf("Baseline error: %q", baselineResp.FailedEvents[0].Error)

				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))
				require.Equal(t, 0, len(candidateResp.Events), "candidate: no success events expected")
				require.Equal(t, 1, len(candidateResp.FailedEvents), "candidate: 1 failed event expected")
				t.Logf("Candidate error: %q", candidateResp.FailedEvents[0].Error)

				diff, equal := baselineResp.Equal(&candidateResp)
				require.Truef(t, equal, "baseline and candidate must agree:\n%s", diff)
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

				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))
				require.Equal(t, 0, len(baselineResp.Events), "baseline: no success events expected")
				require.Equal(t, 1, len(baselineResp.FailedEvents), "baseline: 1 failed event expected")
				t.Logf("Baseline error: %q", baselineResp.FailedEvents[0].Error)

				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))
				require.Equal(t, 0, len(candidateResp.Events), "candidate: no success events expected")
				require.Equal(t, 1, len(candidateResp.FailedEvents), "candidate: 1 failed event expected")
				t.Logf("Candidate error: %q", candidateResp.FailedEvents[0].Error)

				diff, equal := baselineResp.Equal(&candidateResp)
				require.Truef(t, equal, "baseline and candidate must agree:\n%s", diff)
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

				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))
				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))

				require.Equal(t, 0, len(baselineResp.Events), "baseline: no success events expected")
				require.Equal(t, 1, len(baselineResp.FailedEvents), "baseline: 1 failed event expected")
				require.Equal(t, 0, len(candidateResp.Events), "candidate: no success events expected")
				require.Equal(t, 1, len(candidateResp.FailedEvents), "candidate: 1 failed event expected")

				baselineError := baselineResp.FailedEvents[0].Error
				candidateError := candidateResp.FailedEvents[0].Error
				t.Logf("Baseline error: %q", baselineError)
				t.Logf("Candidate error: %q", candidateError)

				require.Contains(t, baselineError, "status code: 429", "baseline: error should mention 429")
				require.Contains(t, candidateError, "status code: 429", "candidate: error should mention 429")

				diff, equal := baselineResp.Equal(&candidateResp)
				require.Truef(t, equal, "baseline and candidate must agree:\n%s", diff)
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

				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))
				require.Equal(t, 0, len(baselineResp.Events), "baseline: no success events expected")
				require.Equal(t, 1, len(baselineResp.FailedEvents), "baseline: 1 failed event expected")
				t.Logf("Baseline error: %q", baselineResp.FailedEvents[0].Error)
				require.NotEmpty(t, baselineResp.FailedEvents[0].Error, "baseline: should have an error")

				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))
				require.Equal(t, 0, len(candidateResp.Events), "candidate: no success events expected")
				require.Equal(t, 1, len(candidateResp.FailedEvents), "candidate: 1 failed event expected")
				t.Logf("Candidate error: %q", candidateResp.FailedEvents[0].Error)
				require.NotEmpty(t, candidateResp.FailedEvents[0].Error, "candidate: should have an error")

				diff, equal := baselineResp.Equal(&candidateResp)
				require.Truef(t, equal, "baseline and candidate must agree:\n%s", diff)
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

				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))
				require.Equal(t, 0, len(baselineResp.Events), "baseline: no success events expected")
				require.Equal(t, 2, len(baselineResp.FailedEvents), "baseline: 2 failed events expected")

				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))
				require.Equal(t, 0, len(candidateResp.Events), "candidate: no success events expected")
				require.Equal(t, 2, len(candidateResp.FailedEvents), "candidate: 2 failed events expected")

				diff, equal := baselineResp.Equal(&candidateResp)
				require.Truef(t, equal, "baseline and candidate must agree:\n%s", diff)
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

				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))
				require.Equal(t, 0, len(baselineResp.Events), "baseline: no success events expected")
				require.Equal(t, 2, len(baselineResp.FailedEvents), "baseline: 2 failed events expected")
				for i := range baselineResp.FailedEvents {
					require.NotEmptyf(t, baselineResp.FailedEvents[i].Error, "baseline: event %d should have an error", i)
				}

				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))
				require.Equal(t, 0, len(candidateResp.Events), "candidate: no success events expected")
				require.Equal(t, 2, len(candidateResp.FailedEvents), "candidate: 2 failed events expected")
				for i := range candidateResp.FailedEvents {
					require.NotEmptyf(t, candidateResp.FailedEvents[i].Error, "candidate: event %d should have an error", i)
				}

				diff, equal := baselineResp.Equal(&candidateResp)
				require.Truef(t, equal, "baseline and candidate must agree:\n%s", diff)
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
			// SANDBOX_HTTP_BUDGET_S does NOT apply to internal geolocation traffic — see
			// TestSandboxHTTPBudgetDoesNotCapGeolocation.
			name:      "GeoTimeout",
			versionID: "bc-geo-timeout-v1",
			config: configBackendEntry{code: `
def transformEvent(event, metadata):
    try:
        result = geolocation("1.2.3.4")
        event["geo"] = result
    except Exception as e:
        # Candidate must NOT reach this branch — GeolocationServerError
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

				// Both versions: timeout → GeolocationServerError → HTTP 503 + X-Rudder-Should-Retry →
				// retries exhausted → failed event (with the user's event["geo_error"] branch never executed).
				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))
				require.Len(t, baselineResp.Events, 0,
					"baseline: slow geolocation must NOT produce a success event "+
						"(GeolocationServerError must bypass user except-Exception)")
				require.Len(t, baselineResp.FailedEvents, 1,
					"baseline: slow geolocation must surface as a failed event after retries")
				require.Contains(t, baselineResp.FailedEvents[0].Error,
					"transformer returned status code: 503",
					"baseline: failed event must carry the correct error message")

				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))
				require.Len(t, candidateResp.Events, 0,
					"candidate: slow geolocation must NOT produce a success event "+
						"(GeolocationServerError must bypass user except-Exception)")
				require.Len(t, candidateResp.FailedEvents, 1,
					"candidate: slow geolocation must surface as a failed event after retries")
				require.Contains(t, candidateResp.FailedEvents[0].Error,
					"transformer returned status code: 503",
					"candidate: failed event must carry the correct error message")

				diff, equal := baselineResp.Equal(&candidateResp)
				require.Truef(t, equal, "baseline and candidate must agree:\n%s", diff)

				retriesCounter := env.CandidateStats.GetByName("processor_user_transformer_http_retries")
				require.Len(t, retriesCounter, 1)
				require.EqualValues(t, 2, retriesCounter[0].Value)

				// Both versions must exhaust the same retry budget on the same 503.
				baselineRetries := env.BaselineStats.GetByName("processor_user_transformer_http_retries")
				require.Len(t, baselineRetries, 1)
				require.EqualValues(t, retriesCounter[0].Value, baselineRetries[0].Value,
					"baseline and candidate must retry the same number of times")
			},
		},
	}

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = 2 * time.Minute

	// Collect all config backend entries.
	allEntries := make(map[string]configBackendEntry, len(subtests))
	for _, st := range subtests {
		if !st.config.isZero() {
			_, dup := allEntries[st.versionID]
			require.Falsef(t, dup, "duplicate versionID %q: with shared containers the versionID is the only "+
				"isolation boundary between subtests, so a collision serves one subtest's code to another", st.versionID)
			allEntries[st.versionID] = st.config
		}
	}
	configBackend := newContractConfigBackend(t, allEntries)
	t.Cleanup(configBackend.Close)

	// Both containers run with the configurable mock geolocation URL.
	// GEOLOCATION_TIMEOUT_SECS=0.1 governs the GeoTimeout subtest's 500 ms mock
	// delay (100 ms < 500 ms → retryable 503). SANDBOX_HTTP_BUDGET_S is kept
	// low as a guard for any future subtest exercising user HTTP traffic; it
	// does NOT affect geolocation calls — see
	// TestSandboxHTTPBudgetDoesNotCapGeolocation.
	pytEnv := []string{
		"GEOLOCATION_URL=" + geoURL,
		"SANDBOX_HTTP_BUDGET_S=0.1",
		"GEOLOCATION_TIMEOUT_SECS=0.1",
	}

	var (
		wg                        sync.WaitGroup
		baselineURL, candidateURL string
	)
	wg.Go(func() {
		baselineURL = startBaselinePytransformer(t, pool, configBackend.URL, pytEnv...)
	})
	wg.Go(func() {
		candidateURL = startCandidatePytransformer(t, pool, configBackend.URL, pytEnv...)
	})
	wg.Wait()

	// Run subtests sequentially.
	for _, st := range subtests {
		t.Run(st.name, func(t *testing.T) {
			env := newBCTestEnv(t, baselineURL, candidateURL,
				withFailOnError(),
				withLimitedRetryableHTTPRetries(),
			)

			// The mock is shared with every other subtest, so hand it back healthy however this one
			// ends — including on failure, which is when a leaked failure mode would be most confusing.
			t.Cleanup(mockGeoCfg.reset)
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
