package processor

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	mocksBackendConfig "github.com/rudderlabs/rudder-server/mocks/backend-config"
	"github.com/rudderlabs/rudder-server/processor/types"
	transformerFeaturesService "github.com/rudderlabs/rudder-server/services/transformer"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
)

const (
	captureOptInSourceID   = "capture-optin-source"
	captureOptInDestID     = "capture-optin-destination"
	captureOptInWorkspace  = "capture-optin-workspace"
	captureOptInEnabledCfg = `{"source":{"syncSettings":{"errorDetailsConfig":{"enabled":true,"retentionInDays":30}}}}`
	captureOptInDisabled   = `{"source":{"syncSettings":{"errorDetailsConfig":{"enabled":false,"retentionInDays":30}}}}`
)

// connectionConfig decodes a connection's `config` column exactly as backend-config does, so the
// nested values under test are the `map[string]any` the resolver actually walks in production.
func connectionConfig(t *testing.T, raw string) map[string]any {
	t.Helper()
	var cfg map[string]any
	require.NoError(t, jsonrs.Unmarshal([]byte(raw), &cfg))
	return cfg
}

// routerTransformFeatures reports every destination type as router-transformable. destTransform then
// skips the destination-transform call and marshals the response it was handed, which is the
// production path that isolates the ParametersT marshal site under test.
type routerTransformFeatures struct {
	transformerFeaturesService.FeaturesService
}

func (routerTransformFeatures) RouterTransform(string) bool { return true }

// newCaptureOptInHandle returns a Handle whose connection config is driven by the real
// backendConfigSubscriber - the same mechanism that rebuilds connectionConfigMap on every config
// push in production - together with the channel that feeds it.
func newCaptureOptInHandle(t *testing.T) (*Handle, chan pubsub.DataEvent) {
	t.Helper()

	ctrl := gomock.NewController(t)
	mockBackendConfig := mocksBackendConfig.NewMockBackendConfig(ctrl)
	configs := make(chan pubsub.DataEvent, 1)
	mockBackendConfig.EXPECT().Subscribe(gomock.Any(), backendconfig.TopicProcessConfig).
		DoAndReturn(func(_ context.Context, _ backendconfig.Topic) pubsub.DataChannel {
			return configs
		}).Times(1)

	proc := &Handle{
		logger:                     logger.NOP,
		backendConfig:              mockBackendConfig,
		transformerFeaturesService: routerTransformFeatures{transformerFeaturesService.NewNoOpService()},
		captureOptInPins:           newCaptureOptInPins(),
	}
	proc.config.asyncInit = misc.NewAsyncInit(1)

	ctx, cancel := context.WithCancel(context.Background())
	subscriberDone := make(chan struct{})
	go func() {
		defer close(subscriberDone)
		proc.backendConfigSubscriber(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		close(configs)
		<-subscriberDone
		ctrl.Finish()
	})

	return proc, configs
}

// pushConnectionConfig publishes a workspace config carrying a single connection and blocks until
// the subscriber has rebuilt connectionConfigMap from it.
func pushConnectionConfig(t *testing.T, proc *Handle, configs chan pubsub.DataEvent, conn backendconfig.Connection, wantResolved bool) {
	t.Helper()

	configs <- pubsub.DataEvent{
		Topic: string(backendconfig.TopicProcessConfig),
		Data: map[string]backendconfig.ConfigT{
			captureOptInWorkspace: {
				WorkspaceID: captureOptInWorkspace,
				Connections: map[string]backendconfig.Connection{"connection-1": conn},
			},
		},
	}
	require.Eventuallyf(t, func() bool {
		return proc.resolveCaptureErrorOptIn(captureOptInSourceID, captureOptInDestID) == wantResolved
	}, 10*time.Second, time.Millisecond, "connection config push never reached connectionConfigMap")
}

func optedInConnection(t *testing.T, enabled bool) backendconfig.Connection {
	t.Helper()

	raw := captureOptInDisabled
	if enabled {
		raw = captureOptInEnabledCfg
	}
	return backendconfig.Connection{
		SourceID:      captureOptInSourceID,
		DestinationID: captureOptInDestID,
		Enabled:       true,
		Config:        connectionConfig(t, raw),
	}
}

// marshalledJobParams drives the real destTransform marshal site with a single transformer response
// and returns the router job's Parameters.
func marshalledJobParams(t *testing.T, proc *Handle, metadata types.Metadata) []byte {
	t.Helper()

	out := proc.destTransform(context.Background(), userTransformAndFilterOutput{
		eventsToTransform: []types.TransformerEvent{{}},
		commonMetaData: &types.Metadata{
			SourceID:      metadata.SourceID,
			DestinationID: metadata.DestinationID,
			WorkspaceID:   captureOptInWorkspace,
		},
		procErrorJobsByDestID: map[string][]procErrorJob{},
		transformAt:           "router",
		response: types.Response{
			Events: []types.TransformerResponse{{
				Output:   map[string]any{"event": "purchase"},
				Metadata: metadata,
			}},
		},
	})
	require.Len(t, out.destJobs, 1)
	return out.destJobs[0].Parameters
}

func retlMetadata(jobRunID string) types.Metadata {
	return types.Metadata{
		SourceID:       captureOptInSourceID,
		DestinationID:  captureOptInDestID,
		WorkspaceID:    captureOptInWorkspace,
		SourceJobRunID: jobRunID,
		RecordID:       "1",
	}
}

// A3.1 / A3.2 at the resolver: the decision comes from the connection's backend config and fails
// closed on every shape that is not an explicit boolean true at the expected path.
func TestResolveCaptureErrorOptIn(t *testing.T) {
	for _, tc := range []struct {
		name string
		raw  string
		want bool
	}{
		{"enabled true", captureOptInEnabledCfg, true},
		{"enabled false", captureOptInDisabled, false},
		{"errorDetailsConfig absent", `{"source":{"syncSettings":{"syncLogsConfig":{"enabled":true}}}}`, false},
		{"syncSettings absent", `{"source":{"name":"a-source"}}`, false},
		{"source absent", `{"destination":{"syncSettings":{"errorDetailsConfig":{"enabled":true}}}}`, false},
		{"empty config", `{}`, false},
		{"enabled is a string", `{"source":{"syncSettings":{"errorDetailsConfig":{"enabled":"true"}}}}`, false},
		{"enabled is a number", `{"source":{"syncSettings":{"errorDetailsConfig":{"enabled":1}}}}`, false},
		{"enabled is null", `{"source":{"syncSettings":{"errorDetailsConfig":{"enabled":null}}}}`, false},
		{"errorDetailsConfig is not an object", `{"source":{"syncSettings":{"errorDetailsConfig":true}}}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := &Handle{}
			proc.config.connectionConfigMap = map[connection]backendconfig.Connection{
				{sourceID: captureOptInSourceID, destinationID: captureOptInDestID}: {
					SourceID:      captureOptInSourceID,
					DestinationID: captureOptInDestID,
					Config:        connectionConfig(t, tc.raw),
				},
			}
			require.Equal(t, tc.want, proc.resolveCaptureErrorOptIn(captureOptInSourceID, captureOptInDestID))
		})
	}

	t.Run("unknown connection fails closed", func(t *testing.T) {
		proc := &Handle{}
		proc.config.connectionConfigMap = map[connection]backendconfig.Connection{}
		require.False(t, proc.resolveCaptureErrorOptIn(captureOptInSourceID, captureOptInDestID))
	})

	t.Run("connection with a nil config fails closed", func(t *testing.T) {
		proc := &Handle{}
		proc.config.connectionConfigMap = map[connection]backendconfig.Connection{
			{sourceID: captureOptInSourceID, destinationID: captureOptInDestID}: {
				SourceID:      captureOptInSourceID,
				DestinationID: captureOptInDestID,
			},
		}
		require.False(t, proc.resolveCaptureErrorOptIn(captureOptInSourceID, captureOptInDestID))
	})
}

// A3.1: an opted-in connection stamps capture_error=true on the job parameters the collector parses.
func TestCaptureErrorReachesJobParameters(t *testing.T) {
	proc, configs := newCaptureOptInHandle(t)
	pushConnectionConfig(t, proc, configs, optedInConnection(t, true), true)

	params := marshalledJobParams(t, proc, retlMetadata("run-1"))

	captureError := gjson.GetBytes(params, "capture_error")
	require.True(t, captureError.Exists(), "capture_error must be present, got: %s", params)
	require.Equal(t, "true", captureError.Raw, "capture_error must be a JSON bool, not a string")
	require.Equal(t, "run-1", gjson.GetBytes(params, "source_job_run_id").String())
}

// A3.2: absent, false and unknown connections all resolve false, and `omitempty` keeps the key out of
// the marshalled parameters entirely - old collectors must see byte-identical params.
func TestCaptureErrorOmittedWhenNotOptedIn(t *testing.T) {
	t.Run("connection opted out", func(t *testing.T) {
		proc, configs := newCaptureOptInHandle(t)
		pushConnectionConfig(t, proc, configs, optedInConnection(t, false), false)

		params := marshalledJobParams(t, proc, retlMetadata("run-1"))
		require.False(t, gjson.GetBytes(params, "capture_error").Exists(), "got: %s", params)
	})

	t.Run("errorDetailsConfig absent from the connection", func(t *testing.T) {
		proc, configs := newCaptureOptInHandle(t)
		pushConnectionConfig(t, proc, configs, backendconfig.Connection{
			SourceID:      captureOptInSourceID,
			DestinationID: captureOptInDestID,
			Enabled:       true,
			Config:        connectionConfig(t, `{"source":{"syncSettings":{"syncLogsConfig":{"enabled":true}}}}`),
		}, false)

		params := marshalledJobParams(t, proc, retlMetadata("run-1"))
		require.False(t, gjson.GetBytes(params, "capture_error").Exists(), "got: %s", params)
	})

	t.Run("unknown connection", func(t *testing.T) {
		proc, configs := newCaptureOptInHandle(t)
		pushConnectionConfig(t, proc, configs, backendconfig.Connection{
			SourceID:      "some-other-source",
			DestinationID: "some-other-destination",
			Config:        connectionConfig(t, captureOptInEnabledCfg),
		}, false)

		params := marshalledJobParams(t, proc, retlMetadata("run-1"))
		require.False(t, gjson.GetBytes(params, "capture_error").Exists(), "got: %s", params)
	})

	t.Run("non-retl traffic is never resolved nor pinned", func(t *testing.T) {
		proc, configs := newCaptureOptInHandle(t)
		pushConnectionConfig(t, proc, configs, optedInConnection(t, true), true)

		metadata := retlMetadata("")
		metadata.RecordID = nil
		params := marshalledJobParams(t, proc, metadata)

		require.False(t, gjson.GetBytes(params, "capture_error").Exists(), "got: %s", params)
		require.Zero(t, proc.captureOptInPins.size(), "an event without a job run id must not be pinned")
	})
}

// A3.3: the first resolution of a job run sticks. A config push landing mid-run must not split that
// run's records between two decisions; the next run picks the new value up.
func TestCaptureErrorPinnedPerJobRun(t *testing.T) {
	proc, configs := newCaptureOptInHandle(t)
	pushConnectionConfig(t, proc, configs, optedInConnection(t, true), true)

	first := marshalledJobParams(t, proc, retlMetadata("run-1"))
	require.True(t, gjson.GetBytes(first, "capture_error").Bool(), "got: %s", first)

	// the connection is toggled off mid-run, through the same subscriber that seeded it
	pushConnectionConfig(t, proc, configs, optedInConnection(t, false), false)

	sameRun := marshalledJobParams(t, proc, retlMetadata("run-1"))
	require.True(t, gjson.GetBytes(sameRun, "capture_error").Bool(),
		"the pinned decision must win for the rest of the run, got: %s", sameRun)

	nextRun := marshalledJobParams(t, proc, retlMetadata("run-2"))
	require.False(t, gjson.GetBytes(nextRun, "capture_error").Exists(),
		"a new run must resolve against the current config, got: %s", nextRun)

	// and the reverse direction: a run started while opted out stays opted out
	pushConnectionConfig(t, proc, configs, optedInConnection(t, true), true)
	stillOff := marshalledJobParams(t, proc, retlMetadata("run-2"))
	require.False(t, gjson.GetBytes(stillOff, "capture_error").Exists(),
		"a run pinned to false must not start capturing mid-run, got: %s", stillOff)
}

// A3.5, processor half: neither a forged payload field nor a forged transformer metadata field can
// induce capture - the connection's backend config is the only input to the decision.
func TestCaptureErrorForgedSignalsAreInert(t *testing.T) {
	proc, configs := newCaptureOptInHandle(t)
	pushConnectionConfig(t, proc, configs, optedInConnection(t, false), false)

	// a transformer response echoing a capture flag: Metadata has no such field any more, so the
	// key is dropped on unmarshal and cannot reach the marshal site
	var forged types.Metadata
	require.NoError(t, jsonrs.Unmarshal([]byte(fmt.Sprintf(
		`{"sourceId":%q,"destinationId":%q,"sourceJobRunId":"run-1","captureError":true,"capture_error":true}`,
		captureOptInSourceID, captureOptInDestID,
	)), &forged))

	out := proc.destTransform(context.Background(), userTransformAndFilterOutput{
		eventsToTransform: []types.TransformerEvent{{}},
		commonMetaData: &types.Metadata{
			SourceID:      captureOptInSourceID,
			DestinationID: captureOptInDestID,
		},
		procErrorJobsByDestID: map[string][]procErrorJob{},
		transformAt:           "router",
		response: types.Response{
			Events: []types.TransformerResponse{{
				// a payload field a client could have sent, and a mapped rETL column that
				// happens to be named capture_error
				Output: map[string]any{
					"context":       map[string]any{"sources": map[string]any{"capture_error": true}},
					"capture_error": true,
				},
				Metadata: forged,
			}},
		},
	})
	require.Len(t, out.destJobs, 1)

	params := out.destJobs[0].Parameters
	require.False(t, gjson.GetBytes(params, "capture_error").Exists(),
		"a forged signal must not produce capture params, got: %s", params)
	require.True(t, gjson.GetBytes(out.destJobs[0].EventPayload, "context.sources.capture_error").Bool(),
		"the forged payload field is inert, not stripped")
}
