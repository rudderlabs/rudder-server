package router

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"

	"github.com/rudderlabs/rudder-server/jobsdb"
	mockjobsdb "github.com/rudderlabs/rudder-server/mocks/jobsdb"
	mockfeatures "github.com/rudderlabs/rudder-server/mocks/services/transformer"
	mocktypes "github.com/rudderlabs/rudder-server/mocks/utils/types"
	"github.com/rudderlabs/rudder-server/router/throttler"
	routerutils "github.com/rudderlabs/rudder-server/router/utils"
	"github.com/rudderlabs/rudder-server/services/rsources"
	"github.com/rudderlabs/rudder-server/services/transientsource"
	txutils "github.com/rudderlabs/rudder-server/utils/tx"
	utilTypes "github.com/rudderlabs/rudder-server/utils/types"
)

const reportingSampleEventMaskingMetric = "router_reporting_sample_event_masking"

func TestMaskReportingSampleEvent(t *testing.T) {
	t.Run("flag off leaves payload byte-identical", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		features := mockfeatures.NewMockFeaturesService(ctrl)
		rt, statsStore := newReportingSampleEventMaskingTestHandle(t, features, false)
		payload := json.RawMessage("{ \"headers\" : { \"Authorization\" : \"secret\" } }")

		masked := rt.maskReportingSampleEvent(payload)

		require.Equal(t, payload, masked)
		require.Empty(t, maskingCounts(statsStore, reportingSampleEventMaskingMetric))
	})

	for _, tc := range []struct {
		name       string
		paths      []string
		ok         bool
		input      string
		want       string
		wantReason string
	}{
		{
			name:       "missing secret paths leaves payload unchanged",
			ok:         false,
			input:      `{ "headers" : { "Authorization" : "secret" } }`,
			want:       `{ "headers" : { "Authorization" : "secret" } }`,
			wantReason: "no_paths",
		},
		{
			name:       "empty secret paths leave payload unchanged",
			paths:      []string{},
			ok:         true,
			input:      `{ "headers" : { "Authorization" : "secret" } }`,
			want:       `{ "headers" : { "Authorization" : "secret" } }`,
			wantReason: "listed_noop",
		},
		{
			name:       "array with missing secret paths stays unchanged",
			ok:         false,
			input:      `[ { "headers": { "Authorization": "secret" } } ]`,
			want:       `[ { "headers": { "Authorization": "secret" } } ]`,
			wantReason: "no_paths",
		},
		{
			name:       "object array with empty secret paths stays unchanged",
			paths:      []string{},
			ok:         true,
			input:      `[ { "headers": { "Authorization": "secret" } } ]`,
			want:       `[ { "headers": { "Authorization": "secret" } } ]`,
			wantReason: "listed_noop",
		},
		{
			name:       "empty secret paths leave non-object array unchanged",
			paths:      []string{},
			ok:         true,
			input:      `[null]`,
			want:       `[null]`,
			wantReason: "listed_noop",
		},
		{
			name:       "non-object payload is never blanket masked",
			paths:      []string{"headers.Authorization"},
			ok:         true,
			input:      `["secret"]`,
			want:       `["secret"]`,
			wantReason: "not_object",
		},
		{
			name:       "listed path is masked with siblings intact",
			paths:      []string{"headers.Authorization"},
			ok:         true,
			input:      `{"endpoint":"visible","headers":{"Authorization":"secret","Content-Type":"application/json"},"body":{"name":"Ada"}}`,
			want:       `{"endpoint":"visible","headers":{"Authorization":"******","Content-Type":"application/json"},"body":{"name":"Ada"}}`,
			wantReason: "listed",
		},
		{
			name:       "listed path is masked in every request of an array",
			paths:      []string{"headers.Authorization"},
			ok:         true,
			input:      `[{"endpoint":"first","headers":{"Authorization":"Bearer first","Content-Type":"application/json"},"body":{"name":"Ada"}},{"endpoint":"second","headers":{"Authorization":"Bearer second","Content-Type":"text/plain"},"body":{"name":"Bob"}}]`,
			want:       `[{"endpoint":"first","headers":{"Authorization":"******","Content-Type":"application/json"},"body":{"name":"Ada"}},{"endpoint":"second","headers":{"Authorization":"******","Content-Type":"text/plain"},"body":{"name":"Bob"}}]`,
			wantReason: "listed",
		},
		{
			name:       "non-object array element does not stop object elements being masked",
			paths:      []string{"headers.Authorization"},
			ok:         true,
			input:      `[ {"headers":{"Authorization":"secret"}}, "scalar-secret" ]`,
			want:       `[{"headers":{"Authorization":"******"}},"scalar-secret"]`,
			wantReason: "not_object",
		},
		{
			name:       "empty array is a no-op",
			paths:      []string{"headers.Authorization"},
			ok:         true,
			input:      `[ ]`,
			want:       `[ ]`,
			wantReason: "listed_noop",
		},
		{
			name:       "missing listed path in array leaves bytes unchanged",
			paths:      []string{"params.api_key"},
			ok:         true,
			input:      `[ {"headers":{"Authorization":"secret"}} , {"body":{"name":"Ada"}} ]`,
			want:       `[ {"headers":{"Authorization":"secret"}} , {"body":{"name":"Ada"}} ]`,
			wantReason: "listed_noop",
		},
		{
			name:       "unsettable array path still masks every settable path",
			paths:      []string{"headers.Authorization", "body.token|@reverse"},
			ok:         true,
			input:      `[{"headers":{"Authorization":"first"}},{"headers":{"Authorization":"second"},"body":{"token":"secret"}}]`,
			want:       `[{"headers":{"Authorization":"******"}},{"headers":{"Authorization":"******"},"body":{"token":"secret"}}]`,
			wantReason: "partial",
		},
		{
			name:       "invalid JSON keeps the non-object reason",
			paths:      []string{"headers.Authorization"},
			ok:         true,
			input:      `[{"headers":{"Authorization":"secret"}`,
			want:       `[{"headers":{"Authorization":"secret"}`,
			wantReason: "not_object",
		},
		{
			name:       "absent listed path leaves payload unchanged",
			paths:      []string{"params.api_key"},
			ok:         true,
			input:      `{ "headers" : { "Authorization" : "secret" } }`,
			want:       `{ "headers" : { "Authorization" : "secret" } }`,
			wantReason: "listed_noop",
		},
		{
			name:       "unsettable path retains successful masks",
			paths:      []string{"headers.Authorization", "", "body.token|@reverse"},
			ok:         true,
			input:      `{"headers":{"Authorization":"secret"},"body":{"token":"also-secret","name":"Ada"}}`,
			want:       `{"headers":{"Authorization":"******"},"body":{"token":"also-secret","name":"Ada"}}`,
			wantReason: "partial",
		},
		{
			name:       "pre-transform event has no delivery-shaped secrets",
			paths:      []string{"headers.Authorization", "params.api_key", "body.access_token", "payload.apiKey"},
			ok:         true,
			input:      `{ "type" : "track", "event" : "Order Completed", "userId" : "user-1", "properties" : { "order_id" : "order-1" } }`,
			want:       `{ "type" : "track", "event" : "Order Completed", "userId" : "user-1", "properties" : { "order_id" : "order-1" } }`,
			wantReason: "listed_noop",
		},
		{
			name:       "endpoint is never masked",
			paths:      []string{"endpoint"},
			ok:         true,
			input:      `{ "endpoint" : "https://example.test/path?token=visible" }`,
			want:       `{ "endpoint" : "https://example.test/path?token=visible" }`,
			wantReason: "listed_noop",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			features := mockfeatures.NewMockFeaturesService(ctrl)
			features.EXPECT().SecretPaths("TEST_DEST").Return(tc.paths, tc.ok)
			rt, statsStore := newReportingSampleEventMaskingTestHandle(t, features, true)
			payload := json.RawMessage(tc.input)

			masked := rt.maskReportingSampleEvent(payload)

			require.Equal(t, tc.want, string(masked))
			require.Equal(t, tc.input, string(payload), "caller payload must not be modified")
			require.Equal(t, map[string]float64{tc.wantReason: 1}, maskingCounts(statsStore, reportingSampleEventMaskingMetric))
		})
	}
}

func TestCommitStatusListReportingSampleMasking(t *testing.T) {
	const (
		secretPaths = "headers.Authorization"
		first       = `{"headers":{"Authorization":"first"}}`
		second      = `{"headers":{"Authorization":"second"},"marker":"counted"}`
		maskedFirst = `{"headers":{"Authorization":"******"}}`
		maskedLast  = `{"headers":{"Authorization":"******"},"marker":"counted"}`
	)
	type statusSample struct {
		state    string
		code     string
		attempt  int
		payload  json.RawMessage
		response string
		tags     map[string]string
	}
	firstFailure := statusSample{state: jobsdb.Failed.State, code: "500", attempt: 1, payload: json.RawMessage(first), response: "first failure", tags: map[string]string{"attempt": "first"}}
	for _, tc := range []struct {
		name         string
		statuses     []statusSample
		transient    bool
		wantCount    int64
		wantSample   json.RawMessage
		wantResponse string
		wantTags     map[string]string
		wantReason   string
	}{
		{name: "waiting with nil payload", statuses: []statusSample{{state: jobsdb.Waiting.State}}},
		{name: "failed retry", statuses: []statusSample{{state: jobsdb.Failed.State, code: "500", attempt: 2, payload: json.RawMessage(first)}}},
		{name: "excluded unmarshal error", statuses: []statusSample{{state: jobsdb.Failed.State, code: "599", attempt: 1, payload: json.RawMessage(first)}}},
		{name: "first failure", statuses: []statusSample{firstFailure}, wantCount: 1, wantSample: json.RawMessage(maskedFirst), wantResponse: firstFailure.response, wantTags: firstFailure.tags, wantReason: "listed"},
		{name: "success", statuses: []statusSample{{state: jobsdb.Succeeded.State, code: "200", payload: json.RawMessage(first)}}, wantCount: 1, wantSample: json.RawMessage(maskedFirst), wantReason: "listed"},
		{name: "filtered", statuses: []statusSample{{state: jobsdb.Filtered.State, code: "200", payload: json.RawMessage(first)}}, wantCount: 1, wantSample: json.RawMessage(maskedFirst), wantReason: "listed"},
		{name: "aborted", statuses: []statusSample{{state: jobsdb.Aborted.State, code: "400", payload: json.RawMessage(first)}}, wantCount: 1, wantSample: json.RawMessage(maskedFirst), wantReason: "listed"},
		{
			name: "retry before first counted failure",
			statuses: []statusSample{
				{state: jobsdb.Failed.State, code: "500", attempt: 2, payload: json.RawMessage(`not json`), response: "retry response", tags: map[string]string{"attempt": "retry"}},
				{state: jobsdb.Failed.State, code: "500", attempt: 1, payload: json.RawMessage(second), response: "counted response", tags: map[string]string{"attempt": "counted"}},
				firstFailure,
			},
			wantCount: 2, wantSample: json.RawMessage(maskedLast), wantResponse: "counted response", wantTags: map[string]string{"attempt": "counted"}, wantReason: "listed",
		},
		{name: "reported non-object", statuses: []statusSample{{state: jobsdb.Succeeded.State, payload: json.RawMessage(`not json`)}}, wantCount: 1, wantSample: json.RawMessage(`not json`), wantReason: "not_object"},
		{name: "transient source", statuses: []statusSample{firstFailure}, transient: true, wantCount: 1, wantSample: json.RawMessage(routerutils.EmptyPayload), wantResponse: firstFailure.response, wantTags: firstFailure.tags},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			features := mockfeatures.NewMockFeaturesService(ctrl)
			if tc.wantReason != "" {
				features.EXPECT().SecretPaths("TEST_DEST").Return([]string{secretPaths}, true).Times(1)
			}
			rt, statsStore := newReportingSampleEventMaskingTestHandle(t, features, true)
			mockDB := mockjobsdb.NewMockJobsDB(ctrl)
			reporter := mocktypes.NewMockReporting(ctrl)
			rt.jobsDB = mockDB
			rt.Reporting = reporter
			rt.logger = logger.NOP
			rt.throttlerFactory = throttler.NewNoOpThrottlerFactory()
			rt.transientSources = transientsource.NewEmptyService()
			if tc.transient {
				rt.transientSources = transientsource.NewStaticService([]string{"source"})
			}
			rt.rsourcesService = rsources.NewNoOpService()
			rt.rsourcesSyncSettings = rsources.NewStaticSyncSettingDelegate("", nil)
			rt.reloadableConfig.jobsDBCommandTimeout = config.SingleValueLoader(time.Second)
			rt.reloadableConfig.jobdDBMaxRetries = config.SingleValueLoader(1)
			rt.reloadableConfig.updateStatusBatchSize = config.SingleValueLoader(10)

			var statuses []workerJobStatus
			for i, sample := range tc.statuses {
				jobID := int64(i + 1)
				statuses = append(statuses, workerJobStatus{
					job:        &jobsdb.JobT{JobID: jobID, Parameters: json.RawMessage(`{"event_name":"Order Completed","event_type":"track"}`)},
					status:     &jobsdb.JobStatusT{JobID: jobID, JobState: sample.state, ErrorCode: sample.code, AttemptNum: sample.attempt, ErrorResponse: json.RawMessage(sample.response)},
					parameters: routerutils.JobParameters{SourceID: "source", DestinationID: "destination"},
					payload:    sample.payload,
					statTags:   sample.tags,
				})
			}
			mockDB.EXPECT().WithUpdateSafeTx(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, fn func(jobsdb.UpdateSafeTx) error) error {
				return fn(jobsdb.EmptyUpdateSafeTx())
			})
			mockDB.EXPECT().UpdateJobStatusInTx(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
			reporter.EXPECT().Report(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, metrics []*utilTypes.PUReportedMetric, _ *txutils.Tx) error {
				if tc.wantCount == 0 {
					require.Empty(t, metrics)
					return nil
				}
				require.Len(t, metrics, 1)
				detail := metrics[0].StatusDetail
				require.Equal(t, tc.wantCount, detail.Count)
				require.Equal(t, tc.wantSample, detail.SampleEvent)
				require.Equal(t, tc.wantResponse, detail.SampleResponse)
				require.Equal(t, tc.wantTags, detail.StatTags)
				return nil
			})

			rt.commitStatusList(&statuses)

			if tc.wantReason == "" {
				require.Empty(t, maskingCounts(statsStore, reportingSampleEventMaskingMetric))
			} else {
				require.Equal(t, map[string]float64{tc.wantReason: 1}, maskingCounts(statsStore, reportingSampleEventMaskingMetric))
			}
		})
	}
}

func newReportingSampleEventMaskingTestHandle(
	t *testing.T,
	features *mockfeatures.MockFeaturesService,
	enabled bool,
) (*Handle, *memstats.Store) {
	t.Helper()
	statsStore, err := memstats.New()
	require.NoError(t, err)
	return &Handle{
		destType:                   "TEST_DEST",
		transformerFeaturesService: features,
		reloadableConfig: &reloadableConfig{
			enableReportingSampleEventMasking: config.SingleValueLoader(enabled),
		},
		reportingSampleEventMaskingStat: func(reason string) stats.Counter {
			return statsStore.NewTaggedStat(reportingSampleEventMaskingMetric, stats.CountType, stats.Tags{"destType": "TEST_DEST", "reason": reason})
		},
	}, statsStore
}
