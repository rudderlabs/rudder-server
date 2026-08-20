package reporting

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/enterprise/reporting/client"
	mocksBackendConfig "github.com/rudderlabs/rudder-server/mocks/backend-config"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
	utilsTx "github.com/rudderlabs/rudder-server/utils/tx"
	"github.com/rudderlabs/rudder-server/utils/types"
)

var _ = Describe("Reporting", func() {
	Context("transformMetricForPII Tests", func() {
		It("Should match transformMetricForPII response for a valid metric", func() {
			inputMetric := types.PUReportedMetric{
				ConnectionDetails: types.ConnectionDetails{
					SourceID:        "some-source-id",
					DestinationID:   "some-destination-id",
					SourceTaskRunID: "some-source-task-run-id",
					SourceJobID:     "some-source-job-id",
					SourceJobRunID:  "some-source-job-run-id",
				},
				PUDetails: types.PUDetails{
					InPU:       "some-in-pu",
					PU:         "some-pu",
					TerminalPU: false,
					InitialPU:  false,
				},
				StatusDetail: &types.StatusDetail{
					Status:         "some-status",
					Count:          3,
					StatusCode:     0,
					SampleResponse: `{"some-sample-response-key": "some-sample-response-value"}`,
					SampleEvent:    []byte(`{"some-sample-event-key": "some-sample-event-value"}`),
					EventName:      "some-event-name",
					EventType:      "some-event-type",
				},
			}

			expectedResponse := types.PUReportedMetric{
				ConnectionDetails: types.ConnectionDetails{
					SourceID:        "some-source-id",
					DestinationID:   "some-destination-id",
					SourceTaskRunID: "some-source-task-run-id",
					SourceJobID:     "some-source-job-id",
					SourceJobRunID:  "some-source-job-run-id",
				},
				PUDetails: types.PUDetails{
					InPU:       "some-in-pu",
					PU:         "some-pu",
					TerminalPU: false,
					InitialPU:  false,
				},
				StatusDetail: &types.StatusDetail{
					Status:         "some-status",
					Count:          3,
					StatusCode:     0,
					SampleResponse: "",
					SampleEvent:    nil,
					EventName:      "",
					EventType:      "",
				},
			}

			piiColumnsToExclude := []string{"sample_response", "sample_event", "event_name", "event_type"}
			transformedMetric := transformMetricForPII(inputMetric, piiColumnsToExclude)
			assertReportMetric(expectedResponse, transformedMetric)
		})
	})
})

func assertReportMetric(expectedMetric, actualMetric types.PUReportedMetric) {
	Expect(expectedMetric.SourceID).To(Equal(actualMetric.SourceID))
	Expect(expectedMetric.DestinationID).To(Equal(actualMetric.DestinationID))
	Expect(expectedMetric.SourceJobID).To(Equal(actualMetric.SourceJobID))
	Expect(expectedMetric.SourceJobRunID).To(Equal(actualMetric.SourceJobRunID))
	Expect(expectedMetric.SourceTaskRunID).To(Equal(actualMetric.SourceTaskRunID))
	Expect(expectedMetric.InPU).To(Equal(actualMetric.InPU))
	Expect(expectedMetric.PU).To(Equal(actualMetric.PU))
	Expect(expectedMetric.TerminalPU).To(Equal(actualMetric.TerminalPU))
	Expect(expectedMetric.InitialPU).To(Equal(actualMetric.InitialPU))
	Expect(expectedMetric.StatusDetail.Status).To(Equal(actualMetric.StatusDetail.Status))
	Expect(expectedMetric.StatusDetail.StatusCode).To(Equal(actualMetric.StatusDetail.StatusCode))
	Expect(expectedMetric.StatusDetail.Count).To(Equal(actualMetric.StatusDetail.Count))
	Expect(expectedMetric.StatusDetail.SampleResponse).To(Equal(actualMetric.StatusDetail.SampleResponse))
	Expect(expectedMetric.StatusDetail.SampleEvent).To(Equal(actualMetric.StatusDetail.SampleEvent))
	Expect(expectedMetric.StatusDetail.EventName).To(Equal(actualMetric.StatusDetail.EventName))
	Expect(expectedMetric.StatusDetail.EventType).To(Equal(actualMetric.StatusDetail.EventType))
}

func TestGetAggregatedReports(t *testing.T) {
	inputReports := []*types.ReportByStatus{
		{
			InstanceDetails: types.InstanceDetails{
				WorkspaceID: "some-workspace-id",
			},
			ConnectionDetails: types.ConnectionDetails{
				SourceID:         "some-source-id",
				DestinationID:    "some-destination-id",
				TransformationID: "some-transformation-id",
				TrackingPlanID:   "some-tracking-plan-id",
			},
			PUDetails: types.PUDetails{
				InPU: "some-in-pu",
				PU:   "some-pu",
			},
			ReportMetadata: types.ReportMetadata{
				ReportedAt: 28017690,
			},
			StatusDetail: &types.StatusDetail{
				Status:         "some-status",
				Count:          3,
				ViolationCount: 5,
				StatusCode:     200,
				SampleResponse: "",
				SampleEvent:    []byte(`{}`),
				ErrorType:      "",
			},
		},
		{
			InstanceDetails: types.InstanceDetails{
				WorkspaceID: "some-workspace-id",
			},
			ConnectionDetails: types.ConnectionDetails{
				SourceID:         "some-source-id",
				DestinationID:    "some-destination-id",
				TransformationID: "some-transformation-id",
				TrackingPlanID:   "some-tracking-plan-id",
			},
			PUDetails: types.PUDetails{
				InPU: "some-in-pu",
				PU:   "some-pu",
			},
			ReportMetadata: types.ReportMetadata{
				ReportedAt: 28017690,
			},
			StatusDetail: &types.StatusDetail{
				Status:         "some-status",
				Count:          2,
				ViolationCount: 10,
				StatusCode:     200,
				SampleResponse: "",
				SampleEvent:    []byte(`{}`),
				ErrorType:      "some-error-type",
			},
		},
		{
			InstanceDetails: types.InstanceDetails{
				WorkspaceID: "some-workspace-id",
			},
			ConnectionDetails: types.ConnectionDetails{
				SourceID:         "some-source-id-2",
				DestinationID:    "some-destination-id",
				TransformationID: "some-transformation-id",
				TrackingPlanID:   "some-tracking-plan-id",
			},
			PUDetails: types.PUDetails{
				InPU: "some-in-pu",
				PU:   "some-pu",
			},
			ReportMetadata: types.ReportMetadata{
				ReportedAt: 28017690,
			},
			StatusDetail: &types.StatusDetail{
				Status:         "some-status",
				Count:          3,
				ViolationCount: 10,
				StatusCode:     200,
				SampleResponse: "",
				SampleEvent:    []byte(`{}`),
				ErrorType:      "some-error-type",
			},
		},
	}
	conf := config.New()
	conf.Set("Reporting.eventSampling.durationInMinutes", 10)
	configSubscriber := newConfigSubscriber(logger.NOP)
	reportHandle := NewDefaultReporter(context.Background(), conf, logger.NOP, configSubscriber, stats.NOP)

	t.Run("Should provide aggregated reports when batch size is 1", func(t *testing.T) {
		conf.Set("Reporting.maxReportsCountInARequest", 1)
		assert.Equal(t, 1, reportHandle.maxReportsCountInARequest.Load())
		bucket, _ := GetAggregationBucketMinute(28017690, 10)
		expectedResponse := []*types.Metric{
			{
				InstanceDetails: types.InstanceDetails{
					WorkspaceID: "some-workspace-id",
				},
				ConnectionDetails: types.ConnectionDetails{
					SourceID:         "some-source-id",
					DestinationID:    "some-destination-id",
					TransformationID: "some-transformation-id",
					TrackingPlanID:   "some-tracking-plan-id",
				},
				PUDetails: types.PUDetails{
					InPU: "some-in-pu",
					PU:   "some-pu",
				},
				ReportMetadata: types.ReportMetadata{
					ReportedAt:        28017690 * 60 * 1000,
					SampleEventBucket: bucket * 60 * 1000,
				},
				StatusDetails: []*types.StatusDetail{
					{
						Status:         "some-status",
						Count:          3,
						ViolationCount: 5,
						StatusCode:     200,
						SampleResponse: "",
						SampleEvent:    []byte(`{}`),
						ErrorType:      "",
					},
				},
			},
			{
				InstanceDetails: types.InstanceDetails{
					WorkspaceID: "some-workspace-id",
				},
				ConnectionDetails: types.ConnectionDetails{
					SourceID:         "some-source-id",
					DestinationID:    "some-destination-id",
					TransformationID: "some-transformation-id",
					TrackingPlanID:   "some-tracking-plan-id",
				},
				PUDetails: types.PUDetails{
					InPU: "some-in-pu",
					PU:   "some-pu",
				},
				ReportMetadata: types.ReportMetadata{
					ReportedAt:        28017690 * 60 * 1000,
					SampleEventBucket: bucket * 60 * 1000,
				},
				StatusDetails: []*types.StatusDetail{
					{
						Status:         "some-status",
						Count:          2,
						ViolationCount: 10,
						StatusCode:     200,
						SampleResponse: "",
						SampleEvent:    []byte(`{}`),
						ErrorType:      "some-error-type",
					},
				},
			},
			{
				InstanceDetails: types.InstanceDetails{
					WorkspaceID: "some-workspace-id",
				},
				ConnectionDetails: types.ConnectionDetails{
					SourceID:         "some-source-id-2",
					DestinationID:    "some-destination-id",
					TransformationID: "some-transformation-id",
					TrackingPlanID:   "some-tracking-plan-id",
				},
				PUDetails: types.PUDetails{
					InPU: "some-in-pu",
					PU:   "some-pu",
				},
				ReportMetadata: types.ReportMetadata{
					ReportedAt:        28017690 * 60 * 1000,
					SampleEventBucket: bucket * 60 * 1000,
				},
				StatusDetails: []*types.StatusDetail{
					{
						Status:         "some-status",
						Count:          3,
						ViolationCount: 10,
						StatusCode:     200,
						SampleResponse: "",
						SampleEvent:    []byte(`{}`),
						ErrorType:      "some-error-type",
					},
				},
			},
		}

		aggregatedMetrics := reportHandle.getAggregatedReports(inputReports)
		assert.Equal(t, expectedResponse, aggregatedMetrics)
	})

	t.Run("Should provide aggregated reports when batch size more than 1", func(t *testing.T) {
		conf.Set("Reporting.maxReportsCountInARequest", 10)
		assert.Equal(t, 10, reportHandle.maxReportsCountInARequest.Load())
		bucket, _ := GetAggregationBucketMinute(28017690, 10)
		expectedResponse := []*types.Metric{
			{
				InstanceDetails: types.InstanceDetails{
					WorkspaceID: "some-workspace-id",
				},
				ConnectionDetails: types.ConnectionDetails{
					SourceID:         "some-source-id",
					DestinationID:    "some-destination-id",
					TransformationID: "some-transformation-id",
					TrackingPlanID:   "some-tracking-plan-id",
				},
				PUDetails: types.PUDetails{
					InPU: "some-in-pu",
					PU:   "some-pu",
				},
				ReportMetadata: types.ReportMetadata{
					ReportedAt:        28017690 * 60 * 1000,
					SampleEventBucket: bucket * 60 * 1000,
				},
				StatusDetails: []*types.StatusDetail{
					{
						Status:         "some-status",
						Count:          3,
						ViolationCount: 5,
						StatusCode:     200,
						SampleResponse: "",
						SampleEvent:    []byte(`{}`),
						ErrorType:      "",
					},
					{
						Status:         "some-status",
						Count:          2,
						ViolationCount: 10,
						StatusCode:     200,
						SampleResponse: "",
						SampleEvent:    []byte(`{}`),
						ErrorType:      "some-error-type",
					},
				},
			},
			{
				InstanceDetails: types.InstanceDetails{
					WorkspaceID: "some-workspace-id",
				},
				ConnectionDetails: types.ConnectionDetails{
					SourceID:         "some-source-id-2",
					DestinationID:    "some-destination-id",
					TransformationID: "some-transformation-id",
					TrackingPlanID:   "some-tracking-plan-id",
				},
				PUDetails: types.PUDetails{
					InPU: "some-in-pu",
					PU:   "some-pu",
				},
				ReportMetadata: types.ReportMetadata{
					ReportedAt:        28017690 * 60 * 1000,
					SampleEventBucket: bucket * 60 * 1000,
				},
				StatusDetails: []*types.StatusDetail{
					{
						Status:         "some-status",
						Count:          3,
						ViolationCount: 10,
						StatusCode:     200,
						SampleResponse: "",
						SampleEvent:    []byte(`{}`),
						ErrorType:      "some-error-type",
					},
				},
			},
		}

		aggregatedMetrics := reportHandle.getAggregatedReports(inputReports)
		assert.Equal(t, expectedResponse, aggregatedMetrics)
	})

	t.Run("Should provide aggregated reports when batch size is more than 1 and reports with same identifier are more then batch size", func(t *testing.T) {
		conf.Set("Reporting.maxReportsCountInARequest", 2)
		assert.Equal(t, 2, reportHandle.maxReportsCountInARequest.Load())
		bucket, _ := GetAggregationBucketMinute(28017690, 10)
		extraReport := &types.ReportByStatus{
			InstanceDetails: types.InstanceDetails{
				WorkspaceID: "some-workspace-id",
			},
			ConnectionDetails: types.ConnectionDetails{
				SourceID:         "some-source-id",
				DestinationID:    "some-destination-id",
				TransformationID: "some-transformation-id",
				TrackingPlanID:   "some-tracking-plan-id",
			},
			PUDetails: types.PUDetails{
				InPU: "some-in-pu",
				PU:   "some-pu",
			},
			ReportMetadata: types.ReportMetadata{
				ReportedAt: 28017690,
			},
			StatusDetail: &types.StatusDetail{
				Status:         "some-status",
				Count:          2,
				ViolationCount: 10,
				StatusCode:     200,
				SampleResponse: "",
				SampleEvent:    []byte(`{}`),
				ErrorType:      "another-error-type",
			},
		}
		newInputReports := append(inputReports, extraReport)
		expectedResponse := []*types.Metric{
			{
				InstanceDetails: types.InstanceDetails{
					WorkspaceID: "some-workspace-id",
				},
				ConnectionDetails: types.ConnectionDetails{
					SourceID:         "some-source-id",
					DestinationID:    "some-destination-id",
					TransformationID: "some-transformation-id",
					TrackingPlanID:   "some-tracking-plan-id",
				},
				PUDetails: types.PUDetails{
					InPU: "some-in-pu",
					PU:   "some-pu",
				},
				ReportMetadata: types.ReportMetadata{
					ReportedAt:        28017690 * 60 * 1000,
					SampleEventBucket: bucket * 60 * 1000,
				},
				StatusDetails: []*types.StatusDetail{
					{
						Status:         "some-status",
						Count:          3,
						ViolationCount: 5,
						StatusCode:     200,
						SampleResponse: "",
						SampleEvent:    []byte(`{}`),
						ErrorType:      "",
					},
					{
						Status:         "some-status",
						Count:          2,
						ViolationCount: 10,
						StatusCode:     200,
						SampleResponse: "",
						SampleEvent:    []byte(`{}`),
						ErrorType:      "some-error-type",
					},
				},
			},
			{
				InstanceDetails: types.InstanceDetails{
					WorkspaceID: "some-workspace-id",
				},
				ConnectionDetails: types.ConnectionDetails{
					SourceID:         "some-source-id-2",
					DestinationID:    "some-destination-id",
					TransformationID: "some-transformation-id",
					TrackingPlanID:   "some-tracking-plan-id",
				},
				PUDetails: types.PUDetails{
					InPU: "some-in-pu",
					PU:   "some-pu",
				},
				ReportMetadata: types.ReportMetadata{
					ReportedAt:        28017690 * 60 * 1000,
					SampleEventBucket: bucket * 60 * 1000,
				},
				StatusDetails: []*types.StatusDetail{
					{
						Status:         "some-status",
						Count:          3,
						ViolationCount: 10,
						StatusCode:     200,
						SampleResponse: "",
						SampleEvent:    []byte(`{}`),
						ErrorType:      "some-error-type",
					},
				},
			},
			{
				InstanceDetails: types.InstanceDetails{
					WorkspaceID: "some-workspace-id",
				},
				ConnectionDetails: types.ConnectionDetails{
					SourceID:         "some-source-id",
					DestinationID:    "some-destination-id",
					TransformationID: "some-transformation-id",
					TrackingPlanID:   "some-tracking-plan-id",
				},
				PUDetails: types.PUDetails{
					InPU: "some-in-pu",
					PU:   "some-pu",
				},
				ReportMetadata: types.ReportMetadata{
					ReportedAt:        28017690 * 60 * 1000,
					SampleEventBucket: bucket * 60 * 1000,
				},
				StatusDetails: []*types.StatusDetail{
					{
						Status:         "some-status",
						Count:          2,
						ViolationCount: 10,
						StatusCode:     200,
						SampleResponse: "",
						SampleEvent:    []byte(`{}`),
						ErrorType:      "another-error-type",
					},
				},
			},
		}

		aggregatedMetrics := reportHandle.getAggregatedReports(newInputReports)
		assert.Equal(t, expectedResponse, aggregatedMetrics)
	})
}

func TestDefaultReporter_Report_EventNameTrimming(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name              string
		eventName         string
		prefixLength      int
		suffixLength      int
		expectedEventName string
	}{
		{
			name:              "short event name - no trimming",
			eventName:         "track",
			prefixLength:      40,
			suffixLength:      10,
			expectedEventName: "track",
		},
		{
			name:              "long event name - default config",
			eventName:         "very_long_event_name_that_exceeds_the_maximum_length_limit_and_should_be_trimmed",
			prefixLength:      40,
			suffixLength:      10,
			expectedEventName: "very_long_event_name_that_exceeds_the_ma...be_trimmed",
		},
		{
			name:              "custom config - smaller limits",
			eventName:         "another_very_long_event_name_for_testing",
			prefixLength:      15,
			suffixLength:      5,
			expectedEventName: "another_very_lo...sting",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// Setup database mock
			db, dbMock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()

			dbMock.ExpectBegin()
			tx, _ := db.Begin()
			mockTx := &utilsTx.Tx{Tx: tx}

			// Setup mock backend config
			mockCtrl := gomock.NewController(t)
			defer mockCtrl.Finish()
			mockBackendConfig := mocksBackendConfig.NewMockBackendConfig(mockCtrl)

			workspaceID := "test-workspace"
			sourceID := "test-source"
			destinationID := "test-destination"

			mockBackendConfig.EXPECT().Subscribe(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, topic backendconfig.Topic) pubsub.DataChannel {
				ch := make(chan pubsub.DataEvent, 1)
				ch <- pubsub.DataEvent{
					Data: map[string]backendconfig.ConfigT{
						workspaceID: {
							WorkspaceID: workspaceID,
							Sources: []backendconfig.SourceT{
								{
									ID:      sourceID,
									Enabled: true,
									Destinations: []backendconfig.DestinationT{
										{
											ID:      destinationID,
											Enabled: true,
											DestinationDefinition: backendconfig.DestinationDefinitionT{
												ID:   "dest-def-id",
												Name: "dest-type",
											},
										},
									},
								},
							},
							Settings: backendconfig.Settings{
								DataRetention: backendconfig.DataRetention{
									DisableReportingPII: false,
								},
							},
						},
					},
					Topic: string(backendconfig.TopicBackendConfig),
				}
				close(ch)
				return ch
			}).AnyTimes()

			// Setup config subscriber
			configSubscriber := newConfigSubscriber(logger.NOP)
			go configSubscriber.Subscribe(context.Background(), mockBackendConfig)
			configSubscriber.Wait()

			// Setup config with test values
			conf := config.New()
			conf.Set("Reporting.eventNameTrimming.prefixLength", tc.prefixLength)
			conf.Set("Reporting.eventNameTrimming.suffixLength", tc.suffixLength)

			ctx := context.Background()
			log := logger.NOP
			stats := stats.NOP

			reporter := NewDefaultReporter(ctx, conf, log, configSubscriber, stats)

			// Create test metric
			metric := &types.PUReportedMetric{
				ConnectionDetails: types.ConnectionDetails{
					SourceID:      sourceID,
					DestinationID: destinationID,
				},
				StatusDetail: &types.StatusDetail{
					EventName: tc.eventName,
					EventType: "track",
					Status:    "success",
					Count:     1,
				},
			}

			// Expect the COPY statement with all required fields
			copyStmt := dbMock.ExpectPrepare(`COPY "reports" \("workspace_id", "namespace", "instance_id", "source_definition_id", "source_category", "source_id", "destination_definition_id", "destination_id", "source_task_run_id", "source_job_id", "source_job_run_id", "transformation_id", "transformation_version_id", "tracking_plan_id", "tracking_plan_version", "in_pu", "pu", "reported_at", "status", "count", "violation_count", "terminal_state", "initial_state", "status_code", "sample_response", "sample_event", "event_name", "event_type", "error_type"\) FROM STDIN`)

			copyStmt.ExpectExec().WithArgs(
				workspaceID,          // workspace_id
				sqlmock.AnyArg(),     // namespace
				sqlmock.AnyArg(),     // instance_id
				sqlmock.AnyArg(),     // source_definition_id
				sqlmock.AnyArg(),     // source_category
				sourceID,             // source_id
				sqlmock.AnyArg(),     // destination_definition_id
				destinationID,        // destination_id
				sqlmock.AnyArg(),     // source_task_run_id
				sqlmock.AnyArg(),     // source_job_id
				sqlmock.AnyArg(),     // source_job_run_id
				sqlmock.AnyArg(),     // transformation_id
				sqlmock.AnyArg(),     // transformation_version_id
				sqlmock.AnyArg(),     // tracking_plan_id
				sqlmock.AnyArg(),     // tracking_plan_version
				sqlmock.AnyArg(),     // in_pu
				sqlmock.AnyArg(),     // pu
				sqlmock.AnyArg(),     // reported_at
				"success",            // status
				1,                    // count
				sqlmock.AnyArg(),     // violation_count
				sqlmock.AnyArg(),     // terminal_state
				sqlmock.AnyArg(),     // initial_state
				sqlmock.AnyArg(),     // status_code
				sqlmock.AnyArg(),     // sample_response
				sqlmock.AnyArg(),     // sample_event
				tc.expectedEventName, // event_name (this is what we're testing)
				"track",              // event_type
				sqlmock.AnyArg(),     // error_type
			).WillReturnResult(sqlmock.NewResult(0, 1))

			// Expect the final ExecContext call to finalize the COPY statement
			copyStmt.ExpectExec().WithoutArgs().WillReturnResult(sqlmock.NewResult(0, 0))
			copyStmt.WillBeClosed()

			// Execute the report
			err = reporter.Report(ctx, []*types.PUReportedMetric{metric}, mockTx)
			require.NoError(t, err)

			// Verify all expectations were met
			err = dbMock.ExpectationsWereMet()
			require.NoError(t, err)
		})
	}
}

func TestDefaultReporter_Report_EventNameTrimming_InvalidConfig(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name         string
		eventName    string
		prefixLength int
		suffixLength int
		expectedErr  string
	}{
		{
			name:         "zero prefixLength",
			eventName:    "long_event_name_that_should_cause_error",
			prefixLength: 0,
			suffixLength: 30,
			expectedErr:  "invalid event name trimming configuration prefixLength and suffixLength must be > 0",
		},
		{
			name:         "zero suffixLength",
			eventName:    "long_event_name_that_should_cause_error",
			prefixLength: 30,
			suffixLength: 0,
			expectedErr:  "invalid event name trimming configuration prefixLength and suffixLength must be > 0",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// Setup database mock
			db, dbMock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()

			dbMock.ExpectBegin()
			tx, _ := db.Begin()
			mockTx := &utilsTx.Tx{Tx: tx}

			// Setup mock backend config
			mockCtrl := gomock.NewController(t)
			defer mockCtrl.Finish()
			mockBackendConfig := mocksBackendConfig.NewMockBackendConfig(mockCtrl)

			workspaceID := "test-workspace"
			sourceID := "test-source"
			destinationID := "test-destination"

			mockBackendConfig.EXPECT().Subscribe(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, topic backendconfig.Topic) pubsub.DataChannel {
				ch := make(chan pubsub.DataEvent, 1)
				ch <- pubsub.DataEvent{
					Data: map[string]backendconfig.ConfigT{
						workspaceID: {
							WorkspaceID: workspaceID,
							Sources: []backendconfig.SourceT{
								{
									ID:      sourceID,
									Enabled: true,
									Destinations: []backendconfig.DestinationT{
										{
											ID:      destinationID,
											Enabled: true,
											DestinationDefinition: backendconfig.DestinationDefinitionT{
												ID:   "dest-def-id",
												Name: "dest-type",
											},
										},
									},
								},
							},
							Settings: backendconfig.Settings{
								DataRetention: backendconfig.DataRetention{
									DisableReportingPII: false,
								},
							},
						},
					},
					Topic: string(backendconfig.TopicBackendConfig),
				}
				close(ch)
				return ch
			}).AnyTimes()

			// Setup config subscriber
			configSubscriber := newConfigSubscriber(logger.NOP)
			go configSubscriber.Subscribe(context.Background(), mockBackendConfig)
			configSubscriber.Wait()

			// Setup config with test values
			conf := config.New()
			conf.Set("Reporting.eventNameTrimming.prefixLength", tc.prefixLength)
			conf.Set("Reporting.eventNameTrimming.suffixLength", tc.suffixLength)

			ctx := context.Background()
			log := logger.NOP
			stats := stats.NOP

			reporter := NewDefaultReporter(ctx, conf, log, configSubscriber, stats)

			// Create test metric
			metric := &types.PUReportedMetric{
				ConnectionDetails: types.ConnectionDetails{
					SourceID:      sourceID,
					DestinationID: destinationID,
				},
				StatusDetail: &types.StatusDetail{
					EventName: tc.eventName,
					EventType: "track",
					Status:    "success",
					Count:     1,
				},
			}

			// Execute the report and expect an error
			err = reporter.Report(ctx, []*types.PUReportedMetric{metric}, mockTx)
			require.Error(t, err)
			require.EqualError(t, err, tc.expectedErr)

			// Since error occurs before database operations, no expectations should be set
			// Just verify that no unexpected calls were made
			err = dbMock.ExpectationsWereMet()
			require.NoError(t, err)
		})
	}
}

func TestSendMetricWithPayloadTooLargeSplit(t *testing.T) {
	t.Run("splits batch and sends individual metrics", func(t *testing.T) {
		var mu sync.Mutex
		var payloads []types.Metric
		requestCount := 0
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			defer mu.Unlock()
			requestCount++
			body, err := io.ReadAll(r.Body)
			require.NoError(t, err)
			var payload types.Metric
			require.NoError(t, jsonrs.Unmarshal(body, &payload))
			payloads = append(payloads, payload)

			if requestCount == 1 {
				w.WriteHeader(http.StatusRequestEntityTooLarge)
				return
			}
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		metricClient := newPayloadTooLargeTestClient(t, server.URL, 0)
		err := sendMetricWithPayloadTooLargeSplit(context.Background(), metricClient, testPayloadTooLargeMetric())
		require.NoError(t, err)

		mu.Lock()
		defer mu.Unlock()
		require.Equal(t, 3, requestCount)
		require.Len(t, payloads[0].StatusDetails, 2)
		require.Len(t, payloads[1].StatusDetails, 1)
		require.Equal(t, "event-1", payloads[1].StatusDetails[0].EventName)
		require.Len(t, payloads[2].StatusDetails, 1)
		require.Equal(t, "event-2", payloads[2].StatusDetails[0].EventName)
	})

	t.Run("replaces individual oversized sample event and retries with fallback", func(t *testing.T) {
		var mu sync.Mutex
		var payloads []types.Metric
		requestCount := 0
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			defer mu.Unlock()
			requestCount++
			body, err := io.ReadAll(r.Body)
			require.NoError(t, err)
			var payload types.Metric
			require.NoError(t, jsonrs.Unmarshal(body, &payload))
			payloads = append(payloads, payload)

			if requestCount <= 2 {
				w.WriteHeader(http.StatusRequestEntityTooLarge)
				return
			}
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		metricClient := newPayloadTooLargeTestClient(t, server.URL, 0)
		metric := testPayloadTooLargeMetric()
		err := sendMetricWithPayloadTooLargeSplit(context.Background(), metricClient, metric)
		require.NoError(t, err)

		mu.Lock()
		defer mu.Unlock()
		// batch (413) + event-1 (413) + event-1 stripped (200) + event-2 (200)
		require.Equal(t, 4, requestCount)
		require.Equal(t, "event-1", payloads[1].StatusDetails[0].EventName)
		require.JSONEq(t, `{"event":"sample-1"}`, string(payloads[1].StatusDetails[0].SampleEvent))
		require.Equal(t, "event-1", payloads[2].StatusDetails[0].EventName)
		require.JSONEq(t, string(sampleEventNotAvailableEntityTooLarge), string(payloads[2].StatusDetails[0].SampleEvent))
		require.Equal(t, "sample-response-1", payloads[2].StatusDetails[0].SampleResponse)
		require.Equal(t, "event-2", payloads[3].StatusDetails[0].EventName)
		require.JSONEq(t, `{"event":"sample-2"}`, string(payloads[3].StatusDetails[0].SampleEvent))
		// caller's metric must not be mutated by the split/strip
		require.Len(t, metric.StatusDetails, 2)
		require.JSONEq(t, `{"event":"sample-1"}`, string(metric.StatusDetails[0].SampleEvent))
	})

	t.Run("single status detail skips redundant resend and goes straight to stripped fallback", func(t *testing.T) {
		var mu sync.Mutex
		var payloads []types.Metric
		requestCount := 0
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			defer mu.Unlock()
			requestCount++
			body, err := io.ReadAll(r.Body)
			require.NoError(t, err)
			var payload types.Metric
			require.NoError(t, jsonrs.Unmarshal(body, &payload))
			payloads = append(payloads, payload)

			if requestCount == 1 {
				w.WriteHeader(http.StatusRequestEntityTooLarge)
				return
			}
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		metricClient := newPayloadTooLargeTestClient(t, server.URL, 0)
		metric := testPayloadTooLargeMetricWithOneStatusDetail()
		err := sendMetricWithPayloadTooLargeSplit(context.Background(), metricClient, metric)
		require.NoError(t, err)

		mu.Lock()
		defer mu.Unlock()
		// batch (413) + stripped (200); no identical individual resend in between
		require.Equal(t, 2, requestCount)
		require.JSONEq(t, `{"event":"sample-1"}`, string(payloads[0].StatusDetails[0].SampleEvent))
		require.JSONEq(t, string(sampleEventNotAvailableEntityTooLarge), string(payloads[1].StatusDetails[0].SampleEvent))
		require.JSONEq(t, `{"event":"sample-1"}`, string(metric.StatusDetails[0].SampleEvent))
	})

	t.Run("stripped oversized metric retries through normal fallback path", func(t *testing.T) {
		var mu sync.Mutex
		requestCount := 0
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			requestCount++
			mu.Unlock()
			w.WriteHeader(http.StatusRequestEntityTooLarge)
		}))
		defer server.Close()

		metricClient := newPayloadTooLargeTestClient(t, server.URL, 1)
		err := sendMetricWithPayloadTooLargeSplit(context.Background(), metricClient, testPayloadTooLargeMetricWithOneStatusDetail())
		require.Error(t, err)
		require.False(t, errors.Is(err, client.ErrPayloadTooLarge))
		require.Contains(t, err.Error(), "statusCode: 413")

		mu.Lock()
		defer mu.Unlock()
		// batch (413) + stripped fallback (413, retried once)
		require.Equal(t, 3, requestCount)
	})

	t.Run("partial failure returns error after delivering earlier metrics", func(t *testing.T) {
		var mu sync.Mutex
		var payloads []types.Metric
		requestCount := 0
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			defer mu.Unlock()
			requestCount++
			body, err := io.ReadAll(r.Body)
			require.NoError(t, err)
			var payload types.Metric
			require.NoError(t, jsonrs.Unmarshal(body, &payload))
			payloads = append(payloads, payload)

			switch requestCount {
			case 1:
				w.WriteHeader(http.StatusRequestEntityTooLarge)
			case 2:
				w.WriteHeader(http.StatusOK)
			default:
				w.WriteHeader(http.StatusInternalServerError)
			}
		}))
		defer server.Close()

		metricClient := newPayloadTooLargeTestClient(t, server.URL, 1)
		err := sendMetricWithPayloadTooLargeSplit(context.Background(), metricClient, testPayloadTooLargeMetric())
		require.Error(t, err)
		require.False(t, errors.Is(err, client.ErrPayloadTooLarge))
		require.Contains(t, err.Error(), "statusCode: 500")

		mu.Lock()
		defer mu.Unlock()
		// 1 batch (413) + 1 for event-1 (200) + 2 for event-2 (500, retried once)
		require.Equal(t, 4, requestCount)
		require.Len(t, payloads[1].StatusDetails, 1)
		require.Equal(t, "event-1", payloads[1].StatusDetails[0].EventName)
		require.Len(t, payloads[2].StatusDetails, 1)
		require.Equal(t, "event-2", payloads[2].StatusDetails[0].EventName)
		require.Equal(t, "event-2", payloads[3].StatusDetails[0].EventName)
	})

	t.Run("individual non-413 error retries through normal path", func(t *testing.T) {
		var mu sync.Mutex
		requestCount := 0
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			requestCount++
			currentRequest := requestCount
			mu.Unlock()

			if currentRequest == 1 {
				w.WriteHeader(http.StatusRequestEntityTooLarge)
				return
			}
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer server.Close()

		metricClient := newPayloadTooLargeTestClient(t, server.URL, 1)
		err := sendMetricWithPayloadTooLargeSplit(context.Background(), metricClient, testPayloadTooLargeMetricWithOneStatusDetail())
		require.Error(t, err)
		require.False(t, errors.Is(err, client.ErrPayloadTooLarge))
		require.Contains(t, err.Error(), "statusCode: 500")

		mu.Lock()
		defer mu.Unlock()
		require.Equal(t, 3, requestCount)
	})
}

func testPayloadTooLargeMetric() *types.Metric {
	metric := testPayloadTooLargeMetricWithOneStatusDetail()
	metric.StatusDetails = append(metric.StatusDetails, &types.StatusDetail{
		Status:         "failed",
		Count:          2,
		StatusCode:     http.StatusBadRequest,
		SampleResponse: "sample-response-2",
		SampleEvent:    []byte(`{"event":"sample-2"}`),
		EventName:      "event-2",
		EventType:      "track",
	})
	return metric
}

func testPayloadTooLargeMetricWithOneStatusDetail() *types.Metric {
	return &types.Metric{
		InstanceDetails: types.InstanceDetails{
			WorkspaceID: "workspace-1",
			InstanceID:  "instance-1",
		},
		ConnectionDetails: types.ConnectionDetails{
			SourceID:      "source-1",
			DestinationID: "destination-1",
		},
		PUDetails: types.PUDetails{
			InPU: "gateway",
			PU:   "router",
		},
		ReportMetadata: types.ReportMetadata{
			ReportedAt:        1000,
			SampleEventBucket: 900,
		},
		StatusDetails: []*types.StatusDetail{
			{
				Status:         "failed",
				Count:          1,
				StatusCode:     http.StatusBadRequest,
				SampleResponse: "sample-response-1",
				SampleEvent:    []byte(`{"event":"sample-1"}`),
				EventName:      "event-1",
				EventType:      "track",
			},
		},
	}
}
