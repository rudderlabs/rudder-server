package reporting

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
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
			expectedErr:  "invalid event name trimming configuration: prefixLength=0, suffixLength=30. prefixLength and suffixLength must be > 0",
		},
		{
			name:         "zero suffixLength",
			eventName:    "long_event_name_that_should_cause_error",
			prefixLength: 30,
			suffixLength: 0,
			expectedErr:  "invalid event name trimming configuration: prefixLength=30, suffixLength=0. prefixLength and suffixLength must be > 0",
		},
		{
			name:         "prefixLength equals maxLength",
			eventName:    "long_event_name_that_should_cause_error",
			prefixLength: 30,
			suffixLength: 0,
			expectedErr:  "invalid event name trimming configuration: prefixLength=30, suffixLength=0. prefixLength and suffixLength must be > 0",
		},
		{
			name:         "suffixLength equals maxLength",
			eventName:    "long_event_name_that_should_cause_error",
			prefixLength: 0,
			suffixLength: 30,
			expectedErr:  "invalid event name trimming configuration: prefixLength=0, suffixLength=30. prefixLength and suffixLength must be > 0",
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
