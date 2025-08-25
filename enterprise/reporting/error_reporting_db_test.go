package reporting

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"go.uber.org/mock/gomock"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	mocksBackendConfig "github.com/rudderlabs/rudder-server/mocks/backend-config"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
	"github.com/rudderlabs/rudder-server/utils/types"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	utilsTx "github.com/rudderlabs/rudder-server/utils/tx"
)

func TestErrorDetailsReport(t *testing.T) {
	tests := []struct {
		name            string
		metrics         []*types.PUReportedMetric
		expectExecution bool
		rateLimitConfig map[string]interface{}
	}{
		{
			name: "PII Reporting Enabled, should report it to error_detail_reports table",
			metrics: []*types.PUReportedMetric{
				{
					ConnectionDetails: types.ConnectionDetails{
						SourceID:      "source1",
						DestinationID: "dest1",
					},
					StatusDetail: &types.StatusDetail{
						StatusCode:     400,
						Count:          1,
						SampleResponse: `{"error": "Bad Request", "message": "Invalid input"}`,
					},
				},
			},
			expectExecution: true,
		},
		{
			name: "PII Reporting Disabled, should not report it to error_detail_reports table",
			metrics: []*types.PUReportedMetric{
				{
					ConnectionDetails: types.ConnectionDetails{
						SourceID:      "source2",
						DestinationID: "dest2",
					},
					StatusDetail: &types.StatusDetail{
						StatusCode:     400,
						Count:          1,
						SampleResponse: `{"error": "Bad Request", "message": "Invalid input"}`,
					},
				},
			},
			expectExecution: false,
		},
		{
			name: "PII Reporting Enabled, should report it to error_detail_reports table",
			metrics: []*types.PUReportedMetric{
				{
					ConnectionDetails: types.ConnectionDetails{
						SourceID:      "source3",
						DestinationID: "dest3",
					},
					StatusDetail: &types.StatusDetail{
						StatusCode:     400,
						Count:          1,
						EventType:      "identify",
						SampleResponse: `{"error": "Bad Request", "message": "Invalid input"}`,
					},
					PUDetails: types.PUDetails{
						PU: "router",
					},
				},
			},
			expectExecution: true,
		},
	}

	configSubscriber := newConfigSubscriber(logger.NOP)
	mockCtrl := gomock.NewController(t)
	mockBackendConfig := mocksBackendConfig.NewMockBackendConfig(mockCtrl)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a new mock for each test case to avoid interference
			db, dbMock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
			}
			defer db.Close()

			dbMock.ExpectBegin()
			defer dbMock.ExpectClose()

			tx, _ := db.Begin()
			mockTx := &utilsTx.Tx{Tx: tx}
			mockBackendConfig.EXPECT().Subscribe(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, topic backendconfig.Topic) pubsub.DataChannel {
				ch := make(chan pubsub.DataEvent, 1)
				ch <- pubsub.DataEvent{
					Data: map[string]backendconfig.ConfigT{
						"workspace1": {
							WorkspaceID: "workspace1",
							Sources: []backendconfig.SourceT{
								{
									ID:      "source1",
									Enabled: true,
									Destinations: []backendconfig.DestinationT{
										{
											ID:      "dest1",
											Enabled: true,
											DestinationDefinition: backendconfig.DestinationDefinitionT{
												ID:   "destDef1",
												Name: "destType",
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
						"workspace2": {
							WorkspaceID: "workspace2",
							Sources: []backendconfig.SourceT{
								{
									ID:      "source2",
									Enabled: true,
									Destinations: []backendconfig.DestinationT{
										{
											ID:      "dest2",
											Enabled: true,
											DestinationDefinition: backendconfig.DestinationDefinitionT{
												ID:   "destDef1",
												Name: "destType",
											},
										},
									},
								},
							},
							Settings: backendconfig.Settings{
								DataRetention: backendconfig.DataRetention{
									DisableReportingPII: true,
								},
							},
						},
						"workspace3": {
							WorkspaceID: "workspace3",
							Sources: []backendconfig.SourceT{
								{
									ID:      "source3",
									Enabled: true,
									Destinations: []backendconfig.DestinationT{
										{
											ID:      "dest3",
											Enabled: true,
											DestinationDefinition: backendconfig.DestinationDefinitionT{
												ID:   "destDef1",
												Name: "destType",
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

			configSubscriber.Subscribe(context.TODO(), mockBackendConfig)

			conf := config.New()

			// Set rate limit configuration if provided
			if tt.rateLimitConfig != nil {
				for key, value := range tt.rateLimitConfig {
					conf.Set(key, value)
				}
			} else {
				conf.Set("Reporting.errorReporting.normalizer.enabled", false)
			}

			edr := NewErrorDetailReporter(
				context.TODO(),
				configSubscriber,
				stats.NOP,
				conf,
			)

			ctx := context.Background()

			// With the new error grouping approach, statement preparation only happens when there are groups to write
			if tt.expectExecution {
				copyStmt := dbMock.ExpectPrepare(`COPY "error_detail_reports" \("workspace_id", "namespace", "instance_id", "source_definition_id", "source_id", "destination_definition_id", "destination_id", "dest_type", "pu", "reported_at", "count", "status_code", "event_type", "error_code", "error_message", "sample_response", "sample_event", "event_name"\) FROM STDIN`)

				// With error grouping, we expect one execution per group (not per individual metric)
				// For the test cases, each metric will likely form its own group since they have different connection details
				var tableRow int64 = 0
				for _, metric := range tt.metrics {
					copyStmt.ExpectExec().WithArgs(
						sqlmock.AnyArg(), // workspace_id
						sqlmock.AnyArg(), // namespace
						sqlmock.AnyArg(), // instance_id
						sqlmock.AnyArg(), // source_definition_id
						metric.ConnectionDetails.SourceID,
						sqlmock.AnyArg(), // destination_definition_id
						metric.ConnectionDetails.DestinationID,
						sqlmock.AnyArg(), // dest_type
						sqlmock.AnyArg(), // pu
						sqlmock.AnyArg(), // reported_at
						sqlmock.AnyArg(), // count (may be aggregated)
						metric.StatusDetail.StatusCode,
						sqlmock.AnyArg(), // event_type
						sqlmock.AnyArg(), // error_code
						sqlmock.AnyArg(), // error_message
						sqlmock.AnyArg(), // sample_response
						sqlmock.AnyArg(), // sample_event
						sqlmock.AnyArg(), // event_name
					).WillReturnResult(sqlmock.NewResult(tableRow, 1))
					tableRow++
				}
				// The final ExecContext call to finalize the COPY statement
				copyStmt.ExpectExec().WithoutArgs().WillReturnResult(sqlmock.NewResult(0, 0))
				copyStmt.WillBeClosed()
			}
			// If expectExecution is false (PII disabled), no statement preparation should happen
			reportErr := edr.Report(ctx, tt.metrics, mockTx)
			assert.NoError(t, reportErr)

			expectErr := dbMock.ExpectationsWereMet()
			assert.NoError(t, expectErr)
		})
	}
}

func TestAggregationLogic(t *testing.T) {
	dbErrs := []*types.EDReportsDB{
		{
			PU: "dest_transformer",
			EDInstanceDetails: types.EDInstanceDetails{
				WorkspaceID: "wsp1",
				InstanceID:  "instance-1",
				Namespace:   "nmspc",
			},
			EDConnectionDetails: types.EDConnectionDetails{
				SourceID:                "src-1",
				SourceDefinitionId:      "src-def-1",
				DestinationDefinitionId: "des-def-1",
				DestinationID:           "des-1",
				DestType:                "DES_1",
			},
			EDErrorDetails: types.EDErrorDetails{
				EDErrorDetailsKey: types.EDErrorDetailsKey{
					StatusCode:   200,
					ErrorCode:    "",
					ErrorMessage: "",
					EventType:    "identify",
				},
			},
			ReportMetadata: types.ReportMetadata{
				ReportedAt: 124335445,
			},
			Count: 10,
		},
		{
			PU: "dest_transformer",
			EDInstanceDetails: types.EDInstanceDetails{
				WorkspaceID: "wsp1",
				InstanceID:  "instance-1",
				Namespace:   "nmspc",
			},
			EDConnectionDetails: types.EDConnectionDetails{
				SourceID:                "src-1",
				SourceDefinitionId:      "src-def-1",
				DestinationDefinitionId: "des-def-1",
				DestinationID:           "des-1",
				DestType:                "DES_1",
			},
			EDErrorDetails: types.EDErrorDetails{
				EDErrorDetailsKey: types.EDErrorDetailsKey{
					StatusCode:   400,
					ErrorCode:    "",
					ErrorMessage: "bad data sent for transformation",
					EventType:    "identify",
				},
			},
			ReportMetadata: types.ReportMetadata{
				ReportedAt: 124335445,
			},
			Count: 5,
		},
		{
			PU: "dest_transformer",
			EDInstanceDetails: types.EDInstanceDetails{
				WorkspaceID: "wsp1",
				InstanceID:  "instance-1",
				Namespace:   "nmspc",
			},
			EDConnectionDetails: types.EDConnectionDetails{
				SourceID:                "src-1",
				SourceDefinitionId:      "src-def-1",
				DestinationDefinitionId: "des-def-1",
				DestinationID:           "des-1",
				DestType:                "DES_1",
			},
			EDErrorDetails: types.EDErrorDetails{
				EDErrorDetailsKey: types.EDErrorDetailsKey{
					StatusCode:   400,
					ErrorCode:    "",
					ErrorMessage: "bad data sent for transformation",
					EventType:    "identify",
				},
			},
			ReportMetadata: types.ReportMetadata{
				ReportedAt: 124335445,
			},
			Count: 15,
		},
		{
			PU: "dest_transformer",
			EDInstanceDetails: types.EDInstanceDetails{
				WorkspaceID: "wsp1",
				InstanceID:  "instance-1",
				Namespace:   "nmspc",
			},
			EDConnectionDetails: types.EDConnectionDetails{
				SourceID:                "src-1",
				SourceDefinitionId:      "src-def-1",
				DestinationDefinitionId: "des-def-1",
				DestinationID:           "des-1",
				DestType:                "DES_1",
			},
			EDErrorDetails: types.EDErrorDetails{
				EDErrorDetailsKey: types.EDErrorDetailsKey{
					StatusCode:   400,
					ErrorCode:    "",
					ErrorMessage: "user_id information missing",
					EventType:    "identify",
				},
			},
			ReportMetadata: types.ReportMetadata{
				ReportedAt: 124335446,
			},
			Count: 20,
		},
		// error occurred at router level(assume this is batching enabled)
		{
			PU: "router",
			EDInstanceDetails: types.EDInstanceDetails{
				WorkspaceID: "wsp1",
				InstanceID:  "instance-1",
				Namespace:   "nmspc",
			},
			EDConnectionDetails: types.EDConnectionDetails{
				SourceID:                "src-1",
				SourceDefinitionId:      "src-def-1",
				DestinationDefinitionId: "des-def-1",
				DestinationID:           "des-1",
				DestType:                "DES_1",
			},
			EDErrorDetails: types.EDErrorDetails{
				EDErrorDetailsKey: types.EDErrorDetailsKey{
					StatusCode:   500,
					ErrorCode:    "",
					ErrorMessage: "Cannot read type property of undefined", // some error during batching
					EventType:    "identify",
				},
			},
			ReportMetadata: types.ReportMetadata{
				ReportedAt: 124335446,
			},
			Count: 15,
		},
	}

	configSubscriber := newConfigSubscriber(logger.NOP)
	ed := NewErrorDetailReporter(context.Background(), configSubscriber, stats.NOP, config.Default)
	reportingMetrics := ed.aggregate(dbErrs)

	reportResults := []*types.EDMetric{
		{
			PU: dbErrs[0].PU,
			EDInstanceDetails: types.EDInstanceDetails{
				WorkspaceID: dbErrs[0].WorkspaceID,
				InstanceID:  dbErrs[0].InstanceID,
				Namespace:   dbErrs[0].Namespace,
			},
			EDConnectionDetails: types.EDConnectionDetails{
				SourceID:                dbErrs[0].SourceID,
				SourceDefinitionId:      dbErrs[0].SourceDefinitionId,
				DestinationDefinitionId: dbErrs[0].DestinationDefinitionId,
				DestinationID:           dbErrs[0].DestinationID,
				DestType:                dbErrs[0].DestType,
			},
			ReportMetadata: types.ReportMetadata{
				ReportedAt: dbErrs[0].ReportedAt * 60 * 1000,
			},
			Errors: []types.EDErrorDetails{
				{
					EDErrorDetailsKey: types.EDErrorDetailsKey{
						StatusCode:   dbErrs[0].StatusCode,
						ErrorCode:    dbErrs[0].ErrorCode,
						ErrorMessage: dbErrs[0].ErrorMessage,
						EventType:    dbErrs[0].EventType,
					},
					ErrorCount: 10,
				},
				{
					EDErrorDetailsKey: types.EDErrorDetailsKey{
						StatusCode:   dbErrs[1].StatusCode,
						ErrorCode:    dbErrs[1].ErrorCode,
						ErrorMessage: dbErrs[1].ErrorMessage,
						EventType:    dbErrs[1].EventType,
					},
					ErrorCount: 20,
				},
			},
		},
		{
			PU: dbErrs[3].PU,
			EDInstanceDetails: types.EDInstanceDetails{
				WorkspaceID: dbErrs[3].WorkspaceID,
				InstanceID:  dbErrs[3].InstanceID,
				Namespace:   dbErrs[3].Namespace,
			},
			EDConnectionDetails: types.EDConnectionDetails{
				SourceID:                dbErrs[3].SourceID,
				SourceDefinitionId:      dbErrs[3].SourceDefinitionId,
				DestinationDefinitionId: dbErrs[3].DestinationDefinitionId,
				DestinationID:           dbErrs[3].DestinationID,
				DestType:                dbErrs[3].DestType,
			},
			ReportMetadata: types.ReportMetadata{
				ReportedAt: dbErrs[3].ReportedAt * 60 * 1000,
			},
			Errors: []types.EDErrorDetails{
				{
					EDErrorDetailsKey: types.EDErrorDetailsKey{
						StatusCode:   dbErrs[3].StatusCode,
						ErrorCode:    dbErrs[3].ErrorCode,
						ErrorMessage: dbErrs[3].ErrorMessage,
						EventType:    dbErrs[3].EventType,
					},
					ErrorCount: 20,
				},
			},
		},
		{
			PU: dbErrs[4].PU,
			EDInstanceDetails: types.EDInstanceDetails{
				WorkspaceID: dbErrs[4].WorkspaceID,
				InstanceID:  dbErrs[4].InstanceID,
				Namespace:   dbErrs[4].Namespace,
			},
			EDConnectionDetails: types.EDConnectionDetails{
				SourceID:                dbErrs[4].SourceID,
				SourceDefinitionId:      dbErrs[4].SourceDefinitionId,
				DestinationDefinitionId: dbErrs[4].DestinationDefinitionId,
				DestinationID:           dbErrs[4].DestinationID,
				DestType:                dbErrs[4].DestType,
			},
			ReportMetadata: types.ReportMetadata{
				ReportedAt: dbErrs[4].ReportedAt * 60 * 1000,
			},
			Errors: []types.EDErrorDetails{
				{
					EDErrorDetailsKey: types.EDErrorDetailsKey{
						StatusCode:   dbErrs[4].StatusCode,
						ErrorCode:    dbErrs[4].ErrorCode,
						ErrorMessage: dbErrs[4].ErrorMessage,
						EventType:    dbErrs[4].EventType,
					},
					ErrorCount: 15,
				},
			},
		},
	}

	require.Equal(t, reportResults, reportingMetrics)
}

func TestErrorDetailsReport_RateLimiting(t *testing.T) {
	// Test rate limiting behavior by sending 2 reports
	// First report should succeed, second should be rate limited

	metrics := []*types.PUReportedMetric{
		{
			ConnectionDetails: types.ConnectionDetails{
				SourceID:      "source3",
				DestinationID: "dest3",
			},
			StatusDetail: &types.StatusDetail{
				StatusCode:     400,
				Count:          1,
				EventType:      "identify",
				SampleResponse: `{"error": "Bad Request", "message": "Invalid input"}`,
			},
			PUDetails: types.PUDetails{
				PU: "router",
			},
		},
	}

	// Create a second set of metrics with a different error message for the second report
	metrics2 := []*types.PUReportedMetric{
		{
			ConnectionDetails: types.ConnectionDetails{
				SourceID:      "source3",
				DestinationID: "dest3",
			},
			StatusDetail: &types.StatusDetail{
				StatusCode:     500,
				Count:          1,
				EventType:      "identify",
				SampleResponse: `{"error": "Internal Server Error", "message": "Server error"}`,
			},
			PUDetails: types.PUDetails{
				PU: "router",
			},
		},
	}

	configSubscriber := newConfigSubscriber(logger.NOP)
	mockCtrl := gomock.NewController(t)
	mockBackendConfig := mocksBackendConfig.NewMockBackendConfig(mockCtrl)

	// Create a new mock for the test
	db, dbMock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	dbMock.ExpectBegin()
	defer dbMock.ExpectClose()

	tx, _ := db.Begin()
	mockTx := &utilsTx.Tx{Tx: tx}
	mockBackendConfig.EXPECT().Subscribe(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, topic backendconfig.Topic) pubsub.DataChannel {
		ch := make(chan pubsub.DataEvent, 1)
		ch <- pubsub.DataEvent{
			Data: map[string]backendconfig.ConfigT{
				"workspace3": {
					WorkspaceID: "workspace3",
					Sources: []backendconfig.SourceT{
						{
							ID:      "source3",
							Enabled: true,
							Destinations: []backendconfig.DestinationT{
								{
									ID:      "dest3",
									Enabled: true,
									DestinationDefinition: backendconfig.DestinationDefinitionT{
										ID:   "destDef1",
										Name: "destType",
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

	configSubscriber.Subscribe(context.TODO(), mockBackendConfig)

	conf := config.New()
	conf.Set("Reporting.errorReporting.normalizer.enabled", true)
	conf.Set("Reporting.errorReporting.normalizer.maxErrorsPerMinute", 1)
	conf.Set("Reporting.errorReporting.normalizer.maxCounters", 1)

	edr := NewErrorDetailReporter(
		context.TODO(),
		configSubscriber,
		stats.NOP,
		conf,
	)

	ctx := context.Background()

	// First report: should succeed and fill the bucket
	firstCopyStmt := dbMock.ExpectPrepare(`COPY "error_detail_reports" \("workspace_id", "namespace", "instance_id", "source_definition_id", "source_id", "destination_definition_id", "destination_id", "dest_type", "pu", "reported_at", "count", "status_code", "event_type", "error_code", "error_message", "sample_response", "sample_event", "event_name"\) FROM STDIN`)

	var tableRow int64 = 0
	for _, metric := range metrics {
		firstCopyStmt.ExpectExec().WithArgs(
			sqlmock.AnyArg(), // workspace_id
			sqlmock.AnyArg(), // namespace
			sqlmock.AnyArg(), // instance_id
			sqlmock.AnyArg(), // source_definition_id
			metric.ConnectionDetails.SourceID,
			sqlmock.AnyArg(), // destination_definition_id
			metric.ConnectionDetails.DestinationID,
			sqlmock.AnyArg(), // dest_type
			sqlmock.AnyArg(), // pu
			sqlmock.AnyArg(), // reported_at
			sqlmock.AnyArg(), // count (may be aggregated)
			metric.StatusDetail.StatusCode,
			sqlmock.AnyArg(), // event_type
			sqlmock.AnyArg(), // error_code
			sqlmock.AnyArg(), // error_message (original message for first report)
			sqlmock.AnyArg(), // sample_response
			sqlmock.AnyArg(), // sample_event
			sqlmock.AnyArg(), // event_name
		).WillReturnResult(sqlmock.NewResult(tableRow, 1))
		tableRow++
	}
	firstCopyStmt.ExpectExec().WithoutArgs().WillReturnResult(sqlmock.NewResult(0, 0))
	firstCopyStmt.WillBeClosed()

	// Send first report to fill the bucket
	firstReportErr := edr.Report(ctx, metrics, mockTx)
	assert.NoError(t, firstReportErr)

	// Second report: should be rate limited and return "UnknownError"
	secondCopyStmt := dbMock.ExpectPrepare(`COPY "error_detail_reports" \("workspace_id", "namespace", "instance_id", "source_definition_id", "source_id", "destination_definition_id", "destination_id", "dest_type", "pu", "reported_at", "count", "status_code", "event_type", "error_code", "error_message", "sample_response", "sample_event", "event_name"\) FROM STDIN`)

	tableRow = 0
	for _, metric := range metrics2 {
		secondCopyStmt.ExpectExec().WithArgs(
			sqlmock.AnyArg(), // workspace_id
			sqlmock.AnyArg(), // namespace
			sqlmock.AnyArg(), // instance_id
			sqlmock.AnyArg(), // source_definition_id
			metric.ConnectionDetails.SourceID,
			sqlmock.AnyArg(), // destination_definition_id
			metric.ConnectionDetails.DestinationID,
			sqlmock.AnyArg(), // dest_type
			sqlmock.AnyArg(), // pu
			sqlmock.AnyArg(), // reported_at
			sqlmock.AnyArg(), // count (may be aggregated)
			metric.StatusDetail.StatusCode,
			sqlmock.AnyArg(), // event_type
			sqlmock.AnyArg(), // error_code
			"UnknownError",   // error_message (rate limited message for second report)
			sqlmock.AnyArg(), // sample_response
			sqlmock.AnyArg(), // sample_event
			sqlmock.AnyArg(), // event_name
		).WillReturnResult(sqlmock.NewResult(tableRow, 1))
		tableRow++
	}
	secondCopyStmt.ExpectExec().WithoutArgs().WillReturnResult(sqlmock.NewResult(0, 0))
	secondCopyStmt.WillBeClosed()

	// Send second report (should be rate limited)
	secondReportErr := edr.Report(ctx, metrics2, mockTx)
	assert.NoError(t, secondReportErr)

	expectErr := dbMock.ExpectationsWereMet()
	assert.NoError(t, expectErr)
}
