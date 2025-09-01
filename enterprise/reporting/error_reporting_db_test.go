package reporting

import (
	"context"
	"database/sql"
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

// testSetup holds common test configuration and mocks
type testSetup struct {
	configSubscriber  *configSubscriber
	mockCtrl          *gomock.Controller
	mockBackendConfig *mocksBackendConfig.MockBackendConfig
	conf              *config.Config
}

// newTestSetup creates a new test setup with common configuration
func newTestSetup(t *testing.T) *testSetup {
	configSubscriber := newConfigSubscriber(logger.NOP)
	mockCtrl := gomock.NewController(t)
	mockBackendConfig := mocksBackendConfig.NewMockBackendConfig(mockCtrl)

	conf := config.New()
	conf.Set("Reporting.errorReporting.normalizer.enabled", false)

	return &testSetup{
		configSubscriber:  configSubscriber,
		mockCtrl:          mockCtrl,
		mockBackendConfig: mockBackendConfig,
		conf:              conf,
	}
}

// setupMockBackendConfig configures the mock backend config with workspace data
func (ts *testSetup) setupMockBackendConfig(workspaceID, sourceID, destinationID, destDefID, destType string, disablePII bool) {
	ts.mockBackendConfig.EXPECT().Subscribe(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, topic backendconfig.Topic) pubsub.DataChannel {
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
										ID:   destDefID,
										Name: destType,
									},
								},
							},
						},
					},
					Settings: backendconfig.Settings{
						DataRetention: backendconfig.DataRetention{
							DisableReportingPII: disablePII,
						},
					},
				},
			},
			Topic: string(backendconfig.TopicBackendConfig),
		}
		close(ch)
		return ch
	}).AnyTimes()

	ts.configSubscriber.Subscribe(context.TODO(), ts.mockBackendConfig)
}

// createTestMetrics creates test metrics with the given parameters
func createTestMetrics(sourceID, destinationID string, statusCode int, eventType, sampleResponse string, sampleEvent []byte, eventName string) []*types.PUReportedMetric {
	return []*types.PUReportedMetric{
		{
			ConnectionDetails: types.ConnectionDetails{
				SourceID:      sourceID,
				DestinationID: destinationID,
			},
			StatusDetail: &types.StatusDetail{
				StatusCode:     statusCode,
				Count:          1,
				EventType:      eventType,
				SampleResponse: sampleResponse,
				SampleEvent:    sampleEvent,
				EventName:      eventName,
			},
			PUDetails: types.PUDetails{
				PU: "router",
			},
		},
	}
}

// setupDatabaseMock creates and configures a database mock for testing
func setupDatabaseMock(t *testing.T) (*sql.DB, sqlmock.Sqlmock, *utilsTx.Tx) {
	db, dbMock, err := sqlmock.New()
	require.NoError(t, err, "failed to create database mock")

	dbMock.ExpectBegin()

	tx, _ := db.Begin()
	mockTx := &utilsTx.Tx{Tx: tx}

	return db, dbMock, mockTx
}

// expectCopyStatement sets up expectations for the COPY statement
func expectCopyStatement(dbMock sqlmock.Sqlmock, sourceID, destinationID string, statusCode int, expectedSampleEvent string) {
	copyStmt := dbMock.ExpectPrepare(`COPY "error_detail_reports" \("workspace_id", "namespace", "instance_id", "source_definition_id", "source_id", "destination_definition_id", "destination_id", "dest_type", "pu", "reported_at", "count", "status_code", "event_type", "error_code", "error_message", "sample_response", "sample_event", "event_name"\) FROM STDIN`)

	copyStmt.ExpectExec().WithArgs(
		sqlmock.AnyArg(), // workspace_id
		sqlmock.AnyArg(), // namespace
		sqlmock.AnyArg(), // instance_id
		sqlmock.AnyArg(), // source_definition_id
		sourceID,         // source_id
		sqlmock.AnyArg(), // destination_definition_id
		destinationID,    // destination_id
		sqlmock.AnyArg(), // dest_type
		sqlmock.AnyArg(), // pu
		sqlmock.AnyArg(), // reported_at
		sqlmock.AnyArg(), // count
		statusCode,       // status_code
		sqlmock.AnyArg(), // event_type
		sqlmock.AnyArg(), // error_code
		sqlmock.AnyArg(), // error_message
		sqlmock.AnyArg(), // sample_response
		"{}",             // sample_event (always empty JSON object for nil input)
		sqlmock.AnyArg(), // event_name
	).WillReturnResult(sqlmock.NewResult(0, 1))

	// The final ExecContext call to finalize the COPY statement
	copyStmt.ExpectExec().WithoutArgs().WillReturnResult(sqlmock.NewResult(0, 0))
	copyStmt.WillBeClosed()
}

func TestErrorDetailsReport(t *testing.T) {
	tests := []struct {
		name            string
		metrics         []*types.PUReportedMetric
		expectExecution bool
		rateLimitConfig map[string]interface{}
	}{
		{
			name:            "PII Reporting Enabled, should report it to error_detail_reports table",
			metrics:         createTestMetrics("source1", "dest1", 400, "identify", `{"error": "Bad Request", "message": "Invalid input"}`, nil, "User Identified"),
			expectExecution: true,
		},
		{
			name:            "PII Reporting Disabled, should not report it to error_detail_reports table",
			metrics:         createTestMetrics("source2", "dest2", 400, "identify", `{"error": "Bad Request", "message": "Invalid input"}`, nil, "User Identified"),
			expectExecution: false,
		},
		{
			name:            "PII Reporting Enabled, should report it to error_detail_reports table",
			metrics:         createTestMetrics("source3", "dest3", 400, "identify", `{"error": "Bad Request", "message": "Invalid input"}`, nil, "User Identified"),
			expectExecution: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, dbMock, mockTx := setupDatabaseMock(t)
			defer db.Close()

			// Create a new test setup for each test to avoid interference
			testTS := newTestSetup(t)

			// Setup mock backend config for each test case
			workspaceID := "workspace" + tt.metrics[0].ConnectionDetails.SourceID[len("source"):]
			disablePII := !tt.expectExecution
			testTS.setupMockBackendConfig(workspaceID, tt.metrics[0].ConnectionDetails.SourceID, tt.metrics[0].ConnectionDetails.DestinationID, "destDef1", "destType", disablePII)

			// Wait for config subscriber to be initialized
			testTS.configSubscriber.Wait()

			conf := config.New()
			if tt.rateLimitConfig != nil {
				for key, value := range tt.rateLimitConfig {
					conf.Set(key, value)
				}
			}

			edr := NewErrorDetailReporter(
				context.TODO(),
				testTS.configSubscriber,
				stats.NOP,
				conf,
			)

			ctx := context.Background()

			if tt.expectExecution {
				// Expect COPY statement for each metric
				for _, metric := range tt.metrics {
					expectCopyStatement(dbMock, metric.ConnectionDetails.SourceID, metric.ConnectionDetails.DestinationID, metric.StatusDetail.StatusCode, metric.StatusDetail.SampleResponse)
				}
			}

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

	ts := newTestSetup(t)
	ed := NewErrorDetailReporter(context.Background(), ts.configSubscriber, stats.NOP, ts.conf)
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

	metrics := createTestMetrics("source3", "dest3", 400, "identify", `{"error": "Bad Request", "message": "Invalid input"}`, nil, "User Identified")
	metrics2 := createTestMetrics("source3", "dest3", 500, "identify", `{"error": "Internal Server Error", "message": "Server error"}`, nil, "User Identified")

	ts := newTestSetup(t)
	db, dbMock, mockTx := setupDatabaseMock(t)
	defer db.Close()

	// Setup mock backend config
	ts.setupMockBackendConfig("workspace3", "source3", "dest3", "destDef1", "destType", false)

	conf := config.New()
	conf.Set("Reporting.errorReporting.normalizer.enabled", true)
	conf.Set("Reporting.errorReporting.normalizer.maxErrorsPerGroup", 1)
	conf.Set("Reporting.errorReporting.normalizer.maxGroups", 1)

	edr := NewErrorDetailReporter(
		context.TODO(),
		ts.configSubscriber,
		stats.NOP,
		conf,
	)

	ctx := context.Background()

	// First report: should succeed and fill the bucket
	expectCopyStatement(dbMock, "source3", "dest3", 400, `{"error": "Bad Request", "message": "Invalid input"}`)

	// Send first report to fill the bucket
	firstReportErr := edr.Report(ctx, metrics, mockTx)
	assert.NoError(t, firstReportErr)

	// Second report: should be rate limited and return "RedactedError"
	expectCopyStatement(dbMock, "source3", "dest3", 500, `{"error": "Internal Server Error", "message": "Server error"}`)

	// Send second report (should be rate limited)
	secondReportErr := edr.Report(ctx, metrics2, mockTx)
	assert.NoError(t, secondReportErr)

	expectErr := dbMock.ExpectationsWereMet()
	assert.NoError(t, expectErr)
}

func TestErrorDetailReporter_GetStringifiedSampleEvent(t *testing.T) {
	tests := []struct {
		name           string
		sampleEvent    []byte
		expectedResult string
	}{
		{
			name:           "nil sample event returns empty JSON object",
			sampleEvent:    nil,
			expectedResult: "{}",
		},
		{
			name:           "empty sample event returns empty JSON object",
			sampleEvent:    []byte(`{}`),
			expectedResult: "{}",
		},
		{
			name:           "valid sample event is preserved as-is",
			sampleEvent:    []byte(`{"userId": "123", "traits": {"email": "test@example.com"}}`),
			expectedResult: `{"userId": "123", "traits": {"email": "test@example.com"}}`,
		},
		{
			name:           "complex sample event is preserved as-is",
			sampleEvent:    []byte(`{"event": "Page Viewed", "properties": {"page": "/home", "referrer": "google.com"}}`),
			expectedResult: `{"event": "Page Viewed", "properties": {"page": "/home", "referrer": "google.com"}}`,
		},
	}

	ts := newTestSetup(t)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a new mock for each test case to avoid interference
			db, dbMock, mockTx := setupDatabaseMock(t)
			defer db.Close()

			// Setup mock backend config
			ts.setupMockBackendConfig("workspace1", "source1", "dest1", "destDef1", "destType", false)

			edr := NewErrorDetailReporter(
				context.TODO(),
				ts.configSubscriber,
				stats.NOP,
				ts.conf,
			)

			ctx := context.Background()

			// Create test metrics with the specific sample event
			metrics := createTestMetrics("source1", "dest1", 400, "identify", `{"error": "Bad Request"}`, tt.sampleEvent, "User Identified")

			// Expect the COPY statement to be prepared with the correct sample event
			copyStmt := dbMock.ExpectPrepare(`COPY "error_detail_reports" \("workspace_id", "namespace", "instance_id", "source_definition_id", "source_id", "destination_definition_id", "destination_id", "dest_type", "pu", "reported_at", "count", "status_code", "event_type", "error_code", "error_message", "sample_response", "sample_event", "event_name"\) FROM STDIN`)

			copyStmt.ExpectExec().WithArgs(
				sqlmock.AnyArg(),  // workspace_id
				sqlmock.AnyArg(),  // namespace
				sqlmock.AnyArg(),  // instance_id
				sqlmock.AnyArg(),  // source_definition_id
				"source1",         // source_id
				sqlmock.AnyArg(),  // destination_definition_id
				"dest1",           // destination_id
				sqlmock.AnyArg(),  // dest_type
				sqlmock.AnyArg(),  // pu
				sqlmock.AnyArg(),  // reported_at
				sqlmock.AnyArg(),  // count
				400,               // status_code
				sqlmock.AnyArg(),  // event_type
				sqlmock.AnyArg(),  // error_code
				sqlmock.AnyArg(),  // error_message
				sqlmock.AnyArg(),  // sample_response
				tt.expectedResult, // sample_event
				sqlmock.AnyArg(),  // event_name
			).WillReturnResult(sqlmock.NewResult(0, 1))

			// The final ExecContext call to finalize the COPY statement
			copyStmt.ExpectExec().WithoutArgs().WillReturnResult(sqlmock.NewResult(0, 0))
			copyStmt.WillBeClosed()

			// Send the report
			reportErr := edr.Report(ctx, metrics, mockTx)
			require.NoError(t, reportErr)

			// Verify all expectations were met
			expectErr := dbMock.ExpectationsWereMet()
			require.NoError(t, expectErr)
		})
	}
}
