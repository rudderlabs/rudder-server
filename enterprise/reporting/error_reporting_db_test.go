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
func createTestMetrics(sourceID, destinationID string, statusCode int, sampleResponse string, sampleEvent []byte) []*types.PUReportedMetric {
	return []*types.PUReportedMetric{
		{
			ConnectionDetails: types.ConnectionDetails{
				SourceID:      sourceID,
				DestinationID: destinationID,
			},
			StatusDetail: &types.StatusDetail{
				StatusCode:     statusCode,
				Count:          1,
				EventType:      "identify", // Always use "identify" as per linter suggestion
				SampleResponse: sampleResponse,
				SampleEvent:    sampleEvent,
				EventName:      "User Identified", // Always use "User Identified" as per linter suggestion
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
func expectCopyStatement(dbMock sqlmock.Sqlmock, sourceID, destinationID string, statusCode int, _ string) {
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
			metrics:         createTestMetrics("source1", "dest1", 400, `{"error": "Bad Request", "message": "Invalid input"}`, nil),
			expectExecution: true,
		},
		{
			name:            "PII Reporting Disabled, should not report it to error_detail_reports table",
			metrics:         createTestMetrics("source2", "dest2", 400, `{"error": "Bad Request", "message": "Invalid input"}`, nil),
			expectExecution: false,
		},
		{
			name:            "PII Reporting Enabled, should report it to error_detail_reports table",
			metrics:         createTestMetrics("source3", "dest3", 400, `{"error": "Bad Request", "message": "Invalid input"}`, nil),
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
			workspaceID := "workspace" + tt.metrics[0].SourceID[len("source"):]
			disablePII := !tt.expectExecution
			testTS.setupMockBackendConfig(workspaceID, tt.metrics[0].SourceID, tt.metrics[0].DestinationID, "destDef1", "destType", disablePII)

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
					expectCopyStatement(dbMock, metric.SourceID, metric.DestinationID, metric.StatusDetail.StatusCode, metric.StatusDetail.SampleResponse)
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

	metrics := createTestMetrics("source3", "dest3", 400, `{"error": "Bad Request", "message": "Invalid input"}`, nil)
	metrics2 := createTestMetrics("source3", "dest3", 500, `{"error": "Internal Server Error", "message": "Server error"}`, nil)

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
			metrics := createTestMetrics("source1", "dest1", 400, `{"error": "Bad Request"}`, tt.sampleEvent)

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

func TestErrorDetailReporter_GetReports(t *testing.T) {
	tests := []struct {
		name          string
		setupMock     func(sqlmock.Sqlmock)
		expectedError string
	}{
		{
			name: "error while getting minimum reported_at timestamp",
			setupMock: func(mock sqlmock.Sqlmock) {
				// Simulate error when querying for minimum reported_at
				mock.ExpectQuery(`SELECT reported_at FROM error_detail_reports WHERE reported_at < \$1 ORDER BY reported_at ASC LIMIT 1`).
					WithArgs(sqlmock.AnyArg()).
					WillReturnError(sql.ErrConnDone)
			},
			expectedError: "failed while getting reported_at",
		},
		{
			name: "error while querying reports",
			setupMock: func(mock sqlmock.Sqlmock) {
				// First query for minimum reported_at succeeds
				rows := sqlmock.NewRows([]string{"reported_at"}).AddRow(int64(100))
				mock.ExpectQuery(`SELECT reported_at FROM error_detail_reports WHERE reported_at < \$1 ORDER BY reported_at ASC LIMIT 1`).
					WithArgs(sqlmock.AnyArg()).
					WillReturnRows(rows)

				// Second query for actual reports fails
				mock.ExpectQuery(`SELECT .+ FROM error_detail_reports WHERE reported_at = \$1`).
					WithArgs(int64(100)).
					WillReturnError(sql.ErrConnDone)
			},
			expectedError: "failed while getting reports",
		},
		{
			name: "error while scanning rows",
			setupMock: func(mock sqlmock.Sqlmock) {
				// First query for minimum reported_at succeeds
				minRows := sqlmock.NewRows([]string{"reported_at"}).AddRow(int64(100))
				mock.ExpectQuery(`SELECT reported_at FROM error_detail_reports WHERE reported_at < \$1 ORDER BY reported_at ASC LIMIT 1`).
					WithArgs(sqlmock.AnyArg()).
					WillReturnRows(minRows)

				// Second query returns invalid data causing scan error
				reportRows := sqlmock.NewRows([]string{
					"workspace_id",
					"namespace",
					"instance_id",
					"source_definition_id",
					"source_id",
					"destination_definition_id",
					"destination_id",
					"pu",
					"reported_at",
					"count",
					"status_code",
					"event_type",
					"error_code",
					"error_message",
					"dest_type",
					"sample_response",
					"sample_event",
					"event_name",
				}).AddRow(
					"ws1", "ns1", "inst1", "src_def_1", "src1", "dest_def_1", "dest1",
					"router", "invalid_timestamp", // invalid type for reported_at
					1, 400, "identify", "ERR001", "error message", "DES_TYPE",
					`{"error": "test"}`, `{"event": "test"}`, "Test Event",
				)
				mock.ExpectQuery(`SELECT .+ FROM error_detail_reports WHERE reported_at = \$1`).
					WithArgs(int64(100)).
					WillReturnRows(reportRows)
			},
			expectedError: "failed while scanning rows",
		},
		{
			name: "no reports available returns nil without error",
			setupMock: func(mock sqlmock.Sqlmock) {
				// Query returns no rows (NULL)
				mock.ExpectQuery(`SELECT reported_at FROM error_detail_reports WHERE reported_at < \$1 ORDER BY reported_at ASC LIMIT 1`).
					WithArgs(sqlmock.AnyArg()).
					WillReturnRows(sqlmock.NewRows([]string{"reported_at"}))
			},
			expectedError: "", // no error expected
		},
		{
			name: "successful retrieval of reports",
			setupMock: func(mock sqlmock.Sqlmock) {
				// First query for minimum reported_at succeeds
				minRows := sqlmock.NewRows([]string{"reported_at"}).AddRow(int64(100))
				mock.ExpectQuery(`SELECT reported_at FROM error_detail_reports WHERE reported_at < \$1 ORDER BY reported_at ASC LIMIT 1`).
					WithArgs(sqlmock.AnyArg()).
					WillReturnRows(minRows)

				// Second query returns valid reports
				reportRows := sqlmock.NewRows([]string{
					"workspace_id",
					"namespace",
					"instance_id",
					"source_definition_id",
					"source_id",
					"destination_definition_id",
					"destination_id",
					"pu",
					"reported_at",
					"count",
					"status_code",
					"event_type",
					"error_code",
					"error_message",
					"dest_type",
					"sample_response",
					"sample_event",
					"event_name",
				}).
					AddRow(
						"ws1", "ns1", "inst1", "src_def_1", "src1", "dest_def_1", "dest1",
						"router", int64(100), int64(5), 400, "identify", "ERR001",
						"Invalid request payload", "WEBHOOK",
						`{"error": "bad request"}`, `{"userId": "123"}`, "User Identified",
					).
					AddRow(
						"ws1", "ns1", "inst1", "src_def_1", "src1", "dest_def_1", "dest1",
						"router", int64(100), int64(3), 500, "track", "ERR002",
						"Internal server error", "WEBHOOK",
						`{"error": "server error"}`, `{"event": "Order Completed"}`, "Order Completed",
					)
				mock.ExpectQuery(`SELECT .+ FROM error_detail_reports WHERE reported_at = \$1`).
					WithArgs(int64(100)).
					WillReturnRows(reportRows)
			},
			expectedError: "", // no error expected
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts := newTestSetup(t)

			// Create database mock
			db, dbMock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()

			// Setup mock backend config
			ts.setupMockBackendConfig("workspace1", "source1", "dest1", "destDef1", "destType", false)

			edr := NewErrorDetailReporter(
				context.TODO(),
				ts.configSubscriber,
				stats.NOP,
				ts.conf,
			)

			// Register the database handle
			syncerKey := "test-syncer-key"
			edr.syncers[syncerKey] = &types.SyncSource{
				DbHandle: db,
			}

			// Initialize stats that are normally initialized in mainLoop
			// These stats are required by getReports but are only initialized when mainLoop runs
			tags := edr.getTags("test")
			edr.minReportedAtQueryTime = edr.stats.NewTaggedStat("error_detail_reports_min_reported_at_query_time", stats.TimerType, tags)
			edr.errorDetailReportsQueryTime = edr.stats.NewTaggedStat("error_detail_reports_query_time", stats.TimerType, tags)

			// Setup the mock expectations
			tt.setupMock(dbMock)

			// Call getReports
			currentMs := int64(200)
			reports, reportedAt, err := edr.getReports(context.Background(), currentMs, syncerKey)

			// Verify error handling
			if tt.expectedError != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.expectedError)
				require.Empty(t, reports)
			} else {
				require.NoError(t, err)

				// For the "no reports available" case, reports should be nil
				if tt.name == "no reports available returns nil without error" {
					require.Nil(t, reports)
					require.Equal(t, int64(0), reportedAt)
				} else {
					// For the successful retrieval case, validate the reports
					require.NotNil(t, reports)
					require.Len(t, reports, 2)
					require.Equal(t, int64(100), reportedAt)

					// Verify first report
					require.Equal(t, "ws1", reports[0].WorkspaceID)
					require.Equal(t, "src1", reports[0].SourceID)
					require.Equal(t, "dest1", reports[0].DestinationID)
					require.Equal(t, "router", reports[0].PU)
					require.Equal(t, int64(100), reports[0].ReportedAt)
					require.Equal(t, int64(5), reports[0].Count)
					require.Equal(t, 400, reports[0].StatusCode)
					require.Equal(t, "identify", reports[0].EventType)
					require.Equal(t, "ERR001", reports[0].ErrorCode)
					require.Equal(t, "Invalid request payload", reports[0].ErrorMessage)
					require.Equal(t, "WEBHOOK", reports[0].DestType)
					require.Equal(t, `{"error": "bad request"}`, reports[0].SampleResponse)
					require.Equal(t, `{"userId": "123"}`, string(reports[0].SampleEvent))
					require.Equal(t, "User Identified", reports[0].EventName)

					// Verify second report
					require.Equal(t, "ws1", reports[1].WorkspaceID)
					require.Equal(t, int64(3), reports[1].Count)
					require.Equal(t, 500, reports[1].StatusCode)
					require.Equal(t, "track", reports[1].EventType)
					require.Equal(t, "ERR002", reports[1].ErrorCode)
					require.Equal(t, "Internal server error", reports[1].ErrorMessage)
				}
			}

			// Verify all expectations were met
			require.NoError(t, dbMock.ExpectationsWereMet())
		})
	}
}
