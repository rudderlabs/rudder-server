package reporting

import (
	"net/http"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	. "github.com/onsi/gomega"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	mocksBackendConfig "github.com/rudderlabs/rudder-server/mocks/backend-config"
	"go.uber.org/mock/gomock"

	"context"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
	"github.com/rudderlabs/rudder-server/utils/types"

	utilsTx "github.com/rudderlabs/rudder-server/utils/tx"
	"github.com/stretchr/testify/assert"
)

func TestShouldReport(t *testing.T) {
	RegisterTestingT(t)

	// Test case 1: Event failure case
	metric1 := &types.PUReportedMetric{
		StatusDetail: &types.StatusDetail{
			StatusCode: http.StatusBadRequest,
		},
	}
	Expect(shouldReport(metric1)).To(BeTrue())

	// Test case 2: Filter event case
	metric2 := &types.PUReportedMetric{
		StatusDetail: &types.StatusDetail{
			StatusCode: types.FilterEventCode,
		},
	}
	Expect(shouldReport(metric2)).To(BeTrue())

	// Test case 3: Suppress event case
	metric3 := &types.PUReportedMetric{
		StatusDetail: &types.StatusDetail{
			StatusCode: types.SuppressEventCode,
		},
	}
	Expect(shouldReport(metric3)).To(BeTrue())

	// Test case 4: Success cases
	metric4 := &types.PUReportedMetric{
		StatusDetail: &types.StatusDetail{
			StatusCode: http.StatusOK,
		},
	}
	Expect(shouldReport(metric4)).To(BeFalse())
}

func TestErrorDetailReporter_Report(t *testing.T) {
	db, dbMock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	dbMock.ExpectBegin()
	defer dbMock.ExpectClose()

	tx, _ := db.Begin()
	mockTx := &utilsTx.Tx{Tx: tx}

	tests := []struct {
		name            string
		metrics         []*types.PUReportedMetric
		expectExecution bool
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
						StatusCode: 400,
						Count:      1,
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
						StatusCode: 400,
						Count:      1,
					},
				},
			},
			expectExecution: false,
		},
	}
	configSubscriber := newConfigSubscriber(logger.NOP)
	mockCtrl := gomock.NewController(t)
	mockBackendConfig := mocksBackendConfig.NewMockBackendConfig(mockCtrl)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

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
					},
					Topic: string(backendconfig.TopicBackendConfig),
				}
				close(ch)
				return ch
			}).AnyTimes()

			configSubscriber.Subscribe(context.TODO(), mockBackendConfig)

			edr := NewErrorDetailReporter(
				context.TODO(),
				configSubscriber,
				stats.NOP,
				config.New(),
			)

			ctx := context.Background()

			copyStmt := dbMock.ExpectPrepare(`COPY "error_detail_reports" \("workspace_id", "namespace", "instance_id", "source_definition_id", "source_id", "destination_definition_id", "destination_id", "dest_type", "pu", "reported_at", "count", "status_code", "event_type", "error_code", "error_message", "sample_response", "sample_event", "event_name"\) FROM STDIN`)
			var tableRow int64 = 0
			if tt.expectExecution {
				for _, metric := range tt.metrics {
					copyStmt.ExpectExec().WithArgs(
						sqlmock.AnyArg(),
						sqlmock.AnyArg(),
						sqlmock.AnyArg(),
						sqlmock.AnyArg(),
						metric.ConnectionDetails.SourceID,
						sqlmock.AnyArg(),
						metric.ConnectionDetails.DestinationID,
						sqlmock.AnyArg(),
						sqlmock.AnyArg(),
						sqlmock.AnyArg(),
						metric.StatusDetail.Count,
						metric.StatusDetail.StatusCode,
						sqlmock.AnyArg(),
						sqlmock.AnyArg(),
						sqlmock.AnyArg(),
						sqlmock.AnyArg(),
						sqlmock.AnyArg(),
						sqlmock.AnyArg(),
					).WillReturnResult(sqlmock.NewResult(tableRow, 1))
					tableRow++
				}
				copyStmt.ExpectExec().WithoutArgs().WillReturnResult(sqlmock.NewResult(tableRow, 1)) // ExecContext
			}
			copyStmt.WillBeClosed()
			err := edr.Report(ctx, tt.metrics, mockTx)
			assert.NoError(t, err)

		})
		err = dbMock.ExpectationsWereMet()
		assert.NoError(t, err)
	}
}
