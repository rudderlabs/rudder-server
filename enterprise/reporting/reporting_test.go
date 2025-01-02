package reporting

import (
	"context"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/assert"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
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
					SampleEvent:    []byte(`{}`),
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
	Expect(expectedMetric.ConnectionDetails.SourceID).To(Equal(actualMetric.ConnectionDetails.SourceID))
	Expect(expectedMetric.ConnectionDetails.DestinationID).To(Equal(actualMetric.ConnectionDetails.DestinationID))
	Expect(expectedMetric.ConnectionDetails.SourceJobID).To(Equal(actualMetric.ConnectionDetails.SourceJobID))
	Expect(expectedMetric.ConnectionDetails.SourceJobRunID).To(Equal(actualMetric.ConnectionDetails.SourceJobRunID))
	Expect(expectedMetric.ConnectionDetails.SourceTaskRunID).To(Equal(actualMetric.ConnectionDetails.SourceTaskRunID))
	Expect(expectedMetric.PUDetails.InPU).To(Equal(actualMetric.PUDetails.InPU))
	Expect(expectedMetric.PUDetails.PU).To(Equal(actualMetric.PUDetails.PU))
	Expect(expectedMetric.PUDetails.TerminalPU).To(Equal(actualMetric.PUDetails.TerminalPU))
	Expect(expectedMetric.PUDetails.InitialPU).To(Equal(actualMetric.PUDetails.InitialPU))
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
		bucket, _ := getAggregationBucketMinute(28017690, 10)
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
		bucket, _ := getAggregationBucketMinute(28017690, 10)
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
		bucket, _ := getAggregationBucketMinute(28017690, 10)
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
