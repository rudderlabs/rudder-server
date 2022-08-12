package reporting

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/rudderlabs/rudder-server/utils/types"
)

var _ = Describe("Reporting", func() {
	Context("transformMetricForPII Tests", func() {
		It("Should match transformMetricForPII response for a valid metric", func() {
			inputMetric := types.PUReportedMetric{
				ConnectionDetails: types.ConnectionDetails{
					SourceID:        "some-source-id",
					DestinationID:   "some-destination-id",
					SourceBatchID:   "some-source-batch-id",
					SourceTaskID:    "some-source-task-id",
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
				},
			}

			expectedResponse := types.PUReportedMetric{
				ConnectionDetails: types.ConnectionDetails{
					SourceID:        "some-source-id",
					DestinationID:   "some-destination-id",
					SourceBatchID:   "some-source-batch-id",
					SourceTaskID:    "some-source-task-id",
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
				},
			}

			piiColumnsToExclude := []string{"sample_response", "sample_event"}
			transformedMetric := transformMetricForPII(inputMetric, piiColumnsToExclude)
			assertReportMetric(expectedResponse, transformedMetric)
		})
	})
})

func assertReportMetric(expectedMetric, actualMetric types.PUReportedMetric) {
	Expect(expectedMetric.ConnectionDetails.SourceID).To(Equal(actualMetric.ConnectionDetails.SourceID))
	Expect(expectedMetric.ConnectionDetails.SourceBatchID).To(Equal(actualMetric.ConnectionDetails.SourceBatchID))
	Expect(expectedMetric.ConnectionDetails.DestinationID).To(Equal(actualMetric.ConnectionDetails.DestinationID))
	Expect(expectedMetric.ConnectionDetails.SourceJobID).To(Equal(actualMetric.ConnectionDetails.SourceJobID))
	Expect(expectedMetric.ConnectionDetails.SourceJobRunID).To(Equal(actualMetric.ConnectionDetails.SourceJobRunID))
	Expect(expectedMetric.ConnectionDetails.SourceTaskRunID).To(Equal(actualMetric.ConnectionDetails.SourceTaskRunID))
	Expect(expectedMetric.ConnectionDetails.SourceTaskID).To(Equal(actualMetric.ConnectionDetails.SourceTaskID))
	Expect(expectedMetric.PUDetails.InPU).To(Equal(actualMetric.PUDetails.InPU))
	Expect(expectedMetric.PUDetails.PU).To(Equal(actualMetric.PUDetails.PU))
	Expect(expectedMetric.PUDetails.TerminalPU).To(Equal(actualMetric.PUDetails.TerminalPU))
	Expect(expectedMetric.PUDetails.InitialPU).To(Equal(actualMetric.PUDetails.InitialPU))
	Expect(expectedMetric.StatusDetail.Status).To(Equal(actualMetric.StatusDetail.Status))
	Expect(expectedMetric.StatusDetail.StatusCode).To(Equal(actualMetric.StatusDetail.StatusCode))
	Expect(expectedMetric.StatusDetail.Count).To(Equal(actualMetric.StatusDetail.Count))
	Expect(expectedMetric.StatusDetail.SampleResponse).To(Equal(actualMetric.StatusDetail.SampleResponse))
	Expect(expectedMetric.StatusDetail.SampleEvent).To(Equal(actualMetric.StatusDetail.SampleEvent))
}
