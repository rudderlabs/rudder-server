package reporting

import (
	"context"
	"fmt"
	"testing"

	"github.com/rudderlabs/rudder-go-kit/stats"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/logger"
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

	Context("getAggregatedReports Tests", func() {
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
					ViolationCount: 10,
					StatusCode:     200,
					SampleResponse: "",
					SampleEvent:    []byte(`{}`),
					ErrorType:      "some-error-type",
				},
			},
		}

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
					ReportedAt: 28017690 * 60 * 1000,
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
						Count:          5,
						ViolationCount: 20,
						StatusCode:     200,
						SampleResponse: "",
						SampleEvent:    []byte(`{}`),
						ErrorType:      "some-error-type",
					},
				},
			},
		}
		configSubscriber := newConfigSubscriber(logger.NOP)
		reportHandle := NewDefaultReporter(context.Background(), logger.NOP, configSubscriber, stats.NOP)

		aggregatedMetrics := reportHandle.getAggregatedReports(inputReports)
		Expect(aggregatedMetrics).To(Equal(expectedResponse))
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

type getValTc struct {
	inputStr string
	expected string
}

var tcs = []getValTc{
	{
		inputStr: `{"response":"{\"message\":\"Primary key 'Contact Key' does not exist.\",\"errorcode\":10000,\"documentation\":\"\"}"}`,
		expected: "Primary key 'Contact Key' does not exist.",
	},
	{
		inputStr: `{"response":"Event Edit_Order_Button_Clicked doesn't match with Snapchat Events!","firstAttemptedAt":"2023-03-30T16:58:05.628Z","content-type":"application/json"}`,
		expected: "Event Edit_Order_Button_Clicked doesn't match with Snapchat Events!",
	},
	{
		inputStr: `{"response":"{\"status\":400,\"message\":\"Failed with Unsupported post request. Object with ID '669556453669016' does not exist, cannot be loaded due to missing permissions, or does not support this operation. Please read the Graph API documentation at https://developers.facebook.com/docs/graph-api during response transformation\",\"destinationResponse\":{\"error\":{\"message\":\"Unsupported post request. Object with ID '669556453669016' does not exist, cannot be loaded due to missing permissions, or does not support this operation. Please read the Graph API documentation at https://developers.facebook.com/docs/graph-api\",\"type\":\"GraphMethodException\",\"code\":100,\"error_subcode\":33,\"fbtrace_id\":\"AAjsXHCiypjAV50Vg-dZx4D\"},\"status\":400},\"statTags\":{\"errorCategory\":\"network\",\"errorType\":\"aborted\",\"destType\":\"FACEBOOK_PIXEL\",\"module\":\"destination\",\"implementation\":\"native\",\"feature\":\"dataDelivery\",\"destinationId\":\"2EXmEugZiMykYfFzoPnXjq6bJ6D\",\"workspaceId\":\"1vTJeDNwZx4bJF7c5ZUWPqJpMVx\",\"context\":\"[Native Integration Service] Failure During Processor Transform\"}}","firstAttemptedAt":"2023-03-30T17:39:17.638Z","content-type":"application/json"}`,
		expected: "Unsupported post request. Object with ID '669556453669016' does not exist, cannot be loaded due to missing permissions, or does not support this operation. Please read the Graph API documentation at https://developers.facebook.com/docs/graph-api",
	},
	{
		inputStr: `{"response":"{\n  \"meta\": {\n    \"error\": \"Unauthorized request\"\n  }\n}\n","firstAttemptedAt":"2023-03-30T17:39:00.377Z","content-type":"application/json; charset=utf-8"}`,
		expected: "Unauthorized request",
	},
	{
		inputStr: `TypeError: Cannot set property 'event_name' of undefined`,
		expected: `TypeError: Cannot set property 'event_name' of undefined`,
	},
	{
		inputStr: `{"response":"{\"code\":400,\"error\":\"Invalid API key: 88ea3fd9e15f74491ac6dd401c8733c9\"}\r\r\r\n","firstAttemptedAt":"2023-03-30T17:39:36.755Z","content-type":"application/json"}`,
		expected: `Invalid API key: 88ea3fd9e15f74491ac6dd401c8733c9`,
	},
	{
		inputStr: `{"response":"{\"status\":\"error\",\"message\":\"Invalid input JSON on line 1, column 54: Cannot deserialize value of type ` + "`" + `java.lang.String` + "`" + ` from Object value (token ` + "`" + `JsonToken.START_OBJECT` + "`" + `)\",\"correlationId\":\"c73c9759-e9fe-4061-8da6-3d7a10159f54\"}","firstAttemptedAt":"2023-03-30T17:39:12.397Z","content-type":"application/json;charset=utf-8"}`,
		expected: `Invalid input JSON on line 1, column 54: Cannot deserialize value of type ` + "`" + `java.lang.String` + "`" + ` from Object value (token ` + "`" + `JsonToken.START_OBJECT` + "`" + `)`,
	},
	{
		inputStr: `{"response":"{\"error\":\"Event request failed (Invalid callback parameters)\"}","firstAttemptedAt":"2023-03-30T17:38:36.152Z","content-type":"application/json; charset=utf-8"}`,
		expected: `Event request failed (Invalid callback parameters)`,
	},
	{
		inputStr: `{"response":"{\"type\":\"error.list\",\"request_id\":\"0000ib5themn9npuifdg\",\"errors\":[{\"code\":\"not_found\",\"message\":\"User Not Found\"}]}","firstAttemptedAt":"2023-03-30T17:38:47.857Z","content-type":"application/json; charset=utf-8"}`,
		expected: `User Not Found`,
	},
	{
		inputStr: `{"response":"{\"type\":\"error.list\",\"request_id\":\"0000ohm9i1s3euaavmmg\",\"errors\":[{\"code\":\"unauthorized\",\"message\":\"Access Token Invalid\"}]}","firstAttemptedAt":"2023-03-30T16:59:37.973Z","content-type":"application/json; charset=utf-8"}`,
		expected: `Access Token Invalid`,
	},
	{
		inputStr: `{"response":"[CDM GOOGLESHEETS] Unable to create client for 21xmYbMXCovrqQn5BIszNvcYmur circuit breaker is open, last error: [GoogleSheets] error :: [GoogleSheets] error  :: error in GoogleSheets while unmarshalling credentials json:: invalid character 'v' looking for beginning of value","firstAttemptedAt":"2023-03-30T16:58:32.010Z","content-type":""}`,
		expected: `[CDM GOOGLESHEETS] Unable to create client for 21xmYbMXCovrqQn5BIszNvcYmur circuit breaker is open, last error: [GoogleSheets] error :: [GoogleSheets] error  :: error in GoogleSheets while unmarshalling credentials json:: invalid character 'v' looking for beginning of value`,
	},
	{
		inputStr: `{"response":"{\n  \"meta\": {\n    \"errors\": [\n      \"id attribute and identifier are not the same value\",\n      \"value for attribute 'token' cannot be longer than 1000 bytes\"\n    ]\n  }\n}\n","firstAttemptedAt":"2023-03-30T17:31:01.599Z","content-type":"application/json; charset=utf-8"}`,
		expected: `id attribute and identifier are not the same value.value for attribute 'token' cannot be longer than 1000 bytes`,
	},
	{
		inputStr: `{"response":"{\"errors\":[\"No users with this external_id found\"]}","firstAttemptedAt":"2023-03-30T17:37:58.184Z","content-type":"application/json; charset=utf-8"}`,
		expected: `No users with this external_id found`,
	},
	{
		inputStr: `{"response":"{\"status\":500,\"destinationResponse\":{\"response\":\"[ECONNRESET] :: Connection reset by peer\",\"status\":500,\"rudderJobMetadata\":{\"jobId\":2942260148,\"attemptNum\":0,\"userId\":\"\",\"sourceId\":\"1qWeSkIiAhg3O96KzVVU5MieZHM\",\"destinationId\":\"2IpcvzcMgbLfXgGA4auDrtfEMuI\",\"workspaceId\":\"1nnEnc0tt7k9eFf7bz1GsiU0MFC\",\"secret\":null}},\"message\":\"[GA4 Response Handler] Request failed for destination ga4 with status: 500\",\"statTags\":{\"errorCategory\":\"network\",\"errorType\":\"retryable\",\"destType\":\"GA4\",\"module\":\"destination\",\"implementation\":\"native\",\"feature\":\"dataDelivery\",\"destinationId\":\"2IpcvzcMgbLfXgGA4auDrtfEMuI\",\"workspaceId\":\"1nnEnc0tt7k9eFf7bz1GsiU0MFC\"}}","firstAttemptedAt":"2023-03-30T17:22:26.488Z","content-type":"application/json"}`,
		expected: `[GA4 Response Handler] Request failed for destination ga4 with status: 500`,
	},
	{
		inputStr: `{"response":"{\"status\":502,\"destinationResponse\":{\"response\":\"\u003chtml\u003e\\r\\n\u003chead\u003e\u003ctitle\u003e502 Bad Gateway\u003c/title\u003e\u003c/head\u003e\\r\\n\u003cbody\u003e\\r\\n\u003ccenter\u003e\u003ch1\u003e502 Bad Gateway\u003c/h1\u003e\u003c/center\u003e\\r\\n\u003c/body\u003e\\r\\n\u003c/html\u003e\\r\\n\",\"status\":502,\"rudderJobMetadata\":{\"jobId\":4946423471,\"attemptNum\":0,\"userId\":\"\",\"sourceId\":\"2CRIZp4OS10sjjGF3uF4guZuGmj\",\"destinationId\":\"2DstxLWX7Oi7gPdPM1CR9CikOko\",\"workspaceId\":\"1yaBlqltp5Y4V2NK8qePowlyafu\",\"secret\":null}},\"message\":\"Request failed  with status: 502\",\"statTags\":{\"errorCategory\":\"network\",\"errorType\":\"retryable\",\"destType\":\"CLEVERTAP\",\"module\":\"destination\",\"implementation\":\"native\",\"feature\":\"dataDelivery\",\"destinationId\":\"2DstxLWX7Oi7gPdPM1CR9CikOko\",\"workspaceId\":\"1yaBlqltp5Y4V2NK8qePowlyafu\"}}","firstAttemptedAt":"2023-03-30T17:11:55.326Z","content-type":"application/json"}`,
		expected: "Request failed  with status: 502",
	},
	{
		inputStr: `{"response":"{\"status\":400,\"destinationResponse\":{\"response\":\"\u003c!DOCTYPE html\u003e\\n\u003chtml lang=en\u003e\\n  \u003cmeta charset=utf-8\u003e\\n  \u003cmeta name=viewport content=\\\"initial-scale=1, minimum-scale=1, width=device-width\\\"\u003e\\n  \u003ctitle\u003eError 400 (Bad Request)!!1\u003c/title\u003e\\n  \u003cstyle\u003e\\n    *{margin:0;padding:0}html,code{font:15px/22px arial,sans-serif}html{background:#fff;color:#222;padding:15px}body{margin:7% auto 0;max-width:390px;min-height:180px;padding:30px 0 15px}* \u003e body{background:url(//www.google.com/images/errors/robot.png) 100% 5px no-repeat;padding-right:205px}p{margin:11px 0 22px;overflow:hidden}ins{color:#777;text-decoration:none}a img{border:0}@media screen and (max-width:772px){body{background:none;margin-top:0;max-width:none;padding-right:0}}#logo{background:url(//www.google.com/images/branding/googlelogo/1x/googlelogo_color_150x54dp.png) no-repeat;margin-left:-5px}@media only screen and (min-resolution:192dpi){#logo{background:url(//www.google.com/images/branding/googlelogo/2x/googlelogo_color_150x54dp.png) no-repeat 0% 0%/100% 100%;-moz-border-image:url(//www.google.com/images/branding/googlelogo/2x/googlelogo_color_150x54dp.png) 0}}@media only screen and (-webkit-min-device-pixel-ratio:2){#logo{background:url(//www.google.com/images/branding/googlelogo/2x/googlelogo_color_150x54dp.png) no-repeat;-webkit-background-size:100% 100%}}#logo{display:inline-block;height:54px;width:150px}\\n  \u003c/style\u003e\\n  \u003ca href=//www.google.com/\u003e\u003cspan id=logo aria-label=Google\u003e\u003c/span\u003e\u003c/a\u003e\\n  \u003cp\u003e\u003cb\u003e400.\u003c/b\u003e \u003cins\u003eThat’s an error.\u003c/ins\u003e\\n  \u003cp\u003eYour client has issued a malformed or illegal request.  \u003cins\u003eThat’s all we know.\u003c/ins\u003e\\n\",\"status\":400,\"rudderJobMetadata\":{\"jobId\":597438246,\"attemptNum\":0,\"userId\":\"\",\"sourceId\":\"2E2b60bs1ybmIKuGvaVxtLT5lYo\",\"destinationId\":\"2E5xYQkj5OVrA3CWexvRLV4b7RH\",\"workspaceId\":\"1sUXvPs0hYgjBxSfjG4gqnRFNoP\",\"secret\":null}},\"message\":\"[GA4 Response Handler] Request failed for destination ga4 with status: 400\",\"statTags\":{\"errorCategory\":\"network\",\"errorType\":\"aborted\",\"destType\":\"GA4\",\"module\":\"destination\",\"implementation\":\"native\",\"feature\":\"dataDelivery\",\"destinationId\":\"2E5xYQkj5OVrA3CWexvRLV4b7RH\",\"workspaceId\":\"1sUXvPs0hYgjBxSfjG4gqnRFNoP\"}}","firstAttemptedAt":"2023-03-30T17:11:25.524Z","content-type":"application/json"}`,
		expected: "[GA4 Response Handler] Request failed for destination ga4 with status: 400",
	},
	{
		inputStr: `{"response":"[GoogleSheets] error :: Failed to insert Payload :: This action would increase the number of cells in the workbook above the limit of 10000000 cells.","firstAttemptedAt":"2023-03-30T17:14:18.737Z","content-type":""}`,
		expected: `[GoogleSheets] error :: Failed to insert Payload :: This action would increase the number of cells in the workbook above the limit of 10000000 cells.`,
	},
	{
		inputStr: `{"response":"{\"msg\":\"Project 9434: The request would increase the number of object fields to 19434 by adding [Application: Invoke Use Positions By Category Mutation, Application: Invoke Use Positions By Category Mutation.request, Application: Invoke Use Positions By Category Mutation.request.cacheID, Application: Invoke Use Positions By Category Mutation.request.metadata, Application: Invoke Use Positions By Category Mutation.request.name, Application: Invoke Use Positions By Category Mutation.request.operationKind, Application: Invoke Use Positions By Category Mutation.request.text, Application: Invoke Use Positions By Category Mutation.variables, Application: Invoke Use Positions By Category Mutation.variables.input, Application: Invoke Use Positions By Category Mutation.variables.input.gigPositionId, Application: Invoke Use Positions By Category Mutation.variables.input.status, Application: Invoke Use Positions By Category Mutation.variables.input.workerId, Application: Pressed  Action Row, Application: Pressed Checkbox], which exceeds the limit of 15000\",\"code\":\"UniqueFieldsLimitExceeded\",\"params\":{\"limit\":15000,\"requestedTotal\":19434,\"newFields\":[\"Application: Invoke Use Positions By Category Mutation\",\"Application: Invoke Use Positions By Category Mutation.request\",\"Application: Invoke Use Positions By Category Mutation.request.cacheID\",\"Application: Invoke Use Positions By Category Mutation.request.metadata\",\"Application: Invoke Use Positions By Category Mutation.request.name\",\"Application: Invoke Use Positions By Category Mutation.request.operationKind\",\"Application: Invoke Use Positions By Category Mutation.request.text\",\"Application: Invoke Use Positions By Category Mutation.variables\",\"Application: Invoke Use Positions By Category Mutation.variables.input\",\"Application: Invoke Use Positions By Category Mutation.variables.input.gigPositionId\",\"Application: Invoke Use Positions By Category Mutation.variables.input.status\",\"Application: Invoke Use Positions By Category Mutation.variables.input.workerId\",\"Application: Pressed  Action Row\",\"Application: Pressed Checkbox\"]}}","firstAttemptedAt":"2023-03-30T17:39:32.189Z","content-type":"application/json"}`,
		expected: `Project 9434: The request would increase the number of object fields to 19434 by adding [Application: Invoke Use Positions By Category Mutation, Application: Invoke Use Positions By Category Mutation.request, Application: Invoke Use Positions By Category Mutation.request.cacheID, Application: Invoke Use Positions By Category Mutation.request.metadata, Application: Invoke Use Positions By Category Mutation.request.name, Application: Invoke Use Positions By Category Mutation.request.operationKind, Application: Invoke Use Positions By Category Mutation.request.text, Application: Invoke Use Positions By Category Mutation.variables, Application: Invoke Use Positions By Category Mutation.variables.input, Application: Invoke Use Positions By Category Mutation.variables.input.gigPositionId, Application: Invoke Use Positions By Category Mutation.variables.input.status, Application: Invoke Use Positions By Category Mutation.variables.input.workerId, Application: Pressed  Action Row, Application: Pressed Checkbox], which exceeds the limit of 15000`,
	},
	{
		inputStr: `{"response":"{\"status\":400,\"destinationResponse\":\"\",\"message\":\"Unable to find conversionActionId for conversion:Order Completed\",\"statTags\":{\"errorCategory\":\"network\",\"errorType\":\"aborted\",\"meta\":\"instrumentation\",\"destType\":\"GOOGLE_ADWORDS_ENHANCED_CONVERSIONS\",\"module\":\"destination\",\"implementation\":\"native\",\"feature\":\"dataDelivery\",\"destinationId\":\"2DiY9INMJbBzfCdjgRlvYzsUl57\",\"workspaceId\":\"1zffyLlFcWBMmv4vvtYldWxEdGg\"}}","firstAttemptedAt":"2023-03-30T17:32:47.268Z","content-type":"application/json"}`,
		expected: `Unable to find conversionActionId for conversion:Order Completed`,
	},
	{
		inputStr: `{"response":"{\"status\":400,\"destinationResponse\":{\"response\":[{\"duplicateResut\":{\"allowSave\":true,\"duplicateRule\":\"Contacts_DR\",\"duplicateRuleEntityType\":\"Contact\",\"errorMessage\":\"You're creating a duplicate record. We recommend you use an existing record instead.\",\"matchResults\":[{\"entityType\":\"Contact\",\"errors\":[],\"matchEngine\":\"ExactMatchEngine\",\"matchRecords\":[{\"additionalInformation\":[],\"fieldDiffs\":[],\"matchConfidence\":100,\"record\":{\"attributes\":{\"type\":\"Contact\",\"url\":\"/services/data/v50.0/sobjects/Contact/0031i000013x2TEAAY\"},\"Id\":\"0031i000013x2TEAAY\"}}],\"rule\":\"Contact_MR\",\"size\":1,\"success\":true}]},\"errorCode\":\"DUPLICATES_DETECTED\",\"message\":\"You're creating a duplicate record. We recommend you use an existing record instead.\"}],\"status\":400,\"rudderJobMetadata\":{\"jobId\":1466739020,\"attemptNum\":0,\"userId\":\"\",\"sourceId\":\"2JF2LaBUedeOfAyt9EoIVJylkKS\",\"destinationId\":\"2JJDsqHbkuIZ89ldOBMaBjPi9L7\",\"workspaceId\":\"26TTcz2tQucRs2xZiGThQzGRk2l\",\"secret\":null,\"destInfo\":{\"authKey\":\"2JJDsqHbkuIZ89ldOBMaBjPi9L7\"}}},\"message\":\"Salesforce Request Failed: \\\"400\\\" due to \\\"You're creating a duplicate record. We recommend you use an existing record instead.\\\", (Aborted) during Salesforce Response Handling\",\"statTags\":{\"errorCategory\":\"network\",\"errorType\":\"aborted\",\"destType\":\"SALESFORCE\",\"module\":\"destination\",\"implementation\":\"native\",\"feature\":\"dataDelivery\",\"destinationId\":\"2JJDsqHbkuIZ89ldOBMaBjPi9L7\",\"workspaceId\":\"26TTcz2tQucRs2xZiGThQzGRk2l\"}}","firstAttemptedAt":"2023-03-30T17:07:52.359Z","content-type":"application/json"}`,
		expected: `You're creating a duplicate record. We recommend you use an existing record instead.`,
	},
	{
		inputStr: `{"response":"{\"destinationResponse\":\"\u003c!DOCTYPE html\u003e\\n\u003chtml lang=\\\"en\\\" id=\\\"facebook\\\"\u003e\\n  \u003chead\u003e\\n    \u003ctitle\u003eFacebook | Error\u003c/title\u003e\\n    \u003cmeta charset=\\\"utf-8\\\"\u003e\\n    \u003cmeta http-equiv=\\\"cache-control\\\" content=\\\"no-cache\\\"\u003e\\n    \u003cmeta http-equiv=\\\"cache-control\\\" content=\\\"no-store\\\"\u003e\\n    \u003cmeta http-equiv=\\\"cache-control\\\" content=\\\"max-age=0\\\"\u003e\\n    \u003cmeta http-equiv=\\\"expires\\\" content=\\\"-1\\\"\u003e\\n    \u003cmeta http-equiv=\\\"pragma\\\" content=\\\"no-cache\\\"\u003e\\n    \u003cmeta name=\\\"robots\\\" content=\\\"noindex,nofollow\\\"\u003e\\n    \u003cstyle\u003e\\n      html, body {\\n        color: #141823;\\n        background-color: #e9eaed;\\n        font-family: Helvetica, Lucida Grande, Arial,\\n                     Tahoma, Verdana, sans-serif;\\n        margin: 0;\\n        padding: 0;\\n        text-align: center;\\n      }\\n\\n      #header {\\n        height: 30px;\\n        padding-bottom: 10px;\\n        padding-top: 10px;\\n        text-align: center;\\n      }\\n\\n      #icon {\\n        width: 30px;\\n      }\\n\\n      h1 {\\n        font-size: 18px;\\n      }\\n\\n      p {\\n        font-size: 13px;\\n      }\\n\\n      #footer {\\n        border-top: 1px solid #ddd;\\n        color: #9197a3;\\n        font-size: 12px;\\n        padding: 5px 8px 6px 0;\\n      }\\n    \u003c/style\u003e\\n  \u003c/head\u003e\\n  \u003cbody\u003e\\n    \u003cdiv id=\\\"header\\\"\u003e\\n      \u003ca href=\\\"//www.facebook.com/\\\"\u003e\\n        \u003cimg id=\\\"icon\\\" src=\\\"//static.facebook.com/images/logos/facebook_2x.png\\\" /\u003e\\n      \u003c/a\u003e\\n    \u003c/div\u003e\\n    \u003cdiv id=\\\"core\\\"\u003e\\n      \u003ch1 id=\\\"sorry\\\"\u003eSorry, something went wrong.\u003c/h1\u003e\\n      \u003cp id=\\\"promise\\\"\u003e\\n        We're working on it and we'll get it fixed as soon as we can.\\n      \u003c/p\u003e\\n      \u003cp id=\\\"back-link\\\"\u003e\\n        \u003ca id=\\\"back\\\" href=\\\"//www.facebook.com/\\\"\u003eGo Back\u003c/a\u003e\\n      \u003c/p\u003e\\n      \u003cdiv id=\\\"footer\\\"\u003e\\n        Facebook\\n        \u003cspan id=\\\"copyright\\\"\u003e\\n          \u0026copy; 2022\\n        \u003c/span\u003e\\n        \u003cspan id=\\\"help-link\\\"\u003e\\n          \u0026#183;\\n          \u003ca id=\\\"help\\\" href=\\\"//www.facebook.com/help/\\\"\u003eHelp Center\u003c/a\u003e\\n        \u003c/span\u003e\\n      \u003c/div\u003e\\n    \u003c/div\u003e\\n    \u003cscript\u003e\\n      document.getElementById('back').onclick = function() {\\n        if (history.length \u003e 1) {\\n          history.back();\\n          return false;\\n        }\\n      };\\n\\n      // Adjust the display based on the window size\\n      if (window.innerHeight \u003c 80 || window.innerWidth \u003c 80) {\\n        // Blank if window is too small\\n        document.body.style.display = 'none';\\n      };\\n      if (window.innerWidth \u003c 200 || window.innerHeight \u003c 150) {\\n        document.getElementById('back-link').style.display = 'none';\\n        document.getElementById('help-link').style.display = 'none';\\n      };\\n      if (window.innerWidth \u003c 200) {\\n        document.getElementById('sorry').style.fontSize = '16px';\\n      };\\n      if (window.innerWidth \u003c 150) {\\n        document.getElementById('promise').style.display = 'none';\\n      };\\n      if (window.innerHeight \u003c 150) {\\n        document.getElementById('sorry').style.margin = '4px 0 0 0';\\n        document.getElementById('sorry').style.fontSize = '14px';\\n        document.getElementById('promise').style.display = 'none';\\n      };\\n    \u003c/script\u003e\\n  \u003c/body\u003e\\n\u003c/html\u003e\\n\",\"message\":\"Request Processed Successfully\",\"status\":502}","firstAttemptedAt":"2023-03-30T17:31:41.884Z","content-type":"application/json"}`,
		expected: "Request Processed Successfully",
	},
	{
		inputStr: `{"response":"{\"status\":402,\"message\":\"[Generic Response Handler] Request failed for destination active_campaign with status: 402\",\"destinationResponse\":{\"response\":\"\",\"status\":402,\"rudderJobMetadata\":{\"jobId\":38065100,\"attemptNum\":0,\"userId\":\"\",\"sourceId\":\"1tzHYMqZp5Cogqe4EeagdfpM7Y3\",\"destinationId\":\"1uFSe3P8gJnacuYQIhWFhRfvCGQ\",\"workspaceId\":\"1tyLn8D94vS107gQwxsmhZaFfP2\",\"secret\":null}},\"statTags\":{\"errorCategory\":\"network\",\"errorType\":\"aborted\",\"destType\":\"ACTIVE_CAMPAIGN\",\"module\":\"destination\",\"implementation\":\"native\",\"feature\":\"dataDelivery\",\"destinationId\":\"1uFSe3P8gJnacuYQIhWFhRfvCGQ\",\"workspaceId\":\"1tyLn8D94vS107gQwxsmhZaFfP2\",\"context\":\"[Native Integration Service] Failure During Processor Transform\"}}","firstAttemptedAt":"2023-03-30T17:03:40.883Z","content-type":"application/json"}`,
		expected: "[Generic Response Handler] Request failed for destination active_campaign with status: 402",
	},
	{
		inputStr: `{"response":"{\"message\":\"Valid data must be provided in the 'attributes', 'events', or 'purchases' fields.\",\"errors\":[{\"type\":\"'external_id' or 'braze_id' or 'user_alias' is required\",\"input_array\":\"attributes\",\"index\":0}]}","firstAttemptedAt":"2023-03-30T17:26:36.847Z","content-type":"application/json"}`,
		expected: "Valid data must be provided in the 'attributes', 'events', or 'purchases' fields.",
	},
	{
		inputStr: `{"fetching_remote_schema_failed":{"attempt":6,"errors":["dial tcp 159.223.171.199:38649: connect: connection refused","dial tcp 159.223.171.199:38649: connect: connection refused","dial tcp 159.223.171.199:38649: connect: connection refused","dial tcp 159.223.171.199:38649: connect: connection refused","dial tcp 159.223.171.199:38649: connect: connection refused","dial tcp 159.223.171.199:38649: connect: connection refused"]}}`,
		expected: `dial tcp 159.223.171.199:38649: connect: connection refused`,
	},
	{
		inputStr: `{"exporting_data_failed":{"attempt":5,"errors":["2 errors occurred:\n1 errors occurred:\nloading identifies table: inserting into original table: pq: Value out of range for 4 bytes.\n1 errors occurred:\nupdate schema: adding columns to warehouse: failed to add columns for table logout in namespace raw_gtm of destination RS:2MeRrhS670OOhZv6gBezLipeVtm with error: pq: must be owner of relation logout","2 errors occurred:\n1 errors occurred:\nupdate schema: adding columns to warehouse: failed to add columns for table logout in namespace raw_gtm of destination RS:2MeRrhS670OOhZv6gBezLipeVtm with error: pq: must be owner of relation logout\n1 errors occurred:\nloading identifies table: inserting into original table: pq: Value out of range for 4 bytes.","2 errors occurred:\n1 errors occurred:\nupdate schema: adding columns to warehouse: failed to add columns for table logout in namespace raw_gtm of destination RS:2MeRrhS670OOhZv6gBezLipeVtm with error: pq: must be owner of relation logout\n1 errors occurred:\nloading identifies table: inserting into original table: pq: Value out of range for 4 bytes.","2 errors occurred:\n1 errors occurred:\nupdate schema: adding columns to warehouse: failed to add columns for table logout in namespace raw_gtm of destination RS:2MeRrhS670OOhZv6gBezLipeVtm with error: pq: must be owner of relation logout\n1 errors occurred:\nloading identifies table: inserting into original table: pq: Value out of range for 4 bytes.","2 errors occurred:\n1 errors occurred:\nupdate schema: adding columns to warehouse: failed to add columns for table logout in namespace raw_gtm of destination RS:2MeRrhS670OOhZv6gBezLipeVtm with error: pq: must be owner of relation logout\n1 errors occurred:\nloading identifies table: inserting into original table: pq: Value out of range for 4 bytes."]}}`,
		expected: "2 errors occurred:\n1 errors occurred:\nloading identifies table: inserting into original table: pq: Value out of range for 4 bytes.\n1 errors occurred:\nupdate schema: adding columns to warehouse: failed to add columns for table logout in namespace raw_gtm of destination RS:2MeRrhS670OOhZv6gBezLipeVtm with error: pq: must be owner of relation logout.2 errors occurred:\n1 errors occurred:\nupdate schema: adding columns to warehouse: failed to add columns for table logout in namespace raw_gtm of destination RS:2MeRrhS670OOhZv6gBezLipeVtm with error: pq: must be owner of relation logout\n1 errors occurred:\nloading identifies table: inserting into original table: pq: Value out of range for 4 bytes.",
	},
	{
		inputStr: `{"response":"{\n\t\t\t\tError: invalid character 'P' looking for beginning of value,\n\t\t\t\t(trRespStCd, trRespBody): (504, Post \"http://transformer.rudder-us-east-1b-blue/v0/destinations/google_adwords_enhanced_conversions/proxy\": context deadline exceeded (Client.Timeout exceeded while awaiting headers)),\n\t\t\t}","firstAttemptedAt":"2023-03-30T17:24:58.068Z","content-type":"text/plain; charset=utf-8"}`,
		expected: "{\n\t\t\t\tError: invalid character 'P' looking for beginning of value,\n\t\t\t\t(trRespStCd, trRespBody): (504, Post \"http://transformer.rudder-us-east-1b-blue/v0/destinations/google_adwords_enhanced_conversions/proxy\": context deadline exceeded (Client.Timeout exceeded while awaiting headers)),\n\t\t\t}",
	},
	{
		inputStr: `{"error":"{\"message\":\"some random message\",\"destinationResponse\":{\"error\":{\"message\":\"Unhandled random error\",\"type\":\"RandomException\",\"code\":5,\"error_subcode\":12,\"fbtrace_id\":\"facebook_px_trace_id_10\"},\"status\":412}}","firstAttemptedAt":"2023-04-20T17:24:58.068Z","content-type":"text/plain; charset=utf-8"}`,
		expected: "Unhandled random error",
	},
	{
		inputStr: `{"error":"unknown error occurred","firstAttemptedAt":"2023-04-21T17:24:58.068Z","content-type":"text/plain; charset=utf-8"}`,
		expected: "unknown error occurred",
	},
}

func TestGetErrorMessageFromResponse(t *testing.T) {
	ext := NewErrorDetailExtractor(logger.NOP)

	for i, tc := range tcs {
		t.Run(fmt.Sprintf("payload-%v", i), func(t *testing.T) {
			msg := ext.GetErrorMessage(tc.inputStr)
			require.Equal(t, tc.expected, msg)
		})
	}
}

func TestExtractErrorDetails(t *testing.T) {
	type depTcOutput struct {
		errorMsg  string
		errorCode string
	}
	type depTc struct {
		caseDescription string
		inputErrMsg     string
		output          depTcOutput
	}
	testCases := []depTc{
		{
			caseDescription: "should validate the deprecation correctly",
			inputErrMsg:     "Offline Conversions API is deprecated from onwards. Please use Conversions API, which is the latest version that supports Offline Conversions API and can be used until.",
			output: depTcOutput{
				errorMsg:  "Offline Conversions API is deprecated from onwards Please use Conversions API which is the latest version that supports Offline Conversions API and can be used until ",
				errorCode: "deprecation",
			},
		},
		{
			caseDescription: "should validate the deprecation correctly even though we have upper-case keywords",
			inputErrMsg:     "Offline Conversions API is DeprEcated from onwards. Please use Conversions API, which is the latest version that supports Offline Conversions API and can be used until.",
			output: depTcOutput{
				errorMsg:  "Offline Conversions API is DeprEcated from onwards Please use Conversions API which is the latest version that supports Offline Conversions API and can be used until ",
				errorCode: "deprecation",
			},
		},
	}

	edr := NewErrorDetailReporter(context.Background(), &configSubscriber{})
	for _, tc := range testCases {
		t.Run(tc.caseDescription, func(t *testing.T) {
			errorDetails := edr.extractErrorDetails(tc.inputErrMsg)

			require.Equal(t, tc.output.errorMsg, errorDetails.ErrorMessage)
			require.Equal(t, tc.output.errorCode, errorDetails.ErrorCode)
		})
	}
}

func TestCleanUpErrorMessage(t *testing.T) {
	ext := NewErrorDetailExtractor(logger.NOP)
	type testCase struct {
		inputStr string
		expected string
	}

	testCases := []testCase{
		{inputStr: "Object with ID '123983489734' is not a valid object", expected: "Object with ID is not a valid object"},
		{inputStr: "http://xyz-rudder.com/v1/endpoint not reachable: context deadline exceeded", expected: " not reachable context deadline exceeded"},
		{inputStr: "http://xyz-rudder.com/v1/endpoint not reachable 172.22.22.10: EOF", expected: " not reachable EOF"},
		{inputStr: "Request failed to process from 16-12-2022:19:30:23T+05:30 due to internal server error", expected: "Request failed to process from due to internal server error"},
		{inputStr: "User with email 'vagor12@bing.com' is not valid", expected: "User with email is not valid"},
		{inputStr: "Allowed timestamp is [15 minutes] into the future", expected: "Allowed timestamp is minutes into the future"},
	}
	for i, tCase := range testCases {
		t.Run(fmt.Sprintf("Case-%d", i), func(t *testing.T) {
			actual := ext.CleanUpErrorMessage(tCase.inputStr)
			require.Equal(t, tCase.expected, actual)
		})
	}
}

func BenchmarkJsonNestedSearch(b *testing.B) {
	extractor := NewErrorDetailExtractor(logger.NOP)

	b.Run("JsonNested used fn", func(b *testing.B) {
		for i := 0; i < len(tcs); i++ {
			extractor.GetErrorMessage(tcs[i].inputStr)
		}
	})
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
				StatusCode:   200,
				ErrorCode:    "",
				ErrorMessage: "",
				EventType:    "identify",
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
				StatusCode:   400,
				ErrorCode:    "",
				ErrorMessage: "bad data sent for transformation",
				EventType:    "identify",
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
				StatusCode:   400,
				ErrorCode:    "",
				ErrorMessage: "bad data sent for transformation",
				EventType:    "identify",
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
				StatusCode:   400,
				ErrorCode:    "",
				ErrorMessage: "user_id information missing",
				EventType:    "identify",
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
				StatusCode:   500,
				ErrorCode:    "",
				ErrorMessage: "Cannot read type property of undefined", // some error during batching
				EventType:    "identify",
			},
			ReportMetadata: types.ReportMetadata{
				ReportedAt: 124335446,
			},
			Count: 15,
		},
	}
	configSubscriber := newConfigSubscriber(logger.NOP)
	ed := NewErrorDetailReporter(context.Background(), configSubscriber)
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
					StatusCode:   dbErrs[0].StatusCode,
					ErrorCode:    dbErrs[0].ErrorCode,
					ErrorMessage: dbErrs[0].ErrorMessage,
					EventType:    dbErrs[0].EventType,
					Count:        10,
				},
				{
					StatusCode:   dbErrs[1].StatusCode,
					ErrorCode:    dbErrs[1].ErrorCode,
					ErrorMessage: dbErrs[1].ErrorMessage,
					EventType:    dbErrs[1].EventType,
					Count:        20,
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
					StatusCode:   dbErrs[3].StatusCode,
					ErrorCode:    dbErrs[3].ErrorCode,
					ErrorMessage: dbErrs[3].ErrorMessage,
					EventType:    dbErrs[3].EventType,
					Count:        20,
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
					StatusCode:   dbErrs[4].StatusCode,
					ErrorCode:    dbErrs[4].ErrorCode,
					ErrorMessage: dbErrs[4].ErrorMessage,
					EventType:    dbErrs[4].EventType,
					Count:        15,
				},
			},
		},
	}

	require.Equal(t, reportResults, reportingMetrics)
}
