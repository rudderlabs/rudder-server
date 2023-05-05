package reporting

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/rudderlabs/rudder-go-kit/logger"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	mock_backendconfig "github.com/rudderlabs/rudder-server/mocks/backend-config"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
	"github.com/rudderlabs/rudder-server/utils/types"
	"github.com/stretchr/testify/require"
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

		reportHandle := NewFromEnvConfig(logger.NOP)

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

func TestReportingBasedOnConfigBackend(t *testing.T) {
	RegisterTestingT(t)
	ctrl := gomock.NewController(t)
	config := mock_backendconfig.NewMockBackendConfig(ctrl)

	configCh := make(chan pubsub.DataEvent)

	var ready sync.WaitGroup
	ready.Add(3)

	var reportingSettings sync.WaitGroup
	reportingSettings.Add(1)

	config.EXPECT().Subscribe(
		gomock.Any(),
		gomock.Eq(backendconfig.TopicBackendConfig),
	).MaxTimes(2).DoAndReturn(func(ctx context.Context, topic backendconfig.Topic) pubsub.DataChannel {
		ready.Done()
		go func() {
			<-ctx.Done()
			close(configCh)
		}()

		return configCh
	})

	f := &Factory{
		EnterpriseToken: "dummy-token",
	}
	f.Setup(config)
	reporting := f.GetReportingInstance()

	var reportingDisabled bool

	go func() {
		ready.Done()
		reportingDisabled = reporting.IsPIIReportingDisabled("testWorkspaceId-1")
		reportingSettings.Done()
	}()

	// When the config backend has not published any event yet
	ready.Wait()
	Expect(reportingDisabled).To(BeFalse())

	configCh <- pubsub.DataEvent{
		Data: map[string]backendconfig.ConfigT{
			"testWorkspaceId-1": {
				WorkspaceID: "testWorkspaceId-1",
				Settings: backendconfig.Settings{
					DataRetention: backendconfig.DataRetention{
						DisableReportingPII: true,
					},
				},
			},
		},
		Topic: string(backendconfig.TopicBackendConfig),
	}

	reportingSettings.Wait()
	Expect(reportingDisabled).To(BeTrue())
}

var jsonBenchCases = []string{
	`{"message": "message-1"}`,
	`{ "a" : { "b": { "message": "messge-2" }}}`,
	// `[{ "a" : { "b": { "message": "messge-2-0" }}}, { "a" : { "b": { "message": "messge-2-1" }}}]`,
	`{"errors": [{"error": { "message": "message-4-1" }}, {"error": { "message": "message-4-2" }} ]}`,
	`{"errors": [{"message": { "error" : "message-5-0" } }, {"message": { "error" : "message-5-1" } }]}`,
}

func TestGetErrorMessageFromResponse(t *testing.T) {
	extractor := NewErrorDetailExtractor(logger.NOP)

	expectedVals := []string{
		"message-1", "messge-2",
		// "messge-2-0",
		"message-4-2", "message-5-1",
	}
	for i, response := range jsonBenchCases {
		t.Run(fmt.Sprintf("case for payload-%d", i), func(t *testing.T) {
			require.Equal(t, extractor.GetErrorMessage(response), expectedVals[i])
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
		{inputStr: "Object with ID '123983489734' is not a valid object", expected: "Object with ID '' is not a valid object"},
		{inputStr: "http://xyz-rudder.com/v1/endpoint not reachable: context deadline exceeded", expected: "not reachable: context deadline exceeded"},
		{inputStr: "http://xyz-rudder.com/v1/endpoint not reachable 172.22.22.10: EOF", expected: "not reachable : EOF"},
		{inputStr: "Request failed to process from 16-12-2022:19:30:23T+05:30 due to internal server error", expected: "Request failed to process from --:::T+: due to internal server error"},
		{inputStr: "User with email 'vagor12@bing.com' is not valid", expected: "User with email '' is not valid"},
		{inputStr: "Allowed timestamp is [15 minutes] into the future", expected: "Allowed timestamp is [ minutes] into the future"},
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
		for i := 0; i < len(jsonBenchCases); i++ {
			// GetErrorMessage(m.Value().(map[string]interface{}))
			extractor.GetErrorMessage(jsonBenchCases[i])
		}
	})
}

func TestGetValFromJSON(t *testing.T) {
	ext := NewErrorDetailExtractor(logger.NOP)
	type getValTc struct {
		inputStr string
		expected string
	}
	tcs := []getValTc{
		{
			inputStr: `{"response":"{\"message\":\"Primary key 'Contact Key' does not exist.\",\"errorcode\":10000,\"documentation\":\"\"}"}`,
			expected: "Primary key 'Contact Key' does not exist.",
		},
		{
			inputStr: `{ "a" : { "b": { "message": "error in looking up" }}}`,
			expected: "error in looking up",
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
			inputStr: `[{ "a" : { "b": { "message": "messge-2-0" }}}, { "a" : { "b": { "message": "messge-2-1" }}}]`,
			expected: `messge-2-0`,
		},
		{
			inputStr: `{"errors": [{"message": { "error" : "message-5-0" } }, {"message": { "error" : "message-5-1" } }]}`,
			expected: `message-5-0`,
		},
		// Think through this one!
		{
			inputStr: `{"response":"[CDM GOOGLESHEETS] Unable to create client for 21xmYbMXCovrqQn5BIszNvcYmur circuit breaker is open, last error: [GoogleSheets] error :: [GoogleSheets] error  :: error in GoogleSheets while unmarshalling credentials json:: invalid character 'v' looking for beginning of value","firstAttemptedAt":"2023-03-30T16:58:32.010Z","content-type":""}`,
			expected: `[CDM GOOGLESHEETS] Unable to create client for 21xmYbMXCovrqQn5BIszNvcYmur circuit breaker is open, last error: [GoogleSheets] error :: [GoogleSheets] error  :: error in GoogleSheets while unmarshalling credentials json:: invalid character 'v' looking for beginning of value`,
		},
		{
			inputStr: `{"response":"{\"errors\":[\"No users with this external_id found\"]}","firstAttemptedAt":"2023-03-30T17:37:58.184Z","content-type":"application/json; charset=utf-8"}`,
			expected: `No users with this external_id found`,
		},
		{
			inputStr: `{"response":"{\"status\":500,\"destinationResponse\":{\"response\":\"[ECONNRESET] :: Connection reset by peer\",\"status\":500,\"rudderJobMetadata\":{\"jobId\":2942260148,\"attemptNum\":0,\"userId\":\"\",\"sourceId\":\"1qWeSkIiAhg3O96KzVVU5MieZHM\",\"destinationId\":\"2IpcvzcMgbLfXgGA4auDrtfEMuI\",\"workspaceId\":\"1nnEnc0tt7k9eFf7bz1GsiU0MFC\",\"secret\":null}},\"message\":\"[GA4 Response Handler] Request failed for destination ga4 with status: 500\",\"statTags\":{\"errorCategory\":\"network\",\"errorType\":\"retryable\",\"destType\":\"GA4\",\"module\":\"destination\",\"implementation\":\"native\",\"feature\":\"dataDelivery\",\"destinationId\":\"2IpcvzcMgbLfXgGA4auDrtfEMuI\",\"workspaceId\":\"1nnEnc0tt7k9eFf7bz1GsiU0MFC\"}}","firstAttemptedAt":"2023-03-30T17:22:26.488Z","content-type":"application/json"}`,
			expected: `[ECONNRESET] :: Connection reset by peer`,
		},
		{
			inputStr: `{"response":"{\"status\":502,\"destinationResponse\":{\"response\":\"\u003chtml\u003e\\r\\n\u003chead\u003e\u003ctitle\u003e502 Bad Gateway\u003c/title\u003e\u003c/head\u003e\\r\\n\u003cbody\u003e\\r\\n\u003ccenter\u003e\u003ch1\u003e502 Bad Gateway\u003c/h1\u003e\u003c/center\u003e\\r\\n\u003c/body\u003e\\r\\n\u003c/html\u003e\\r\\n\",\"status\":502,\"rudderJobMetadata\":{\"jobId\":4946423471,\"attemptNum\":0,\"userId\":\"\",\"sourceId\":\"2CRIZp4OS10sjjGF3uF4guZuGmj\",\"destinationId\":\"2DstxLWX7Oi7gPdPM1CR9CikOko\",\"workspaceId\":\"1yaBlqltp5Y4V2NK8qePowlyafu\",\"secret\":null}},\"message\":\"Request failed  with status: 502\",\"statTags\":{\"errorCategory\":\"network\",\"errorType\":\"retryable\",\"destType\":\"CLEVERTAP\",\"module\":\"destination\",\"implementation\":\"native\",\"feature\":\"dataDelivery\",\"destinationId\":\"2DstxLWX7Oi7gPdPM1CR9CikOko\",\"workspaceId\":\"1yaBlqltp5Y4V2NK8qePowlyafu\"}}","firstAttemptedAt":"2023-03-30T17:11:55.326Z","content-type":"application/json"}`,
			expected: "<html>\r\n<head><title>502 Bad Gateway</title></head>\r\n<body>\r\n<center><h1>502 Bad Gateway</h1></center>\r\n</body>\r\n</html>\r\n",
		},
		{
			inputStr: `{"response":"{\"status\":400,\"destinationResponse\":{\"response\":\"\u003c!DOCTYPE html\u003e\\n\u003chtml lang=en\u003e\\n  \u003cmeta charset=utf-8\u003e\\n  \u003cmeta name=viewport content=\\\"initial-scale=1, minimum-scale=1, width=device-width\\\"\u003e\\n  \u003ctitle\u003eError 400 (Bad Request)!!1\u003c/title\u003e\\n  \u003cstyle\u003e\\n    *{margin:0;padding:0}html,code{font:15px/22px arial,sans-serif}html{background:#fff;color:#222;padding:15px}body{margin:7% auto 0;max-width:390px;min-height:180px;padding:30px 0 15px}* \u003e body{background:url(//www.google.com/images/errors/robot.png) 100% 5px no-repeat;padding-right:205px}p{margin:11px 0 22px;overflow:hidden}ins{color:#777;text-decoration:none}a img{border:0}@media screen and (max-width:772px){body{background:none;margin-top:0;max-width:none;padding-right:0}}#logo{background:url(//www.google.com/images/branding/googlelogo/1x/googlelogo_color_150x54dp.png) no-repeat;margin-left:-5px}@media only screen and (min-resolution:192dpi){#logo{background:url(//www.google.com/images/branding/googlelogo/2x/googlelogo_color_150x54dp.png) no-repeat 0% 0%/100% 100%;-moz-border-image:url(//www.google.com/images/branding/googlelogo/2x/googlelogo_color_150x54dp.png) 0}}@media only screen and (-webkit-min-device-pixel-ratio:2){#logo{background:url(//www.google.com/images/branding/googlelogo/2x/googlelogo_color_150x54dp.png) no-repeat;-webkit-background-size:100% 100%}}#logo{display:inline-block;height:54px;width:150px}\\n  \u003c/style\u003e\\n  \u003ca href=//www.google.com/\u003e\u003cspan id=logo aria-label=Google\u003e\u003c/span\u003e\u003c/a\u003e\\n  \u003cp\u003e\u003cb\u003e400.\u003c/b\u003e \u003cins\u003eThat’s an error.\u003c/ins\u003e\\n  \u003cp\u003eYour client has issued a malformed or illegal request.  \u003cins\u003eThat’s all we know.\u003c/ins\u003e\\n\",\"status\":400,\"rudderJobMetadata\":{\"jobId\":597438246,\"attemptNum\":0,\"userId\":\"\",\"sourceId\":\"2E2b60bs1ybmIKuGvaVxtLT5lYo\",\"destinationId\":\"2E5xYQkj5OVrA3CWexvRLV4b7RH\",\"workspaceId\":\"1sUXvPs0hYgjBxSfjG4gqnRFNoP\",\"secret\":null}},\"message\":\"[GA4 Response Handler] Request failed for destination ga4 with status: 400\",\"statTags\":{\"errorCategory\":\"network\",\"errorType\":\"aborted\",\"destType\":\"GA4\",\"module\":\"destination\",\"implementation\":\"native\",\"feature\":\"dataDelivery\",\"destinationId\":\"2E5xYQkj5OVrA3CWexvRLV4b7RH\",\"workspaceId\":\"1sUXvPs0hYgjBxSfjG4gqnRFNoP\"}}","firstAttemptedAt":"2023-03-30T17:11:25.524Z","content-type":"application/json"}`,
			expected: "<!DOCTYPE html>\n<html lang=en>\n  <meta charset=utf-8>\n  <meta name=viewport content=\"initial-scale=1, minimum-scale=1, width=device-width\">\n  <title>Error 400 (Bad Request)!!1</title>\n  <style>\n    *{margin:0;padding:0}html,code{font:15px/22px arial,sans-serif}html{background:#fff;color:#222;padding:15px}body{margin:7% auto 0;max-width:390px;min-height:180px;padding:30px 0 15px}* > body{background:url(//www.google.com/images/errors/robot.png) 100% 5px no-repeat;padding-right:205px}p{margin:11px 0 22px;overflow:hidden}ins{color:#777;text-decoration:none}a img{border:0}@media screen and (max-width:772px){body{background:none;margin-top:0;max-width:none;padding-right:0}}#logo{background:url(//www.google.com/images/branding/googlelogo/1x/googlelogo_color_150x54dp.png) no-repeat;margin-left:-5px}@media only screen and (min-resolution:192dpi){#logo{background:url(//www.google.com/images/branding/googlelogo/2x/googlelogo_color_150x54dp.png) no-repeat 0% 0%/100% 100%;-moz-border-image:url(//www.google.com/images/branding/googlelogo/2x/googlelogo_color_150x54dp.png) 0}}@media only screen and (-webkit-min-device-pixel-ratio:2){#logo{background:url(//www.google.com/images/branding/googlelogo/2x/googlelogo_color_150x54dp.png) no-repeat;-webkit-background-size:100% 100%}}#logo{display:inline-block;height:54px;width:150px}\n  </style>\n  <a href=//www.google.com/><span id=logo aria-label=Google></span></a>\n  <p><b>400.</b> <ins>That’s an error.</ins>\n  <p>Your client has issued a malformed or illegal request.  <ins>That’s all we know.</ins>\n",
		},
		{
			inputStr: `{"response":"[GoogleSheets] error :: Failed to insert Payload :: This action would increase the number of cells in the workbook above the limit of 10000000 cells.","firstAttemptedAt":"2023-03-30T17:14:18.737Z","content-type":""}`,
			expected: `[GoogleSheets] error :: Failed to insert Payload :: This action would increase the number of cells in the workbook above the limit of 10000000 cells.`,
		},
	}

	for i, tc := range tcs {
		t.Run(fmt.Sprintf("payload-%d", i), func(t *testing.T) {
			res := ext.GetErrorMessageFromResponse(tc.inputStr)
			require.Equal(t, tc.expected, res)
		})
	}
}
