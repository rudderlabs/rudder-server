package eventfilter

import (
	"testing"

	"github.com/stretchr/testify/require"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/processor/transformer"
)

func TestFilterEventsForHybridMode(t *testing.T) {
	type testDestinationT struct {
		id                 string
		isProcessorEnabled bool
		definitionID       string
		definitionConfig   map[string]interface{}
		config             map[string]interface{}
	}

	type testSourceT struct {
		name          string
		id            string
		sourceDefType string
	}

	type filterConditionT struct {
		destination        testDestinationT
		source             testSourceT
		event              *EventParams
		isSupportedMsgType bool
	}

	type testCaseT struct {
		caseName string
		input    filterConditionT
		expected bool
	}

	testCases := []testCaseT{
		{
			caseName: "when mode is hybrid for web srcType if track event(supportedMsgType) is sent, return true",
			input: filterConditionT{
				isSupportedMsgType: true,
				destination: testDestinationT{
					id: "1",
					definitionConfig: map[string]interface{}{
						"supportedMessageTypes": []interface{}{"track", "page", "group"},
						"supportedSourceTypes":  []interface{}{"web"},
						"supportedConnectionModes": map[string]interface{}{
							"android": []interface{}{
								"cloud",
								"device",
							},
							"web": []interface{}{
								"cloud",
								"device",
								"hybrid",
							},
						},
						"hybridModeCloudEventsFilter": map[string]interface{}{
							"web": map[string]interface{}{
								"messageType": []interface{}{"track", "page"},
							},
						},
					},
					definitionID: "1DefId",
					config: map[string]interface{}{
						"connectionMode": "hybrid",
					},
					isProcessorEnabled: true,
				},
				source: testSourceT{
					name:          "demo-source",
					id:            "1src",
					sourceDefType: "web",
				},
				event: &EventParams{
					MessageType: "track",
				},
			},
			expected: true,
		},
		{
			caseName: "when mode is hybrid for web srcType if group event(does not contain in hybridModeCloudEventsFilter.web.messageType) is sent, return false",
			input: filterConditionT{
				isSupportedMsgType: true,
				destination: testDestinationT{
					id: "1",
					definitionConfig: map[string]interface{}{
						"supportedMessageTypes": []interface{}{"track", "page", "group"},
						"supportedSourceTypes":  []interface{}{"web"},
						"supportedConnectionModes": map[string]interface{}{
							"android": []interface{}{
								"cloud",
								"device",
							},
							"web": []interface{}{
								"cloud",
								"device",
								"hybrid",
							},
						},
						"hybridModeCloudEventsFilter": map[string]interface{}{
							"web": map[string]interface{}{
								"messageType": []interface{}{"track", "page"},
							},
						},
					},
					definitionID: "1DefId",
					config: map[string]interface{}{
						"connectionMode": "hybrid",
					},
					isProcessorEnabled: true,
				},
				source: testSourceT{
					name:          "demo-source",
					id:            "1src",
					sourceDefType: "web",
				},
				event: &EventParams{
					MessageType: "group",
				},
			},
			expected: false,
		},
		{
			caseName: "when mode is cloud for web srcType if track event(supportedMsgType) is sent, return true",
			input: filterConditionT{
				isSupportedMsgType: true,
				destination: testDestinationT{
					id: "1",
					definitionConfig: map[string]interface{}{
						"supportedMessageTypes": []interface{}{"track", "page", "group"},
						"supportedSourceTypes":  []interface{}{"web"},
						"supportedConnectionModes": map[string]interface{}{
							"android": []interface{}{
								"cloud",
								"device",
							},
							"web": []interface{}{
								"cloud",
								"device",
								"hybrid",
							},
						},
						"hybridModeCloudEventsFilter": map[string]interface{}{
							"web": map[string]interface{}{
								"messageType": []interface{}{"track", "page"},
							},
						},
					},
					definitionID: "1DefId",
					config: map[string]interface{}{
						"connectionMode": "cloud",
					},
					isProcessorEnabled: true,
				},
				source: testSourceT{
					name:          "demo-source",
					id:            "1src",
					sourceDefType: "web",
				},
				event: &EventParams{
					MessageType: "track",
				},
			},
			expected: true,
		},
		{
			caseName: "when mode is device(isProcessorEnabled is false) for web srcType if track event(supportedMsgType) is sent, return false",
			input: filterConditionT{
				isSupportedMsgType: true,
				destination: testDestinationT{
					id: "1",
					definitionConfig: map[string]interface{}{
						"supportedMessageTypes": []interface{}{"track", "page", "group"},
						"supportedSourceTypes":  []interface{}{"web"},
						"supportedConnectionModes": map[string]interface{}{
							"android": []interface{}{
								"cloud",
								"device",
							},
							"web": []interface{}{
								"cloud",
								"device",
								"hybrid",
							},
						},
						"hybridModeCloudEventsFilter": map[string]interface{}{
							"web": map[string]interface{}{
								"messageType": []interface{}{"track", "page"},
							},
						},
					},
					definitionID: "1DefId",
					config: map[string]interface{}{
						"connectionMode": "device",
					},
					isProcessorEnabled: false,
				},
				source: testSourceT{
					name:          "demo-source",
					id:            "1src",
					sourceDefType: "web",
				},
				event: &EventParams{
					MessageType: "track",
				},
			},
			expected: false,
		},
		{
			caseName: "when mode is hybrid for android srcType(not present in hybridModeCloudEventsFilter) if track event(supportedMsgType) is sent, return true",
			input: filterConditionT{
				isSupportedMsgType: true,
				destination: testDestinationT{
					id: "1",
					definitionConfig: map[string]interface{}{
						"supportedMessageTypes": []interface{}{"track", "page", "group"},
						"supportedSourceTypes":  []interface{}{"web", "android"},
						"supportedConnectionModes": map[string]interface{}{
							"android": []interface{}{
								"cloud",
								"device",
							},
							"web": []interface{}{
								"cloud",
								"device",
								"hybrid",
							},
						},
						"hybridModeCloudEventsFilter": map[string]interface{}{
							"web": map[string]interface{}{
								"messageType": []interface{}{"track", "page"},
							},
						},
					},
					definitionID: "1DefId",
					config: map[string]interface{}{
						"connectionMode": "hybrid",
					},
					isProcessorEnabled: true,
				},
				source: testSourceT{
					name:          "demo-source",
					id:            "1src",
					sourceDefType: "web",
				},
				event: &EventParams{
					MessageType: "track",
				},
			},
			expected: true,
		},
		{
			caseName: "when mode is hybrid for web srcType but hybridModeCloudEventsFilter is empty map if track event(supportedMsgType) is sent, return true",
			input: filterConditionT{
				isSupportedMsgType: true,
				destination: testDestinationT{
					id: "1",
					definitionConfig: map[string]interface{}{
						"supportedMessageTypes": []interface{}{"track", "page", "group"},
						"supportedSourceTypes":  []interface{}{"web", "android"},
						"supportedConnectionModes": map[string]interface{}{
							"android": []interface{}{
								"cloud",
								"device",
							},
							"web": []interface{}{
								"cloud",
								"device",
								"hybrid",
							},
						},
						"hybridModeCloudEventsFilter": map[string]interface{}{},
					},
					definitionID: "1DefId",
					config: map[string]interface{}{
						"connectionMode": "hybrid",
					},
					isProcessorEnabled: true,
				},
				source: testSourceT{
					name:          "demo-source",
					id:            "1src",
					sourceDefType: "web",
				},
				event: &EventParams{
					MessageType: "track",
				},
			},
			expected: true,
		},
		{
			caseName: "when mode is hybrid for web srcType but hybridModeCloudEventsFilter is itself not present, if track event(supportedMsgType) is sent, return true",
			input: filterConditionT{
				isSupportedMsgType: true,
				destination: testDestinationT{
					id: "1",
					definitionConfig: map[string]interface{}{
						"supportedMessageTypes": []interface{}{"track", "page", "group"},
						"supportedSourceTypes":  []interface{}{"web", "android"},
						"supportedConnectionModes": map[string]interface{}{
							"android": []interface{}{
								"cloud",
								"device",
							},
							"web": []interface{}{
								"cloud",
								"device",
								"hybrid",
							},
						},
					},
					definitionID: "1DefId",
					config: map[string]interface{}{
						"connectionMode": "hybrid",
					},
					isProcessorEnabled: true,
				},
				source: testSourceT{
					name:          "demo-source",
					id:            "1src",
					sourceDefType: "web",
				},
				event: &EventParams{
					MessageType: "track",
				},
			},
			expected: true,
		},
		{
			caseName: "when mode is hybrid for web srcType but hybridModeCloudEventsFilter.web.messageType is an empty array, if track event(supportedMsgType) is sent, return true",
			input: filterConditionT{
				isSupportedMsgType: true,
				destination: testDestinationT{
					id: "1",
					definitionConfig: map[string]interface{}{
						"supportedMessageTypes": []interface{}{"track", "page", "group"},
						"supportedSourceTypes":  []interface{}{"web", "android"},
						"supportedConnectionModes": map[string]interface{}{
							"android": []interface{}{
								"cloud",
								"device",
							},
							"web": []interface{}{
								"cloud",
								"device",
								"hybrid",
							},
						},
						"hybridModeCloudEventsFilter": map[string]interface{}{
							"web": map[string]interface{}{
								"messageType": []interface{}{},
							},
						},
					},
					definitionID: "1DefId",
					config: map[string]interface{}{
						"connectionMode": "hybrid",
					},
					isProcessorEnabled: true,
				},
				source: testSourceT{
					name:          "demo-source",
					id:            "1src",
					sourceDefType: "web",
				},
				event: &EventParams{
					MessageType: "track",
				},
			},
			expected: true,
		},
	}

	destination := &backendconfig.DestinationT{
		ID:   "1",
		Name: "GA4-demo",
		DestinationDefinition: backendconfig.DestinationDefinitionT{
			ID:     "acpo9i",
			Name:   "GA4",
			Config: map[string]interface{}{},
		},
		Config: map[string]interface{}{},
	}
	for _, testCase := range testCases {
		t.Run(testCase.caseName, func(t *testing.T) {
			// Setting test-case values to some default values for destination
			destination.Config = testCase.input.destination.config
			destination.DestinationDefinition.Config = testCase.input.destination.definitionConfig
			destination.DestinationDefinition.ID = testCase.input.destination.definitionID
			destination.ID = testCase.input.destination.id
			destination.IsProcessorEnabled = testCase.input.destination.isProcessorEnabled

			// Setting test-case values for source
			source := &backendconfig.SourceT{
				ID: testCase.input.source.id,
				SourceDefinition: backendconfig.SourceDefinitionT{
					ID:   "src1",
					Type: testCase.input.source.sourceDefType,
				},
			}

			actualOutput := FilterEventsForHybridMode(
				ConnectionModeFilterParams{
					SrcType:          source.SourceDefinition.Type,
					Destination:      destination,
					Event:            testCase.input.event,
					DefaultBehaviour: testCase.input.isSupportedMsgType && testCase.input.destination.isProcessorEnabled,
				},
			)

			require.Equal(t, testCase.expected, actualOutput)
		})
	}
}

func TestConvertToArrayOfType(t *testing.T) {
	type convertTestCases[T EventPropsTypes] struct {
		caseName string
		input    interface{}
		expected []T
	}

	strTestCases := []convertTestCases[string]{
		{
			caseName: "when proper input array of strings(internally) is sent, return proper []string",
			input:    []interface{}{"1", "2", "3"},
			expected: []string{"1", "2", "3"},
		},
		{
			caseName: "when empty array of strings(internally) is sent, return empty string array",
			input:    []interface{}{},
			expected: []string{},
		},
		{
			caseName: "when empty array of different types is sent, return empty string array",
			input:    []interface{}{"1", 2, "omgo"},
			expected: []string{},
		},
		{
			caseName: "when proper(golang type) string array is sent, return same string array",
			input:    []string{"lks", "omgo"},
			expected: []string{"lks", "omgo"},
		},
	}

	for _, strTestCase := range strTestCases {
		t.Run(strTestCase.caseName, func(t *testing.T) {
			actual := ConvertToArrayOfType[string](strTestCase.input)
			require.EqualValues(t, strTestCase.expected, actual)
		})
	}
}

func TestAllowEventToDestTransformation(t *testing.T) {
	type testCaseT struct {
		caseName          string
		transformerEvent  *transformer.TransformerEvent
		expected          bool
		supportedMsgTypes []string
		expectedResp      *transformer.TransformerResponse
	}

	testCases := []testCaseT{
		{
			caseName:          "if message type is invalid, return false with statusCode 400",
			transformerEvent:  &transformer.TransformerEvent{Message: map[string]interface{}{"type": ""}},
			expected:          false,
			supportedMsgTypes: []string{"track"},
			expectedResp: &transformer.TransformerResponse{
				Output:     map[string]interface{}{"type": ""},
				StatusCode: 400,
				Error:      "Invalid message type. Type assertion failed",
			},
		},
		{
			caseName:          "if message type is unsupported, return false with statusCode 298",
			transformerEvent:  &transformer.TransformerEvent{Message: map[string]interface{}{"type": "identify"}},
			expected:          false,
			supportedMsgTypes: []string{"track"},
			expectedResp: &transformer.TransformerResponse{
				Output:     map[string]interface{}{"type": "identify"},
				StatusCode: 298,
				Error:      "Message type not supported",
			},
		},
		{
			caseName:          "if event is filtered due to FilterEventsForHybridMode, return statusCode 298",
			transformerEvent:  &transformer.TransformerEvent{Message: map[string]interface{}{"type": "track"}, Metadata: transformer.Metadata{}},
			expected:          false,
			supportedMsgTypes: []string{"track"},
			expectedResp: &transformer.TransformerResponse{
				Output:     map[string]interface{}{"type": "track"},
				StatusCode: 298,
				Error:      "Filtering event based on hybridModeFilter",
			},
		},
		{
			caseName:          "if event is legit, return true with nil response",
			transformerEvent:  &transformer.TransformerEvent{Message: map[string]interface{}{"type": "track"}, Destination: backendconfig.DestinationT{IsProcessorEnabled: true}},
			expected:          true,
			supportedMsgTypes: []string{"track"},
			expectedResp:      nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.caseName, func(t *testing.T) {
			allow, resp := AllowEventToDestTransformation(tc.transformerEvent, tc.supportedMsgTypes)
			require.Equal(t, tc.expected, allow)
			require.Equal(t, tc.expectedResp, resp)
		})
	}
}
