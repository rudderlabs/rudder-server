package eventfilter

import (
	"testing"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/stretchr/testify/require"
)

func TestDestinationFilterCondition(t *testing.T) {
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
			caseName: "when mode is cloud for web srcType if track event is sent, return true",
			input: filterConditionT{
				isSupportedMsgType: true,
				destination: testDestinationT{
					id: "1",
					definitionConfig: map[string]interface{}{
						"supportedMessageTypes": []interface{}{"track", "page", "group"},
						"supportedSourceTypes":  []interface{}{"web"},
						"supportedConnectionModes": map[string]interface{}{
							"web": map[string]interface{}{
								"cloud": map[string]interface{}{
									"messageType": map[string]interface{}{
										"allowedValues": []interface{}{"track", "group"},
									},
								},
								"hybrid": map[string]interface{}{
									"messageType": map[string]interface{}{
										"allowAll": true,
									},
								},
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
			caseName: "when mode is hybrid for web srcType if page event is sent, return false",
			input: filterConditionT{
				isSupportedMsgType: true,
				destination: testDestinationT{
					id: "1",
					definitionConfig: map[string]interface{}{
						"supportedMessageTypes": []interface{}{"track", "page", "group"},
						"supportedSourceTypes":  []interface{}{"web"},
						"supportedConnectionModes": map[string]interface{}{
							"web": map[string]interface{}{
								"cloud": map[string]interface{}{
									"messageType": map[string]interface{}{
										"allowedValues": []interface{}{"track", "group"},
									},
								},
								"hybrid": map[string]interface{}{
									"messageType": map[string]interface{}{
										"allowAll": true,
									},
								},
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
					MessageType: "page",
				},
			},
			expected: false,
		},
		{
			caseName: "when mode is device for web srcType if track event is sent, return false",
			input: filterConditionT{
				isSupportedMsgType: true,
				destination: testDestinationT{
					id: "2",
					definitionConfig: map[string]interface{}{
						"supportedMessageTypes": []interface{}{"track", "page", "group"},
						"supportedSourceTypes":  []interface{}{"web"},
						"supportedConnectionModes": map[string]interface{}{
							"web": map[string]interface{}{
								"cloud": map[string]interface{}{
									"messageType": map[string]interface{}{
										"allowedValues": []interface{}{"track", "group"},
									},
								},
								"device": map[string]interface{}{
									"messageType": map[string]interface{}{
										"allowAll": true,
									},
								},
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
			caseName: "when mode is hybrid for web srcType if event type is empty, return false",
			input: filterConditionT{
				isSupportedMsgType: false,
				destination: testDestinationT{
					id: "1",
					definitionConfig: map[string]interface{}{
						"supportedMessageTypes": []interface{}{"track", "page", "group"},
						"supportedSourceTypes":  []interface{}{"web"},
						"supportedConnectionModes": map[string]interface{}{
							"web": map[string]interface{}{
								"cloud": map[string]interface{}{
									"messageType": map[string]interface{}{
										"allowedValues": []interface{}{"track", "group"},
									},
								},
								"hybrid": map[string]interface{}{
									"messageType": map[string]interface{}{
										"allowAll": true,
									},
								},
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
				event: &EventParams{},
			},
			expected: false,
		},
		{
			caseName: "when mode is not present for web srcType if event type is track, return true",
			input: filterConditionT{
				isSupportedMsgType: true,
				destination: testDestinationT{
					id: "1",
					definitionConfig: map[string]interface{}{
						"supportedMessageTypes": []interface{}{"track", "page", "group"},
						"supportedSourceTypes":  []interface{}{"web"},
						"supportedConnectionModes": map[string]interface{}{
							"web": map[string]interface{}{
								"cloud": map[string]interface{}{
									"messageType": map[string]interface{}{
										"allowedValues": []interface{}{"track", "group"},
									},
								},
								"hybrid": map[string]interface{}{
									"messageType": map[string]interface{}{
										"allowAll": true,
									},
								},
							},
						},
					},
					definitionID:       "1DefId",
					config:             map[string]interface{}{},
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
			caseName: "when supportedConnectionModes is not present, connectionMode is cloud for web srcType and if event type is track, return true",
			input: filterConditionT{
				isSupportedMsgType: true,
				destination: testDestinationT{
					id: "1",
					definitionConfig: map[string]interface{}{
						"supportedMessageTypes": []interface{}{"track", "page", "group"},
						"supportedSourceTypes":  []interface{}{"web"},
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
			caseName: "when connected sourceType(android) is not present in supportedConnectionModes but present in supportedSourceTypes with cloud connectionMode if event type is track, return true",
			input: filterConditionT{
				isSupportedMsgType: true,
				destination: testDestinationT{
					id: "1",
					definitionConfig: map[string]interface{}{
						"supportedMessageTypes": []interface{}{"track", "page", "group"},
						"supportedSourceTypes":  []interface{}{"web", "android"},
						"supportedConnectionModes": map[string]interface{}{
							"web": map[string]interface{}{
								"cloud": map[string]interface{}{
									"messageType": map[string]interface{}{
										"allowedValues": []interface{}{"track", "group"},
									},
								},
								"hybrid": map[string]interface{}{
									"messageType": map[string]interface{}{
										"allowAll": true,
									},
								},
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
					sourceDefType: "android",
				},
				event: &EventParams{
					MessageType: "track",
				},
			},
			expected: true,
		},
		{
			caseName: "when connected sourceType(web) is present in supportedConnectionModes but connectionMode(hybrid) is not available if event type is track, return false",
			input: filterConditionT{
				isSupportedMsgType: true,
				destination: testDestinationT{
					id: "1",
					definitionConfig: map[string]interface{}{
						"supportedMessageTypes": []interface{}{"track", "page", "group"},
						"supportedSourceTypes":  []interface{}{"web", "android"},
						"supportedConnectionModes": map[string]interface{}{
							"web": map[string]interface{}{
								"cloud": map[string]interface{}{
									"messageType": map[string]interface{}{
										"allowedValues": []interface{}{"track", "group"},
									},
								},
								"device": map[string]interface{}{
									"messageType": map[string]interface{}{
										"allowAll": true,
									},
								},
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
			expected: false,
		},
		{
			caseName: "when supportedConnectionModes.web.cloud is an empty map({}) destination is cloud-mode for web srcType if track event is sent, return true",
			input: filterConditionT{
				isSupportedMsgType: true,
				destination: testDestinationT{
					id: "1",
					definitionConfig: map[string]interface{}{
						"supportedMessageTypes": []interface{}{"track", "page", "group"},
						"supportedSourceTypes":  []interface{}{"web"},
						"supportedConnectionModes": map[string]interface{}{
							"web": map[string]interface{}{
								"cloud": map[string]interface{}{},
								"hybrid": map[string]interface{}{
									"messageType": map[string]interface{}{
										"allowAll": true,
									},
								},
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
			caseName: "when mode is cloud for web srcType & allowedMessageTypes is [track, group] if page event is sent, return false",
			input: filterConditionT{
				isSupportedMsgType: true,
				destination: testDestinationT{
					id: "1",
					definitionConfig: map[string]interface{}{
						"supportedMessageTypes": []interface{}{"track", "page", "group"},
						"supportedSourceTypes":  []interface{}{"web"},
						"supportedConnectionModes": map[string]interface{}{
							"web": map[string]interface{}{
								"cloud": map[string]interface{}{
									"messageType": map[string]interface{}{
										"allowedValues": []interface{}{"track", "group"},
									},
								},
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
					MessageType: "page",
				},
			},
			expected: false,
		},
		{
			caseName: "when mode is cloud for web srcType & allowedMessageTypes is [track, group] if track event is sent, return true",
			input: filterConditionT{
				isSupportedMsgType: true,
				destination: testDestinationT{
					id: "1",
					definitionConfig: map[string]interface{}{
						"supportedMessageTypes": []interface{}{"track", "page", "group"},
						"supportedSourceTypes":  []interface{}{"web"},
						"supportedConnectionModes": map[string]interface{}{
							"web": map[string]interface{}{
								"cloud": map[string]interface{}{
									"messageType": map[string]interface{}{
										"allowedValues": []interface{}{"track", "group"},
									},
								},
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
			caseName: "when mode is cloud for web srcType & allowAll is true if page event is sent, return false",
			input: filterConditionT{
				isSupportedMsgType: true,
				destination: testDestinationT{
					id: "1",
					definitionConfig: map[string]interface{}{
						"supportedMessageTypes": []interface{}{"track", "page", "group"},
						"supportedSourceTypes":  []interface{}{"web"},
						"supportedConnectionModes": map[string]interface{}{
							"web": map[string]interface{}{
								"cloud": map[string]interface{}{
									"messageType": map[string]interface{}{
										"allowAll": true,
									},
								},
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
					MessageType: "page",
				},
			},
			expected: true,
		},
		{
			caseName: "when mode is cloud for web srcType & allowAll is true if identify event(not in supportedMessageTypes) is sent, return false",
			input: filterConditionT{
				isSupportedMsgType: false,
				destination: testDestinationT{
					id: "1",
					definitionConfig: map[string]interface{}{
						"supportedMessageTypes": []interface{}{"track", "page", "group"},
						"supportedSourceTypes":  []interface{}{"web"},
						"supportedConnectionModes": map[string]interface{}{
							"web": map[string]interface{}{
								"cloud": map[string]interface{}{
									"messageType": map[string]interface{}{
										"allowAll": true,
									},
								},
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
					MessageType: "identify",
				},
			},
			expected: false,
		},

		{
			caseName: "when mode is cloud for web srcType & allowAll is true & allowedValues contains [track] if page event(in supportedMessageTypes) is sent, return true",
			input: filterConditionT{
				isSupportedMsgType: true,
				destination: testDestinationT{
					id: "1",
					definitionConfig: map[string]interface{}{
						"supportedMessageTypes": []interface{}{"track", "page", "group"},
						"supportedSourceTypes":  []interface{}{"web"},
						"supportedConnectionModes": map[string]interface{}{
							"web": map[string]interface{}{
								"cloud": map[string]interface{}{
									"messageType": map[string]interface{}{
										"allowAll":    true,
										"allowValues": []interface{}{"track"},
									},
								},
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
					MessageType: "page",
				},
			},
			expected: true,
		},
		{
			caseName: "when mode is cloud for web srcType & allowAll, allowedValues not present if page event(in supportedMessageTypes) is sent, return true",
			input: filterConditionT{
				isSupportedMsgType: true,
				destination: testDestinationT{
					id: "1",
					definitionConfig: map[string]interface{}{
						"supportedMessageTypes": []interface{}{"track", "page", "group"},
						"supportedSourceTypes":  []interface{}{"web"},
						"supportedConnectionModes": map[string]interface{}{
							"web": map[string]interface{}{
								"cloud": map[string]interface{}{
									"messageType": map[string]interface{}{},
								},
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
					MessageType: "page",
				},
			},
			expected: true,
		},
		{
			caseName: "when mode is cloud for web srcType & allowAll, allowedValues not present if alias event(not in supportedMessageTypes) is sent, return false",
			input: filterConditionT{
				isSupportedMsgType: false,
				destination: testDestinationT{
					id: "1",
					definitionConfig: map[string]interface{}{
						"supportedMessageTypes": []interface{}{"track", "page", "group"},
						"supportedSourceTypes":  []interface{}{"web"},
						"supportedConnectionModes": map[string]interface{}{
							"web": map[string]interface{}{
								"cloud": map[string]interface{}{
									"messageType": map[string]interface{}{},
								},
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
					MessageType: "alias",
				},
			},
			expected: false,
		},
		{
			caseName: "when mode is cloud for web srcType & allowedValues contains [identify] but not present in supportedMessageTypes if identify event is sent, return false",
			input: filterConditionT{
				isSupportedMsgType: false,
				destination: testDestinationT{
					id: "1",
					definitionConfig: map[string]interface{}{
						"supportedMessageTypes": []interface{}{"track", "page", "group"},
						"supportedSourceTypes":  []interface{}{"web"},
						"supportedConnectionModes": map[string]interface{}{
							"web": map[string]interface{}{
								"cloud": map[string]interface{}{
									"messageType": map[string]interface{}{
										"allowValues": []interface{}{"identify"},
									},
								},
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
					MessageType: "identify",
				},
			},
			expected: false,
		},
		{
			caseName: "when mode is cloud for web srcType & allowAll is false & allowedValues contains [track] if track event(in supportedMessageTypes) is sent, return true",
			input: filterConditionT{
				isSupportedMsgType: true,
				destination: testDestinationT{
					id: "1",
					definitionConfig: map[string]interface{}{
						"supportedMessageTypes": []interface{}{"track", "page", "group"},
						"supportedSourceTypes":  []interface{}{"web"},
						"supportedConnectionModes": map[string]interface{}{
							"web": map[string]interface{}{
								"cloud": map[string]interface{}{
									"messageType": map[string]interface{}{
										"allowAll":    false,
										"allowValues": []interface{}{"track"},
									},
								},
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
			caseName: "when mode is `cloud` for `cloud` srcType & supportedConnectionModes is in previous version if track event is sent, return true",
			input: filterConditionT{
				isSupportedMsgType: true,
				destination: testDestinationT{
					id: "3",
					definitionConfig: map[string]interface{}{
						"supportedMessageTypes": []interface{}{"track", "page", "group"},
						"supportedSourceTypes":  []interface{}{"cloud", "android", "ios", "flutter", "reactnative"},
						"supportedConnectionModes": map[string]interface{}{
							"cloud":       []interface{}{"cloud", "device"},
							"android":     []interface{}{"cloud", "device"},
							"ios":         []interface{}{"cloud", "device"},
							"flutter":     []interface{}{"cloud", "device"},
							"reactnative": []interface{}{"cloud", "device"},
						},
					},
					definitionID: "2DefId",
					config: map[string]interface{}{
						"connectionMode": "cloud",
					},
					isProcessorEnabled: true,
				},
				source: testSourceT{
					name:          "demo-source",
					id:            "1src",
					sourceDefType: "cloud",
				},
				event: &EventParams{
					MessageType: "track",
				},
			},
			expected: true,
		},
		{
			caseName: "when mode is `cloud` for `cloud` srcType & supportedConnectionModes is in previous version if alias event is sent, return false",
			input: filterConditionT{
				isSupportedMsgType: false,
				destination: testDestinationT{
					id: "3",
					definitionConfig: map[string]interface{}{
						"supportedMessageTypes": []interface{}{"track", "page", "group"},
						"supportedSourceTypes":  []interface{}{"cloud", "android", "ios", "flutter", "reactnative"},
						"supportedConnectionModes": map[string]interface{}{
							"cloud":       []interface{}{"cloud", "device"},
							"android":     []interface{}{"cloud", "device"},
							"ios":         []interface{}{"cloud", "device"},
							"flutter":     []interface{}{"cloud", "device"},
							"reactnative": []interface{}{"cloud", "device"},
						},
					},
					definitionID: "2DefId",
					config: map[string]interface{}{
						"connectionMode": "cloud",
					},
					isProcessorEnabled: true,
				},
				source: testSourceT{
					name:          "demo-source",
					id:            "1src",
					sourceDefType: "cloud",
				},
				event: &EventParams{
					MessageType: "alias",
				},
			},
			expected: false,
		},
		{
			caseName: "when mode is `cloud` for `cloud` srcType(not in supportedConnectionModes) & supportedConnectionModes is in previous version if track event is sent, return true",
			input: filterConditionT{
				isSupportedMsgType: true,
				destination: testDestinationT{
					id: "4",
					definitionConfig: map[string]interface{}{
						"supportedMessageTypes": []interface{}{"track", "page", "group"},
						"supportedSourceTypes":  []interface{}{"cloud", "android", "ios", "flutter", "reactnative"},
						"supportedConnectionModes": map[string]interface{}{
							"android":     []interface{}{"cloud", "device"},
							"ios":         []interface{}{"cloud", "device"},
							"flutter":     []interface{}{"cloud", "device"},
							"reactnative": []interface{}{"cloud", "device"},
						},
					},
					definitionID: "3DefId",
					config: map[string]interface{}{
						"connectionMode": "cloud",
					},
					isProcessorEnabled: true,
				},
				source: testSourceT{
					name:          "demo-source",
					id:            "1src",
					sourceDefType: "cloud",
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

			actualOutput, _ := FilterUsingSupportedConnectionModes(
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
