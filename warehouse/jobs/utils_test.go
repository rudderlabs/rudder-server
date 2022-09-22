package jobs

import (
	"testing"
)

func TestValidatePayload(t *testing.T) {
	payloadTests := []struct {
		payload  StartJobReqPayload
		expected bool
	}{
		{
			StartJobReqPayload{
				JobRunID:  "",
				TaskRunID: "",
			},
			false,
		},
		{
			StartJobReqPayload{
				JobRunID:      "abc",
				TaskRunID:     "bbc",
				SourceID:      "cbc",
				DestinationID: "dbc",
			},
			true,
		},
	}
	for _, tt := range payloadTests {
		output := validatePayload(tt.payload)
		if output != tt.expected {
			t.Errorf("error in function validatepayload, expected %t and got %t", tt.expected, output)
		}
	}
}

func TestGetMessagePayloadsFromAsyncJobPayloads(t *testing.T) {
	var asyncPayload = []AsyncJobPayloadT{
		{
			TableName: "abc",
		}, {
			TableName: "bbc",
		}, {
			TableName: "cbc",
		},
	}
	payloadTests := []struct {
		payload   []AsyncJobPayloadT
		tableName string
		expected  int
	}{
		{
			payload:   asyncPayload,
			tableName: "abc",
			expected:  0,
		},
		{
			payload:   asyncPayload,
			tableName: "bbc",
			expected:  1,
		},
		{
			payload:   asyncPayload,
			tableName: "cbc",
			expected:  2,
		},
	}
	for _, tt := range payloadTests {
		output := getSinglePayloadFromBatchPayloadByTableName(tt.payload, tt.tableName)
		if output != tt.expected {
			t.Errorf("error in function validatepayload, expected %d and got %d", tt.expected, output)
		}
	}
}
