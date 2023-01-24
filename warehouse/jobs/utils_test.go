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
				WorkspaceID:   "ebc",
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
