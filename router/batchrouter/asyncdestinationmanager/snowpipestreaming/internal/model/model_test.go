package model

import (
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/require"

	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestChannelResponse_GenerateSnowpipeSchema(t *testing.T) {
	testCases := []struct {
		name        string
		tableSchema map[string]ColumnInfo
		expected    whutils.ModelTableSchema
	}{
		{
			name: "Valid types with scale",
			tableSchema: map[string]ColumnInfo{
				"column1": {Type: lo.ToPtr("VARCHAR(16777216)")},
				"column2": {Type: lo.ToPtr("NUMBER(2,0)"), Scale: lo.ToPtr(2.0)},
				"column3": {Type: lo.ToPtr("NUMBER(2,0)"), Scale: lo.ToPtr(0.0)},
				"column4": {Type: lo.ToPtr("NUMBER(2,0)")},
				"column5": {Type: lo.ToPtr("BOOLEAN")},
				"column6": {Type: lo.ToPtr("TIMESTAMP_TZ(9)"), Scale: lo.ToPtr(9.0)},
				"column7": {Type: lo.ToPtr("TIMESTAMP_TZ(9)"), Scale: lo.ToPtr(9.5)},
				"column8": {Type: lo.ToPtr("VARCHAR")},
			},
			expected: whutils.ModelTableSchema{
				"column1": "string",
				"column2": "float",
				"column3": "int",
				"column4": "int",
				"column5": "boolean",
				"column6": "datetime",
				"column7": "datetime",
				"column8": "string",
			},
		},
		{
			name: "Unknown type",
			tableSchema: map[string]ColumnInfo{
				"column1": {Type: lo.ToPtr("VARCHAR(16777216)")},
				"column2": {Type: lo.ToPtr("UNKNOWN")},
			},
			expected: whutils.ModelTableSchema{
				"column1": "string",
			},
		},
		{
			name: "Missing scale for number",
			tableSchema: map[string]ColumnInfo{
				"column1": {Type: lo.ToPtr("NUMBER(2,0)")},
			},
			expected: whutils.ModelTableSchema{
				"column1": "int",
			},
		},
		{
			name: "Missing type",
			tableSchema: map[string]ColumnInfo{
				"column1": {Scale: lo.ToPtr(2.0)},
			},
			expected: whutils.ModelTableSchema{},
		},
		{
			name:        "Empty table schema",
			tableSchema: map[string]ColumnInfo{},
			expected:    nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, generateSnowpipeSchema(tc.tableSchema))
		})
	}
}

func TestChannelResponse_UnmarshalJSON(t *testing.T) {
	testCases := []struct {
		name             string
		response         []byte
		expectedResponse ChannelResponse
		wantError        bool
	}{
		{
			name:     "Valid success response",
			response: []byte(`{"success":true,"channelId":"channelId","channelName":"channelName","clientName":"clientName","valid":true,"deleted":false,"tableSchema":{"EVENT":{"type":"VARCHAR(16777216)","logicalType":"TEXT","precision":null,"scale":null,"byteLength":16777216,"length":16777216,"nullable":true},"ID":{"type":"VARCHAR(16777216)","logicalType":"TEXT","precision":null,"scale":null,"byteLength":16777216,"length":16777216,"nullable":true},"TIMESTAMP":{"type":"TIMESTAMP_TZ(9)","logicalType":"TIMESTAMP_TZ","precision":0,"scale":9,"byteLength":null,"length":null,"nullable":true}}}`),
			expectedResponse: ChannelResponse{
				Success:     true,
				ChannelID:   "channelId",
				ChannelName: "channelName",
				ClientName:  "clientName",
				Valid:       true,
				Deleted:     false,
				SnowpipeSchema: whutils.ModelTableSchema{
					"EVENT":     "string",
					"ID":        "string",
					"TIMESTAMP": "datetime",
				},
			},
		},
		{
			name:     "Valid failure response",
			response: []byte(`{"success":false,"error":"Open channel request failed: HTTP Status: 400 ErrorBody: {\n  \"status_code\" : 4,\n  \"message\" : \"The supplied table does not exist or is not authorized.\"\n}.","code":"ERR_TABLE_DOES_NOT_EXIST_OR_NOT_AUTHORIZED","snowflakeSDKCode":"0007","snowflakeAPIHttpCode":400,"snowflakeAPIStatusCode":4,"snowflakeAPIMessage":"The supplied table does not exist or is not authorized."}`),
			expectedResponse: ChannelResponse{
				Success: false,
				Error:   "Open channel request failed: HTTP Status: 400 ErrorBody: {\n  \"status_code\" : 4,\n  \"message\" : \"The supplied table does not exist or is not authorized.\"\n}.", Code: "ERR_TABLE_DOES_NOT_EXIST_OR_NOT_AUTHORIZED",
				SnowflakeSDKCode:       "0007",
				SnowflakeAPIHttpCode:   400,
				SnowflakeAPIStatusCode: 4,
				SnowflakeAPIMessage:    "The supplied table does not exist or is not authorized.",
			},
		},
		{
			name:      "Malformed JSON",
			response:  []byte(`{"success":true,"channelId`),
			wantError: true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var response ChannelResponse
			err := json.Unmarshal(tc.response, &response)
			if tc.wantError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expectedResponse, response)
		})
	}
}
