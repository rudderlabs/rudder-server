package rules

import (
	"testing"

	"github.com/stretchr/testify/require"

	ptrans "github.com/rudderlabs/rudder-server/processor/transformer"
	"github.com/rudderlabs/rudder-server/utils/types"
	"github.com/rudderlabs/rudder-server/warehouse/transformer/internal/response"
)

func TestIsRudderReservedColumn(t *testing.T) {
	testCases := []struct {
		name       string
		eventType  string
		columnName string
		expected   bool
	}{
		{name: "track", eventType: "track", columnName: "id", expected: true},
		{name: "page", eventType: "page", columnName: "id", expected: true},
		{name: "screen", eventType: "screen", columnName: "id", expected: true},
		{name: "identify", eventType: "identify", columnName: "id", expected: true},
		{name: "group", eventType: "group", columnName: "id", expected: true},
		{name: "alias", eventType: "alias", columnName: "id", expected: true},
		{name: "extract", eventType: "extract", columnName: "id", expected: true},
		{name: "not reserved event type", eventType: "not reserved", columnName: "id", expected: false},
		{name: "not reserved column name", eventType: "track", columnName: "not reserved", expected: false},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, IsRudderReservedColumn(tc.eventType, tc.columnName))
		})
	}
}

func TestExtractRecordID(t *testing.T) {
	testCases := []struct {
		name             string
		metadata         ptrans.Metadata
		expectedRecordID any
		expectedError    error
	}{
		{name: "recordId is nil", metadata: ptrans.Metadata{RecordID: nil}, expectedRecordID: nil, expectedError: response.ErrRecordIDEmpty},
		{name: "recordId is empty", metadata: ptrans.Metadata{RecordID: ""}, expectedRecordID: nil, expectedError: response.ErrRecordIDEmpty},
		{name: "recordId is not empty", metadata: ptrans.Metadata{RecordID: "123"}, expectedRecordID: "123", expectedError: nil},
		{name: "recordId is an object", metadata: ptrans.Metadata{RecordID: map[string]any{"key": "value"}}, expectedRecordID: nil, expectedError: response.ErrRecordIDObject},
		{name: "recordId is a string", metadata: ptrans.Metadata{RecordID: "123"}, expectedRecordID: "123", expectedError: nil},
		{name: "recordId is a number", metadata: ptrans.Metadata{RecordID: 123}, expectedRecordID: 123, expectedError: nil},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			recordID, err := extractRecordID(&tc.metadata)
			require.Equal(t, tc.expectedError, err)
			require.Equal(t, tc.expectedRecordID, recordID)
		})
	}
}

func TestExtractCloudRecordID(t *testing.T) {
	testCases := []struct {
		name             string
		message          types.SingularEventT
		metadata         ptrans.Metadata
		fallbackValue    any
		expectedRecordID any
		expectedError    error
	}{
		{name: "sources version is nil", message: types.SingularEventT{"context": map[string]any{"sources": map[string]any{"version": nil}}}, metadata: ptrans.Metadata{}, fallbackValue: "fallback", expectedRecordID: "fallback", expectedError: nil},
		{name: "sources version is empty", message: types.SingularEventT{"context": map[string]any{"sources": map[string]any{"version": ""}}}, metadata: ptrans.Metadata{}, fallbackValue: "fallback", expectedRecordID: "fallback", expectedError: nil},
		{name: "sources version is not empty", message: types.SingularEventT{"context": map[string]any{"sources": map[string]any{"version": "1.0"}}}, metadata: ptrans.Metadata{RecordID: "123"}, fallbackValue: "fallback", expectedRecordID: "123", expectedError: nil},
		{name: "recordId is nil", message: types.SingularEventT{"context": map[string]any{"sources": map[string]any{"version": "1.0"}}}, metadata: ptrans.Metadata{}, fallbackValue: "fallback", expectedRecordID: nil, expectedError: response.ErrRecordIDEmpty},
		{name: "recordId is empty", message: types.SingularEventT{"context": map[string]any{"sources": map[string]any{"version": "1.0"}}}, metadata: ptrans.Metadata{RecordID: ""}, fallbackValue: "fallback", expectedRecordID: nil, expectedError: response.ErrRecordIDEmpty},
		{name: "recordId is an object", message: types.SingularEventT{"context": map[string]any{"sources": map[string]any{"version": "1.0"}}}, metadata: ptrans.Metadata{RecordID: map[string]any{"key": "value"}}, fallbackValue: "fallback", expectedRecordID: nil, expectedError: response.ErrRecordIDObject},
		{name: "recordId is a string", message: types.SingularEventT{"context": map[string]any{"sources": map[string]any{"version": "1.0"}}}, metadata: ptrans.Metadata{RecordID: "123"}, fallbackValue: "fallback", expectedRecordID: "123", expectedError: nil},
		{name: "recordId is a number", message: types.SingularEventT{"context": map[string]any{"sources": map[string]any{"version": "1.0"}}}, metadata: ptrans.Metadata{RecordID: 123}, fallbackValue: "fallback", expectedRecordID: 123, expectedError: nil},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			recordID, err := extractCloudRecordID(tc.message, &tc.metadata, tc.fallbackValue)
			require.Equal(t, tc.expectedError, err)
			require.Equal(t, tc.expectedRecordID, recordID)
		})
	}
}
