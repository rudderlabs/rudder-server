package reporting

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestGetErrorCode_DeprecationDetection(t *testing.T) {
	ext := NewErrorDetailExtractor(logger.NOP, config.New())

	testCases := []struct {
		name         string
		errorMessage string
		statTags     map[string]string
		destType     string
		expectedCode string
		description  string
	}{
		{
			name:         "warehouse destination should not return deprecation error",
			errorMessage: "API version deprecated and no longer supported",
			statTags:     map[string]string{},
			destType:     "DELTALAKE",
			expectedCode: "",
			description:  "DELTALAKE warehouse destination should be skipped for deprecation detection",
		},
		{
			name:         "warehouse destination should not return deprecation error for SNOWFLAKE",
			errorMessage: "API version deprecated and no longer supported",
			statTags:     map[string]string{},
			destType:     "SNOWFLAKE",
			expectedCode: "",
			description:  "SNOWFLAKE warehouse destination should be skipped for deprecation detection",
		},
		{
			name:         "warehouse destination should not return deprecation error for BQ",
			errorMessage: "API version deprecated and no longer supported",
			statTags:     map[string]string{},
			destType:     "BQ",
			expectedCode: "",
			description:  "BQ warehouse destination should be skipped for deprecation detection",
		},
		{
			name:         "non-warehouse destination should return deprecation error",
			errorMessage: "API version deprecated and no longer supported",
			statTags:     map[string]string{},
			destType:     "FACEBOOK_PIXEL",
			expectedCode: "deprecation",
			description:  "Non-warehouse destination should still detect deprecation errors",
		},
		{
			name:         "non-warehouse destination should return deprecation error for GOOGLE_ANALYTICS",
			errorMessage: "API version deprecated and no longer supported",
			statTags:     map[string]string{},
			destType:     "GOOGLE_ANALYTICS",
			expectedCode: "deprecation",
			description:  "Non-warehouse destination should still detect deprecation errors",
		},
		{
			name:         "warehouse destination should return error code from stat tags",
			errorMessage: "API version deprecated and no longer supported",
			statTags:     map[string]string{"errorCategory": "network", "errorType": "aborted"},
			destType:     "DELTALAKE",
			expectedCode: "network:aborted",
			description:  "Warehouse destination should still return error codes from stat tags",
		},
		{
			name:         "non-warehouse destination should return error code from stat tags",
			errorMessage: "API version deprecated and no longer supported",
			statTags:     map[string]string{"errorCategory": "network", "errorType": "aborted"},
			destType:     "FACEBOOK_PIXEL",
			expectedCode: "network:aborted",
			description:  "Non-warehouse destination should still return error codes from stat tags",
		},
		{
			name:         "empty destination type should not return deprecation error",
			errorMessage: "API version deprecated and no longer supported",
			statTags:     map[string]string{},
			destType:     "",
			expectedCode: "deprecation",
			description:  "Empty destination type should be treated as non-warehouse",
		},
		{
			name:         "unknown destination type should return deprecation error",
			errorMessage: "API version deprecated and no longer supported",
			statTags:     map[string]string{},
			destType:     "UNKNOWN_DESTINATION",
			expectedCode: "deprecation",
			description:  "Unknown destination type should be treated as non-warehouse",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result := ext.GetErrorCode(tc.errorMessage, tc.statTags, tc.destType)
			require.Equal(t, tc.expectedCode, result, tc.description)
		})
	}
}

func TestGetErrorCode_AllWarehouseDestinations(t *testing.T) {
	ext := NewErrorDetailExtractor(logger.NOP, config.New())
	errorMessage := "API version deprecated and no longer supported"
	statTags := map[string]string{}

	// Test all warehouse destinations
	for _, destType := range warehouseutils.WarehouseDestinations {
		t.Run("warehouse_destination_"+destType, func(t *testing.T) {
			t.Parallel()
			result := ext.GetErrorCode(errorMessage, statTags, destType)
			require.Equal(t, "", result, "Warehouse destination %s should not return deprecation error", destType)
		})
	}
}

func TestGetErrorCode_EdgeCases(t *testing.T) {
	ext := NewErrorDetailExtractor(logger.NOP, config.New())

	testCases := []struct {
		name         string
		errorMessage string
		statTags     map[string]string
		destType     string
		expectedCode string
		description  string
	}{
		{
			name:         "warehouse destination with non-deprecation error",
			errorMessage: "Connection timeout",
			statTags:     map[string]string{},
			destType:     "DELTALAKE",
			expectedCode: "",
			description:  "Warehouse destination should return empty for non-deprecation errors",
		},
		{
			name:         "non-warehouse destination with non-deprecation error",
			errorMessage: "Connection timeout",
			statTags:     map[string]string{},
			destType:     "FACEBOOK_PIXEL",
			expectedCode: "",
			description:  "Non-warehouse destination should return empty for non-deprecation errors",
		},
		{
			name:         "warehouse destination with empty error message",
			errorMessage: "",
			statTags:     map[string]string{},
			destType:     "DELTALAKE",
			expectedCode: "",
			description:  "Warehouse destination should return empty for empty error messages",
		},
		{
			name:         "non-warehouse destination with empty error message",
			errorMessage: "",
			statTags:     map[string]string{},
			destType:     "FACEBOOK_PIXEL",
			expectedCode: "",
			description:  "Non-warehouse destination should return empty for empty error messages",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result := ext.GetErrorCode(tc.errorMessage, tc.statTags, tc.destType)
			require.Equal(t, tc.expectedCode, result, tc.description)
		})
	}
}

func TestExtractErrorDetails(t *testing.T) {
	testCases := []struct {
		caseDescription string
		inputErrMsg     string
		expectedMsg     string
		expectedCode    string
		statTags        map[string]string
	}{
		{
			caseDescription: "should validate the deprecation correctly",
			inputErrMsg:     "Offline Conversions API is deprecated from onwards. Please use Conversions API, which is the latest version that supports Offline Conversions API and can be used until.",
			expectedMsg:     "Offline Conversions API is deprecated from onwards Please use Conversions API which is the latest version that supports Offline Conversions API and can be used until",
			expectedCode:    "deprecation",
		},
		{
			caseDescription: "should validate the deprecation correctly even though we have upper-case keywords",
			inputErrMsg:     "Offline Conversions API is DeprEcated from onwards. Please use Conversions API, which is the latest version that supports Offline Conversions API and can be used until.",
			expectedMsg:     "Offline Conversions API is DeprEcated from onwards Please use Conversions API which is the latest version that supports Offline Conversions API and can be used until",
			expectedCode:    "deprecation",
		},
		{
			caseDescription: "should use statTags to compute errorCode",
			statTags: map[string]string{
				"errorCategory": "dataValidation",
				"errorType":     "configuration",
			},
			inputErrMsg:  "Some error",
			expectedMsg:  "Some error",
			expectedCode: "dataValidation:configuration",
		},
		{
			caseDescription: "should use complex patterns to match version deprecation 1",
			inputErrMsg:     "version v1 not active",
			expectedMsg:     "version not active",
			expectedCode:    "deprecation",
		},
		{
			caseDescription: "should use complex patterns to match version deprecation 2",
			inputErrMsg:     "expired 202402 version is used",
			expectedMsg:     "expired version is used",
			expectedCode:    "deprecation",
		},
		{
			caseDescription: "should match standalone strong deprecation term 'end-of-support'",
			inputErrMsg:     "API version 1.0 is end-of-support",
			expectedMsg:     "API version is end of support",
			expectedCode:    "deprecation",
		},
		{
			caseDescription: "should mark 'API is deprecated' as deprecation",
			inputErrMsg:     "API is deprecated",
			expectedMsg:     "API is deprecated",
			expectedCode:    "deprecation",
		},
		{
			caseDescription: "should not mark 'invalid api key' as deprecation",
			inputErrMsg:     "Invalid API key",
			expectedMsg:     "Invalid API key",
			expectedCode:    "",
		},
		{
			caseDescription: "should match strong deprecation term 'no longer supported'",
			inputErrMsg:     "Legacy API is no longer supported",
			expectedMsg:     "Legacy API is no longer supported",
			expectedCode:    "deprecation",
		},
		{
			caseDescription: "should match 'deprecated version' combination",
			inputErrMsg:     "This version is deprecated",
			expectedMsg:     "This version is deprecated",
			expectedCode:    "deprecation",
		},
		{
			caseDescription: "should match 'expired version' combination",
			inputErrMsg:     "This version is expired",
			expectedMsg:     "This version is expired",
			expectedCode:    "deprecation",
		},
		{
			caseDescription: "should not match conversion for version deprecation",
			inputErrMsg:     "This conversion is expired",
			expectedMsg:     "This conversion is expired",
			expectedCode:    "",
		},
		{
			caseDescription: "should match 'deprecated endpoint' combination",
			inputErrMsg:     "This endpoint is deprecated",
			expectedMsg:     "This endpoint is deprecated",
			expectedCode:    "deprecation",
		},
	}

	edr := NewErrorDetailReporter(context.Background(), &configSubscriber{}, nil, config.Default)
	for _, tc := range testCases {
		t.Run(tc.caseDescription, func(t *testing.T) {
			t.Parallel()
			errorDetails := edr.extractErrorDetails(tc.inputErrMsg, tc.statTags, "TEST_DESTINATION")
			require.Equal(t, tc.expectedMsg, errorDetails.Message)
			require.Equal(t, tc.expectedCode, errorDetails.Code)
		})
	}
}
