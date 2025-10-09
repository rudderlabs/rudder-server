package reporting

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestNewErrorDetailExtractor(t *testing.T) {
	conf := config.New()
	conf.Set("Reporting.ErrorDetail.ErrorMessageKeys", []string{"custom_error", "custom_message"})
	conf.Set("Reporting.errorReporting.maxErrorMessageLength", 20000)

	extractor := NewErrorDetailExtractor(logger.NOP, conf)
	require.NotNil(t, extractor)
	require.Equal(t, 20000, extractor.maxMessageLength.Load())
	require.Contains(t, extractor.ErrorMessageKeys, "error_message")
	require.Contains(t, extractor.ErrorMessageKeys, "message")
}

func TestExtractorHandle_GetErrorMessage(t *testing.T) {
	extractor := NewErrorDetailExtractor(logger.NOP, config.New())

	testCases := []struct {
		name           string
		sampleResponse string
		expected       string
	}{
		{
			name:           "simple string response",
			sampleResponse: "Database connection failed",
			expected:       "Database connection failed",
		},
		{
			name:           "JSON response with error key",
			sampleResponse: `{"error": "Authentication failed"}`,
			expected:       "Authentication failed",
		},
		{
			name:           "JSON response with message key",
			sampleResponse: `{"message": "Network timeout"}`,
			expected:       "Network timeout",
		},
		{
			name:           "JSON response with description key",
			sampleResponse: `{"description": "Invalid request"}`,
			expected:       "Invalid request",
		},
		{
			name:           "JSON response with detail key",
			sampleResponse: `{"detail": "Server error"}`,
			expected:       "Server error",
		},
		{
			name:           "JSON response with title key",
			sampleResponse: `{"title": "API Error"}`,
			expected:       "API Error",
		},
		{
			name:           "JSON response with response wrapper",
			sampleResponse: `{"response": {"error": "Nested error"}}`,
			expected:       "Nested error",
		},
		{
			name:           "JSON response with destinationResponse",
			sampleResponse: `{"destinationResponse": {"message": "Destination error"}}`,
			expected:       "Destination error",
		},
		{
			name:           "JSON response with errors array",
			sampleResponse: `{"errors": ["Error 1", "Error 2"]}`,
			expected:       "Error Error",
		},
		{
			name:           "HTML response",
			sampleResponse: `<body><h1>Error</h1><p>HTML error message</p></body>`,
			expected:       "body Error p HTML error message p body",
		},
		{
			name:           "unicode-escaped HTML in JSON response field",
			sampleResponse: `{"response":"\u003c!DOCTYPE html\u003e\n\u003chtml\u003e\n\u003ctitle\u003eError 411\u003c/title\u003e\n\u003cp\u003ePOST requests require a Content-length header\u003c/p\u003e\n\u003c/html\u003e"}`,
			expected:       "Error POST requests require a Content length header",
		},
		{
			name:           "warehouse error with internal_processing_failed",
			sampleResponse: `{"internal_processing_failed": {"errors": ["Warehouse error"]}}`,
			expected:       "Warehouse error",
		},
		{
			name:           "warehouse error with fetching_remote_schema_failed",
			sampleResponse: `{"fetching_remote_schema_failed": {"errors": ["Schema error"]}}`,
			expected:       "Schema error",
		},
		{
			name:           "warehouse error with exporting_data_failed",
			sampleResponse: `{"exporting_data_failed": {"errors": ["Export error"]}}`,
			expected:       "Export error",
		},
		{
			name:           "JSON response with msg key",
			sampleResponse: `{"msg": "Direct message"}`,
			expected:       "Direct message",
		},
		{
			name:           "invalid JSON",
			sampleResponse: `{"invalid": json}`,
			expected:       "invalid json",
		},
		{
			name:           "empty string",
			sampleResponse: "",
			expected:       "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result := extractor.GetErrorMessage(tc.sampleResponse)
			require.Equal(t, tc.expected, result)
		})
	}
}

func TestExtractorHandle_HandleKey(t *testing.T) {
	extractor := NewErrorDetailExtractor(logger.NOP, config.New())

	testCases := []struct {
		name     string
		key      string
		value    interface{}
		expected string
	}{
		{
			name:     "reason key with string",
			key:      "reason",
			value:    "Reason for failure",
			expected: "Reason for failure",
		},
		{
			name:     "reason key with non-string",
			key:      "reason",
			value:    123,
			expected: "",
		},
		{
			name:     "Error key with string",
			key:      "Error",
			value:    "Error message\nSecond line",
			expected: "Error message",
		},
		{
			name:     "Error key with JSON string",
			key:      "Error",
			value:    `{"error": "JSON error"}`,
			expected: "",
		},
		{
			name:     "response key with string",
			key:      "response",
			value:    "Response message",
			expected: "Response message",
		},
		{
			name:     "error key with string",
			key:      "error",
			value:    "Error message",
			expected: "Error message",
		},
		{
			name:     "internal_processing_failed with map",
			key:      "internal_processing_failed",
			value:    map[string]interface{}{"errors": []interface{}{"Processing error"}},
			expected: "Processing error",
		},
		{
			name:     "internal_processing_failed with non-map",
			key:      "internal_processing_failed",
			value:    "not a map",
			expected: "",
		},
		{
			name:     "unknown key",
			key:      "unknown",
			value:    "value",
			expected: "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result := extractor.handleKey(tc.key, tc.value)
			require.Equal(t, tc.expected, result)
		})
	}
}

func TestIsHTMLString(t *testing.T) {
	testCases := []struct {
		name     string
		input    string
		expected bool
	}{
		{
			name:     "HTML with DOCTYPE and html tags",
			input:    `<!DOCTYPE html><html><title>Error</title></html>`,
			expected: true,
		},
		{
			name:     "HTML with body tags",
			input:    `<body><p>Error</p></body>`,
			expected: true,
		},
		{
			name:     "HTML with html and title tags",
			input:    `<html><title>Error 411</title><p>Content</p></html>`,
			expected: true,
		},
		{
			name:     "HTML with html and head tags",
			input:    `<html><head><meta charset="utf-8"></head></html>`,
			expected: true,
		},
		{
			name:     "Case insensitive DOCTYPE",
			input:    `<!doctype HTML><HTML><TITLE>Error</TITLE></HTML>`,
			expected: true,
		},
		{
			name:     "Plain text",
			input:    "This is a plain error message",
			expected: false,
		},
		{
			name:     "JSON string",
			input:    `{"error": "message"}`,
			expected: false,
		},
		{
			name:     "Incomplete HTML - only html tag",
			input:    `<html>Some content`,
			expected: false,
		},
		{
			name:     "Incomplete HTML - only doctype",
			input:    `<!DOCTYPE html>`,
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result := isHTMLString(tc.input)
			require.Equal(t, tc.expected, result)
		})
	}
}

func TestExtractorHandle_HandleResponseOrErrorKey(t *testing.T) {
	extractor := NewErrorDetailExtractor(logger.NOP, config.New())

	testCases := []struct {
		name     string
		valueStr string
		expected string
	}{
		{
			name:     "JSON string",
			valueStr: `{"message": "JSON message"}`,
			expected: "JSON message",
		},
		{
			name:     "HTML string",
			valueStr: `<body><h1>Error</h1><p>HTML content</p></body>`,
			expected: "Error\r\n\r\nHTML content\r\n\r\n",
		},
		{
			name:     "HTML with DOCTYPE and no explicit body tag",
			valueStr: `<!DOCTYPE html><html><title>Error 411</title><p>POST requests require a Content-length header.</p></html>`,
			expected: "Error 411\r\n\r\nPOST requests require a Content-length header.\r\n\r\n",
		},
		{
			name:     "HTML with html and title tags",
			valueStr: `<html><title>Error 404</title><p>Page not found</p></html>`,
			expected: "Error 404\r\n\r\nPage not found\r\n\r\n",
		},
		{
			name:     "plain string",
			valueStr: "Plain error message",
			expected: "Plain error message",
		},
		{
			name:     "invalid JSON",
			valueStr: `{"invalid": json}`,
			expected: `{"invalid": json}`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result := extractor.handleResponseOrErrorKey(tc.valueStr)
			require.Equal(t, tc.expected, result)
		})
	}
}

func TestExtractorHandle_HandleWarehouseError(t *testing.T) {
	extractor := NewErrorDetailExtractor(logger.NOP, config.New())

	testCases := []struct {
		name     string
		value    interface{}
		key      string
		expected string
	}{
		{
			name:     "valid warehouse error map",
			value:    map[string]interface{}{"errors": []interface{}{"Warehouse error 1", "Warehouse error 2"}},
			key:      "internal_processing_failed",
			expected: "Warehouse error 1.Warehouse error 2",
		},
		{
			name:     "warehouse error with non-array errors",
			value:    map[string]interface{}{"errors": "not an array"},
			key:      "internal_processing_failed",
			expected: "",
		},
		{
			name:     "warehouse error without errors key",
			value:    map[string]interface{}{"other": "value"},
			key:      "internal_processing_failed",
			expected: "",
		},
		{
			name:     "non-map value",
			value:    "not a map",
			key:      "internal_processing_failed",
			expected: "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result := extractor.handleWarehouseError(tc.value, tc.key)
			require.Equal(t, tc.expected, result)
		})
	}
}

func TestExtractorHandle_TruncateMessage(t *testing.T) {
	conf := config.New()
	conf.Set("Reporting.errorReporting.maxErrorMessageLength", 10)
	extractor := NewErrorDetailExtractor(logger.NOP, conf)

	testCases := []struct {
		name     string
		message  string
		expected string
	}{
		{
			name:     "message within limit",
			message:  "Short",
			expected: "Short",
		},
		{
			name:     "message at limit",
			message:  "Exactly 10",
			expected: "Exactly 10",
		},
		{
			name:     "message exceeds limit",
			message:  "This message is too long",
			expected: "This messa...",
		},
		{
			name:     "empty message",
			message:  "",
			expected: "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result := extractor.truncateMessage(tc.message)
			require.Equal(t, tc.expected, result)
		})
	}
}

func TestExtractorHandle_CleanUpErrorMessage(t *testing.T) {
	extractor := NewErrorDetailExtractor(logger.NOP, config.New())

	testCases := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "URL cleanup",
			input:    "Error at https://example.com/api/v1/endpoint",
			expected: "Error at",
		},
		{
			name:     "IP address cleanup",
			input:    "Connection failed to 192.168.1.1",
			expected: "Connection failed to",
		},
		{
			name:     "email cleanup",
			input:    "User test@example.com not found",
			expected: "User not found",
		},
		{
			name:     "ID cleanup",
			input:    "Object 12345-a_bc-def not found",
			expected: "Object not found",
		},
		{
			name:     "multiple special characters",
			input:    "Error: https://api.com/user@test.com/12345 failed",
			expected: "Error failed",
		},
		{
			name:     "whitespace normalization",
			input:    "Multiple    spaces   and\ttabs",
			expected: "Multiple spaces and tabs",
		},
		{
			name:     "empty string",
			input:    "",
			expected: "",
		},
		{
			name:     "only special characters",
			input:    "https://example.com 192.168.1.1 test@example.com 12345",
			expected: "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result := extractor.CleanUpErrorMessage(tc.input)
			require.Equal(t, tc.expected, result)
		})
	}
}

func TestExtractorHandle_GetErrorCode(t *testing.T) {
	extractor := NewErrorDetailExtractor(logger.NOP, config.New())

	testCases := []struct {
		name         string
		errorMessage string
		statTags     map[string]string
		destType     string
		expected     string
	}{
		{
			name:         "error code from stat tags",
			errorMessage: "Some error",
			statTags:     map[string]string{"errorCategory": "auth", "errorType": "invalid_token"},
			destType:     "s3",
			expected:     "auth:invalid_token",
		},
		{
			name:         "deprecation error for non-warehouse destination",
			errorMessage: "API version deprecated",
			statTags:     map[string]string{},
			destType:     "s3",
			expected:     "deprecation",
		},
		{
			name:         "deprecation error with version keyword",
			errorMessage: "API version action required",
			statTags:     map[string]string{},
			destType:     "s3",
			expected:     "deprecation",
		},
		{
			name:         "deprecation error with endpoint keyword",
			errorMessage: "endpoint deprecated",
			statTags:     map[string]string{},
			destType:     "s3",
			expected:     "deprecation",
		},
		{
			name:         "deprecation error with api keyword",
			errorMessage: "api deprecated",
			statTags:     map[string]string{},
			destType:     "s3",
			expected:     "deprecation",
		},
		{
			name:         "no deprecation error for warehouse destination",
			errorMessage: "API version deprecated",
			statTags:     map[string]string{},
			destType:     warehouseutils.SNOWFLAKE,
			expected:     "",
		},
		{
			name:         "no error code",
			errorMessage: "Regular error message",
			statTags:     map[string]string{},
			destType:     "s3",
			expected:     "",
		},
		{
			name:         "only errorCategory in stat tags",
			errorMessage: "Some error",
			statTags:     map[string]string{"errorCategory": "auth"},
			destType:     "s3",
			expected:     "auth",
		},
		{
			name:         "only errorType in stat tags",
			errorMessage: "Some error",
			statTags:     map[string]string{"errorType": "invalid_token"},
			destType:     "s3",
			expected:     "invalid_token",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result := extractor.GetErrorCode(tc.errorMessage, tc.statTags, tc.destType)
			require.Equal(t, tc.expected, result)
		})
	}
}

func TestExtractorHandle_IsVersionDeprecationError(t *testing.T) {
	extractor := NewErrorDetailExtractor(logger.NOP, config.New())

	testCases := []struct {
		name         string
		errorMessage string
		expected     bool
	}{
		{
			name:         "version deprecation error",
			errorMessage: "API version deprecated",
			expected:     true,
		},
		{
			name:         "version action required",
			errorMessage: "API version action required",
			expected:     true,
		},
		{
			name:         "version end of life",
			errorMessage: "API version end of life",
			expected:     true,
		},
		{
			name:         "endpoint deprecation",
			errorMessage: "endpoint deprecated",
			expected:     true,
		},
		{
			name:         "api deprecation",
			errorMessage: "api deprecated",
			expected:     true,
		},
		{
			name:         "not a deprecation error",
			errorMessage: "Regular error message",
			expected:     false,
		},
		{
			name:         "empty message",
			errorMessage: "",
			expected:     false,
		},
		{
			name:         "case insensitive",
			errorMessage: "API VERSION DEPRECATED",
			expected:     true,
		},
		{
			name:         "with hyphens",
			errorMessage: "API-version-deprecated",
			expected:     true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result := extractor.isVersionDeprecationError(tc.errorMessage)
			require.Equal(t, tc.expected, result)
		})
	}
}

func TestContainsDeprecationKey(t *testing.T) {
	testCases := []struct {
		name         string
		errorMessage string
		key          string
		expected     bool
	}{
		{
			name:         "key at start",
			errorMessage: "version deprecated",
			key:          "version",
			expected:     true,
		},
		{
			name:         "key in middle",
			errorMessage: "API version deprecated",
			key:          "version",
			expected:     true,
		},
		{
			name:         "key not present",
			errorMessage: "API deprecated",
			key:          "version",
			expected:     false,
		},
		{
			name:         "empty message",
			errorMessage: "",
			key:          "version",
			expected:     false,
		},
		{
			name:         "empty key",
			errorMessage: "some message",
			key:          "",
			expected:     true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result := containsDeprecationKey(tc.errorMessage, tc.key)
			require.Equal(t, tc.expected, result)
		})
	}
}

func TestContainsAllKeywords(t *testing.T) {
	testCases := []struct {
		name         string
		errorMessage string
		keywordSets  [][]string
		expected     bool
	}{
		{
			name:         "all keywords present",
			errorMessage: "API version deprecated",
			keywordSets:  [][]string{{"api", "version"}, {"deprecated"}},
			expected:     true,
		},
		{
			name:         "some keywords missing",
			errorMessage: "API deprecated",
			keywordSets:  [][]string{{"api", "version"}, {"deprecated"}},
			expected:     true,
		},
		{
			name:         "no keywords present",
			errorMessage: "Regular error",
			keywordSets:  [][]string{{"api", "version"}, {"deprecated"}},
			expected:     false,
		},
		{
			name:         "empty keyword sets",
			errorMessage: "Some message",
			keywordSets:  [][]string{},
			expected:     false,
		},
		{
			name:         "empty message",
			errorMessage: "",
			keywordSets:  [][]string{{"api", "version"}},
			expected:     false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result := containsAllKeywords(tc.errorMessage, tc.keywordSets)
			require.Equal(t, tc.expected, result)
		})
	}
}

func TestGetErrorCodeFromStatTags(t *testing.T) {
	testCases := []struct {
		name     string
		statTags map[string]string
		expected string
	}{
		{
			name:     "both errorCategory and errorType",
			statTags: map[string]string{"errorCategory": "auth", "errorType": "invalid_token"},
			expected: "auth:invalid_token",
		},
		{
			name:     "only errorCategory",
			statTags: map[string]string{"errorCategory": "auth"},
			expected: "auth",
		},
		{
			name:     "only errorType",
			statTags: map[string]string{"errorType": "invalid_token"},
			expected: "invalid_token",
		},
		{
			name:     "neither present",
			statTags: map[string]string{"other": "value"},
			expected: "",
		},
		{
			name:     "empty stat tags",
			statTags: map[string]string{},
			expected: "",
		},
		{
			name:     "nil stat tags",
			statTags: nil,
			expected: "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result := getErrorCodeFromStatTags(tc.statTags)
			require.Equal(t, tc.expected, result)
		})
	}
}

func TestFindKeys(t *testing.T) {
	testCases := []struct {
		name     string
		keys     []string
		jsonObj  interface{}
		expected map[string]interface{}
	}{
		{
			name:     "simple map with matching keys",
			keys:     []string{"message", "error"},
			jsonObj:  map[string]interface{}{"message": "test", "error": "err", "other": "value"},
			expected: map[string]interface{}{"message": "test", "error": "err"},
		},
		{
			name: "nested map with matching keys",
			keys: []string{"message"},
			jsonObj: map[string]interface{}{
				"response": map[string]interface{}{"message": "nested"},
				"other":    "value",
			},
			expected: map[string]interface{}{"message": "nested"},
		},
		{
			name: "array with matching keys",
			keys: []string{"message"},
			jsonObj: []interface{}{
				map[string]interface{}{"message": "first"},
				map[string]interface{}{"message": "second"},
			},
			expected: map[string]interface{}{"message": "second"}, // Last one wins
		},
		{
			name:     "no matching keys",
			keys:     []string{"message"},
			jsonObj:  map[string]interface{}{"other": "value"},
			expected: map[string]interface{}{},
		},
		{
			name:     "empty keys",
			keys:     []string{},
			jsonObj:  map[string]interface{}{"message": "test"},
			expected: map[string]interface{}{},
		},
		{
			name:     "nil jsonObj",
			keys:     []string{"message"},
			jsonObj:  nil,
			expected: map[string]interface{}{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result := findKeys(tc.keys, tc.jsonObj)
			require.Equal(t, tc.expected, result)
		})
	}
}

func TestFindFirstExistingKey(t *testing.T) {
	testCases := []struct {
		name     string
		keys     []string
		jsonObj  map[string]interface{}
		expected interface{}
	}{
		{
			name:     "first key exists",
			keys:     []string{"message", "error"},
			jsonObj:  map[string]interface{}{"message": "test", "error": "err"},
			expected: "test",
		},
		{
			name:     "first key missing, second exists",
			keys:     []string{"message", "error"},
			jsonObj:  map[string]interface{}{"error": "err"},
			expected: "err",
		},
		{
			name:     "no keys exist",
			keys:     []string{"message", "error"},
			jsonObj:  map[string]interface{}{"other": "value"},
			expected: nil,
		},
		{
			name:     "empty keys",
			keys:     []string{},
			jsonObj:  map[string]interface{}{"message": "test"},
			expected: nil,
		},
		{
			name:     "nil jsonObj",
			keys:     []string{"message"},
			jsonObj:  nil,
			expected: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result := getFirstNonNilValue(tc.keys, tc.jsonObj)
			require.Equal(t, tc.expected, result)
		})
	}
}

func TestConvertInterfaceArrToStrArrWithDelimitter(t *testing.T) {
	testCases := []struct {
		name       string
		arr        []interface{}
		delimitter string
		expected   string
	}{
		{
			name:       "string array",
			arr:        []interface{}{"error1", "error2", "error3"},
			delimitter: ".",
			expected:   "error1.error2.error3",
		},
		{
			name:       "mixed types",
			arr:        []interface{}{"error1", 123, true},
			delimitter: "|",
			expected:   "error1|123|true",
		},
		{
			name:       "empty array",
			arr:        []interface{}{},
			delimitter: ".",
			expected:   "",
		},
		{
			name:       "single element",
			arr:        []interface{}{"error"},
			delimitter: ".",
			expected:   "error",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result := convertInterfaceArrToStrArrWithDelimitter(tc.arr, tc.delimitter)
			require.Equal(t, tc.expected, result)
		})
	}
}

func TestGetErrorMessageFromResponse(t *testing.T) {
	testCases := []struct {
		name        string
		resp        interface{}
		messageKeys []string
		expected    string
	}{
		{
			name:        "direct msg key",
			resp:        map[string]interface{}{"msg": "Direct message"},
			messageKeys: []string{"message", "error"},
			expected:    "Direct message",
		},
		{
			name: "destinationResponse with message",
			resp: map[string]interface{}{
				"destinationResponse": map[string]interface{}{"message": "Dest message"},
			},
			messageKeys: []string{"message", "error"},
			expected:    "Dest message",
		},
		{
			name:        "direct message key",
			resp:        map[string]interface{}{"message": "Test message"},
			messageKeys: []string{"message", "error"},
			expected:    "Test message",
		},
		{
			name:        "errors array",
			resp:        map[string]interface{}{"errors": []interface{}{"Error 1", "Error 2"}},
			messageKeys: []string{"message", "error"},
			expected:    "Error 1.Error 2",
		},
		{
			name: "nested errors",
			resp: map[string]interface{}{
				"response": map[string]interface{}{"errors": []interface{}{"Nested error"}},
			},
			messageKeys: []string{"message", "error"},
			expected:    "Nested error",
		},
		{
			name:        "no message found",
			resp:        map[string]interface{}{"other": "value"},
			messageKeys: []string{"message", "error"},
			expected:    "",
		},
		{
			name:        "non-map response",
			resp:        "string response",
			messageKeys: []string{"message", "error"},
			expected:    "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result := getErrorMessageFromResponse(tc.resp, tc.messageKeys)
			require.Equal(t, tc.expected, result)
		})
	}
}

func TestGetErrorFromWarehouse(t *testing.T) {
	testCases := []struct {
		name     string
		resp     map[string]interface{}
		expected string
	}{
		{
			name:     "valid errors array",
			resp:     map[string]interface{}{"errors": []interface{}{"Error 1", "Error 2"}},
			expected: "Error 1.Error 2",
		},
		{
			name:     "duplicate errors",
			resp:     map[string]interface{}{"errors": []interface{}{"Error 1", "Error 1", "Error 2"}},
			expected: "Error 1.Error 2",
		},
		{
			name:     "no errors key",
			resp:     map[string]interface{}{"other": "value"},
			expected: "",
		},
		{
			name:     "errors not an array",
			resp:     map[string]interface{}{"errors": "not an array"},
			expected: "",
		},
		{
			name:     "empty errors array",
			resp:     map[string]interface{}{"errors": []interface{}{}},
			expected: "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result := getErrorFromWarehouse(tc.resp)
			require.Equal(t, tc.expected, result)
		})
	}
}

func TestIsJSON(t *testing.T) {
	testCases := []struct {
		name     string
		input    string
		expected bool
	}{
		{
			name:     "valid JSON object",
			input:    `{"key": "value"}`,
			expected: true,
		},
		{
			name:     "valid JSON array",
			input:    `["item1", "item2"]`,
			expected: true,
		},
		{
			name:     "JSON with whitespace",
			input:    `  {"key": "value"}  `,
			expected: true,
		},
		{
			name:     "invalid JSON",
			input:    `{"key": "value"`,
			expected: false,
		},
		{
			name:     "string with braces but not JSON",
			input:    `{not json}`,
			expected: true,
		},
		{
			name:     "empty string",
			input:    "",
			expected: false,
		},
		{
			name:     "plain text",
			input:    "This is not JSON",
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result := IsJSON(tc.input)
			require.Equal(t, tc.expected, result)
		})
	}
}

func TestCheckForGoMapOrList(t *testing.T) {
	testCases := []struct {
		name     string
		value    interface{}
		expected bool
	}{
		{
			name:     "map[string]interface{}",
			value:    map[string]interface{}{"key": "value"},
			expected: true,
		},
		{
			name:     "[]interface{}",
			value:    []interface{}{"item1", "item2"},
			expected: true,
		},
		{
			name:     "string",
			value:    "string value",
			expected: false,
		},
		{
			name:     "int",
			value:    123,
			expected: false,
		},
		{
			name:     "bool",
			value:    true,
			expected: false,
		},
		{
			name:     "nil",
			value:    nil,
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result := checkForGoMapOrList(tc.value)
			require.Equal(t, tc.expected, result)
		})
	}
}
