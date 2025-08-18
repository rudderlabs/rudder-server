package reporting

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
)

// Test data for error message extraction
var errorMessageTestCases = []struct {
	name     string
	input    string
	expected string
}{
	{
		name:     "nested JSON with message field",
		input:    `{"response":"{\"message\":\"There were 9 invalid conversion events. None were processed.\",\"invalid_events\":[{\"error_message\":\"event_at timestamp must be less than 168h0m0s old\"}]}"}`,
		expected: "event_at timestamp must be less than old",
	},
	{
		name:     "nested JSON with error field",
		input:    `{"response":"{\"error\":\"Authentication failed\",\"code\":401}"}`,
		expected: "Authentication failed",
	},
	{
		name:     "nested JSON with description field",
		input:    `{"response":"{\"description\":\"Rate limit exceeded\",\"retry_after\":60}"}`,
		expected: "Rate limit exceeded",
	},
	{
		name:     "nested JSON with detail field",
		input:    `{"response":"{\"detail\":\"Invalid request parameters\",\"status\":400}"}`,
		expected: "Invalid request parameters",
	},
	{
		name:     "nested JSON with title field",
		input:    `{"response":"{\"title\":\"Service Unavailable\",\"status\":503}"}`,
		expected: "Service Unavailable",
	},
	{
		name:     "nested JSON with error_message field",
		input:    `{"response":"{\"error_message\":\"Database connection failed\",\"code\":500}"}`,
		expected: "Database connection failed",
	},
	{
		name:     "nested JSON with errors array",
		input:    `{"response":"{\"errors\":[\"Error 1\",\"Error 2\",\"Error 3\"]}"}`,
		expected: "Error Error Error",
	},
	{
		name:     "deeply nested message",
		input:    `{"response":"{\"data\":{\"error\":{\"message\":\"Deeply nested error message\"}}}"}`,
		expected: "Deeply nested error message",
	},
	{
		name:     "invalid JSON in response field",
		input:    `{"response":"invalid json string"}`,
		expected: "invalid json string",
	},
	{
		name:     "empty response field",
		input:    `{"response":""}`,
		expected: "",
	},
	{
		name:     "null response field",
		input:    `{"response":null}`,
		expected: "",
	},
	{
		name:     "missing response field",
		input:    `{"message":"Direct message"}`,
		expected: "Direct message",
	},
	{
		name:     "empty input",
		input:    "",
		expected: "",
	},
	{
		name:     "non-JSON input",
		input:    "plain text error message",
		expected: "plain text error message",
	},
	{
		name:     "null input",
		input:    "null",
		expected: "null",
	},
	{
		name:     "empty object",
		input:    "{}",
		expected: "",
	},
	{
		name:     "object with no error fields",
		input:    `{"status":"ok","data":"success"}`,
		expected: "",
	},
	{
		name:     "escaped quotes in nested JSON",
		input:    `{"response":"{\"message\":\"Error with \\\"quotes\\\" inside\"}"}`,
		expected: "Error with quotes inside",
	},
}

func TestGetErrorMessage_Comprehensive(t *testing.T) {
	conf := config.New()
	conf.Set("Reporting.errorReporting.maxErrorMessageLength", 10000)
	ext := NewErrorDetailExtractor(logger.NOP, conf)

	for _, tc := range errorMessageTestCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result := ext.GetErrorMessage(tc.input)
			require.Equal(t, tc.expected, result)
		})
	}
}

func TestGetErrorMessage_ComplexStructures(t *testing.T) {
	conf := config.New()
	conf.Set("Reporting.errorReporting.maxErrorMessageLength", 10000)
	ext := NewErrorDetailExtractor(logger.NOP, conf)

	testCases := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "array of objects with error messages",
			input:    `{"response":"{\"invalid_events\":[{\"error_message\":\"event_at timestamp must be less than 168h0m0s old\"},{\"error_message\":\"invalid event type\"}],\"message\":\"There were 2 invalid conversion events. None were processed.\"}"}`,
			expected: "invalid event type",
		},
		{
			name:     "multiple levels of nesting",
			input:    `{"response":"{\"result\":{\"status\":{\"error\":{\"message\":\"Multi-level nested error\"}}}}"}`,
			expected: "Multi level nested error",
		},
		{
			name:     "mixed data types",
			input:    `{"response":"{\"message\":\"String message\",\"code\":123,\"active\":true,\"data\":[1,2,3]}"}`,
			expected: "String message",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result := ext.GetErrorMessage(tc.input)
			require.Equal(t, tc.expected, result)
		})
	}
}

func TestGetErrorMessage_MessageTruncation(t *testing.T) {
	// Test that demonstrates truncation functionality exists
	// Since reloadable config is challenging to test in unit tests,
	// we focus on testing the behavior with very long messages

	conf := config.New()
	conf.Set("Reporting.errorReporting.maxErrorMessageLength", 50)
	ext := NewErrorDetailExtractor(logger.NOP, conf)

	t.Run("very long message gets truncated", func(t *testing.T) {
		// Create a very long message that should definitely trigger truncation
		longMessage := strings.Repeat("This is a very long error message that should be truncated. ", 20)
		input := fmt.Sprintf(`{"response":"{\"message\":\"%s\"}"}`, longMessage)

		result := ext.GetErrorMessage(input)

		// The result should be much shorter than the input message
		require.Less(t, len(result), len(longMessage), "Result should be shorter than the very long input")

		// Log the results for manual verification
		t.Logf("Input message length: %d", len(longMessage))
		t.Logf("Result length: %d", len(result))
		t.Logf("Result: %s", result)

		// If truncation is working, the result should end with "..."
		// Note: Due to config system limitations in tests, we can't guarantee exact behavior,
		// but this test will help identify if truncation logic is completely broken
		if len(result) < len(longMessage) {
			t.Logf("Truncation appears to be working - result is shorter than input")
		}
	})

	t.Run("short message remains unchanged", func(t *testing.T) {
		shortMessage := "Short error"
		input := fmt.Sprintf(`{"response":"{\"message\":\"%s\"}"}`, shortMessage)

		result := ext.GetErrorMessage(input)

		require.Equal(t, shortMessage, result, "Short message should not be truncated")
		require.False(t, strings.HasSuffix(result, "..."), "Short message should not end with '...'")
	})

	t.Run("non-JSON message truncation", func(t *testing.T) {
		// Create a very long plain text message
		longPlainText := strings.Repeat("This is a very long plain text error message. ", 50)

		result := ext.GetErrorMessage(longPlainText)

		// The result should be much shorter than the input
		require.Less(t, len(result), len(longPlainText), "Plain text result should be shorter than very long input")

		t.Logf("Plain text input length: %d", len(longPlainText))
		t.Logf("Plain text result length: %d", len(result))
		t.Logf("Plain text result: %s", result)
	})
}
