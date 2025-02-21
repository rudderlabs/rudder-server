package transformer

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"

	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestSafeNamespace(t *testing.T) {
	testCases := []struct {
		destType, namespace, expected string
	}{
		{destType: whutils.RS, namespace: "omega", expected: "omega"},
		{destType: whutils.RS, namespace: "omega v2 ", expected: "omega_v_2"},
		{destType: whutils.RS, namespace: "9mega", expected: "_9_mega"},
		{destType: whutils.RS, namespace: "mega&", expected: "mega"},
		{destType: whutils.RS, namespace: "ome$ga", expected: "ome_ga"},
		{destType: whutils.RS, namespace: "omega$", expected: "omega"},
		{destType: whutils.RS, namespace: "ome_ ga", expected: "ome_ga"},
		{destType: whutils.RS, namespace: "9mega________-________90", expected: "_9_mega_90"},
		{destType: whutils.RS, namespace: "Cízǔ", expected: "c_z"},
		{destType: whutils.RS, namespace: "Rudderstack", expected: "rudderstack"},
		{destType: whutils.RS, namespace: "___", expected: "stringempty"},
		{destType: whutils.RS, namespace: "group", expected: "_group"},
		{destType: whutils.RS, namespace: "k3_namespace", expected: "k_3_namespace"},
		{destType: whutils.BQ, namespace: "k3_namespace", expected: "k3_namespace"},
	}
	for _, tc := range testCases {
		c := config.New()
		c.Set("Warehouse.bigquery.skipNamespaceSnakeCasing", true)

		require.Equal(t, tc.expected, safeNamespace(c, tc.destType, tc.namespace))
	}
}

func TestSafeTableName(t *testing.T) {
	testCases := []struct {
		name, destType, tableName, expected string
		options                             intrOptions
		expectError                         bool
	}{
		{
			name:        "Empty table name",
			destType:    whutils.SNOWFLAKE,
			tableName:   "",
			expected:    "",
			expectError: true, // Should return response
		},
		{
			name:        "Snowflake uppercase conversion",
			destType:    whutils.SNOWFLAKE,
			tableName:   "myTable",
			expected:    "MYTABLE",
			expectError: false,
		},
		{
			name:        "Postgres truncation and lowercase",
			destType:    whutils.POSTGRES,
			tableName:   "ThisIsAReallyLongTableNameThatExceedsThe63CharacterLimitForPostgresTables",
			expected:    "thisisareallylongtablenamethatexceedsthe63characterlimitforpost",
			expectError: false,
		},
		{
			name:        "Lowercase conversion for other destTypes",
			destType:    whutils.BQ,
			tableName:   "MyTableName",
			expected:    "mytablename",
			expectError: false,
		},
		{
			name:        "Reserved keyword escaping",
			destType:    whutils.SNOWFLAKE,
			tableName:   "SELECT",
			expected:    "_SELECT", // Should escape reserved keyword
			expectError: false,
		},
		{
			name:        "No reserved keyword escaping with skip option",
			destType:    whutils.SNOWFLAKE,
			tableName:   "SELECT",
			options:     intrOptions{skipReservedKeywordsEscaping: true},
			expected:    "SELECT", // Should not escape reserved keyword
			expectError: false,
		},
		{
			name:        "Data lake, no trimming",
			destType:    whutils.S3Datalake,
			tableName:   "ThisIsAReallyLongTableNameThatExceedsThe63CharacterLimitForDatalakeTables",
			expected:    "thisisareallylongtablenamethatexceedsthe63characterlimitfordatalaketables",
			expectError: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := safeTableName(tc.destType, &tc.options, tc.tableName)

			if tc.expectError {
				require.Error(t, err)
				require.Empty(t, result)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expected, result)
			}
		})
	}
}

func TestSafeColumnName(t *testing.T) {
	testCases := []struct {
		name, destType, columnName, expected string
		options                              intrOptions
		expectError                          bool
	}{
		{
			name:        "Empty column name",
			destType:    whutils.SNOWFLAKE,
			columnName:  "",
			expected:    "",
			expectError: true, // Should return response
		},
		{
			name:        "Snowflake uppercase conversion",
			destType:    whutils.SNOWFLAKE,
			columnName:  "myColumn",
			expected:    "MYCOLUMN",
			expectError: false,
		},
		{
			name:        "Postgres truncation and lowercase",
			destType:    whutils.POSTGRES,
			columnName:  "ThisIsAReallyLongColumnNameThatExceedsThe63CharacterLimitForPostgresTables",
			expected:    "thisisareallylongcolumnnamethatexceedsthe63characterlimitforpos",
			expectError: false,
		},
		{
			name:        "Lowercase conversion for other destTypes",
			destType:    whutils.BQ,
			columnName:  "MyColumnName",
			expected:    "mycolumnname",
			expectError: false,
		},
		{
			name:        "Reserved keyword escaping",
			destType:    whutils.SNOWFLAKE,
			columnName:  "SELECT",
			expected:    "_SELECT", // Should escape reserved keyword
			expectError: false,
		},
		{
			name:        "No reserved keyword escaping with skip option",
			destType:    whutils.SNOWFLAKE,
			columnName:  "SELECT",
			options:     intrOptions{skipReservedKeywordsEscaping: true},
			expected:    "SELECT", // Should not escape reserved keyword
			expectError: false,
		},
		{
			name:        "Data lake, no trimming",
			destType:    whutils.S3Datalake,
			columnName:  "ThisIsAReallyLongColumnNameThatExceedsThe63CharacterLimitForDatalakeColumns",
			expected:    "thisisareallylongcolumnnamethatexceedsthe63characterlimitfordatalakecolumns",
			expectError: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := safeColumnName(tc.destType, &tc.options, tc.columnName)

			if tc.expectError {
				require.Error(t, err)
				require.Empty(t, result)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expected, result)
			}
		})
	}
}

func TestTransformTableName(t *testing.T) {
	testCases := []struct {
		name      string
		intrOpts  intrOptions
		destOpts  destOptions
		tableName string
		expected  string
	}{
		{
			name:      "Blendo casing - table name trimmed and lowercased",
			intrOpts:  intrOptions{useBlendoCasing: true},
			destOpts:  destOptions{},
			tableName: " TableName ",
			expected:  "tablename",
		},
		{
			name:      "Blendo casing - mixedcased to lowercased",
			intrOpts:  intrOptions{useBlendoCasing: true},
			destOpts:  destOptions{},
			tableName: "CaMeLcAsE",
			expected:  "camelcase",
		},
		{
			name:      "Blendo casing - mixedcased to lowercased",
			intrOpts:  intrOptions{useBlendoCasing: true},
			destOpts:  destOptions{},
			tableName: "Table@Name!",
			expected:  "table@name!",
		},
		{
			name:      "Blendo casing - alphanumeric",
			intrOpts:  intrOptions{useBlendoCasing: true},
			destOpts:  destOptions{},
			tableName: "TableName123",
			expected:  "tablename123",
		},

		{
			name:      "Standard casing - underscoreDivideNumbers(true) - remove symbols and join continuous letters and numbers with a single underscore",
			intrOpts:  intrOptions{useBlendoCasing: false},
			destOpts:  destOptions{underscoreDivideNumbers: true},
			tableName: "&4yasdfa(84224_fs9##_____*3q",
			expected:  "_4_yasdfa_84224_fs_9_3_q",
		},
		{
			name:      "Standard casing - underscoreDivideNumbers(true) - omega to omega",
			intrOpts:  intrOptions{useBlendoCasing: false},
			destOpts:  destOptions{underscoreDivideNumbers: true},
			tableName: "omega",
			expected:  "omega",
		},
		{
			name:      "Standard casing - underscoreDivideNumbers(true) - omega v2 to omega_v_2",
			intrOpts:  intrOptions{useBlendoCasing: false},
			destOpts:  destOptions{underscoreDivideNumbers: true},
			tableName: "omega v2",
			expected:  "omega_v_2",
		},
		{
			name:      "Standard casing - underscoreDivideNumbers(true) - prepend underscore if name starts with a number",
			intrOpts:  intrOptions{useBlendoCasing: false},
			destOpts:  destOptions{underscoreDivideNumbers: true},
			tableName: "9mega",
			expected:  "_9_mega",
		},
		{
			name:      "Standard casing - underscoreDivideNumbers(true) - remove trailing special characters",
			intrOpts:  intrOptions{useBlendoCasing: false},
			destOpts:  destOptions{underscoreDivideNumbers: true},
			tableName: "mega&",
			expected:  "mega",
		},
		{
			name:      "Standard casing - underscoreDivideNumbers(true) - replace special character in the middle with underscore",
			intrOpts:  intrOptions{useBlendoCasing: false},
			destOpts:  destOptions{underscoreDivideNumbers: true},
			tableName: "ome$ga",
			expected:  "ome_ga",
		},
		{
			name:      "Standard casing - underscoreDivideNumbers(true) - remove trailing $ character",
			intrOpts:  intrOptions{useBlendoCasing: false},
			destOpts:  destOptions{underscoreDivideNumbers: true},
			tableName: "omega$",
			expected:  "omega",
		},
		{
			name:      "Standard casing - underscoreDivideNumbers(true) - spaces and special characters by converting to underscores",
			intrOpts:  intrOptions{useBlendoCasing: false},
			destOpts:  destOptions{underscoreDivideNumbers: true},
			tableName: "ome_ ga",
			expected:  "ome_ga",
		},
		{
			name:      "Standard casing - underscoreDivideNumbers(true) - multiple underscores and hyphens by reducing to single underscores",
			intrOpts:  intrOptions{useBlendoCasing: false},
			destOpts:  destOptions{underscoreDivideNumbers: true},
			tableName: "9mega________-________90",
			expected:  "_9_mega_90",
		},
		{
			name:      "Standard casing - underscoreDivideNumbers(true) - non-ASCII characters by converting them to underscores",
			intrOpts:  intrOptions{useBlendoCasing: false},
			destOpts:  destOptions{underscoreDivideNumbers: true},
			tableName: "Cízǔ",
			expected:  "c_z",
		},
		{
			name:      "Standard casing - underscoreDivideNumbers(true) - CamelCase123Key to camel_case_123_key",
			intrOpts:  intrOptions{useBlendoCasing: false},
			destOpts:  destOptions{underscoreDivideNumbers: true},
			tableName: "CamelCase123Key",
			expected:  "camel_case_123_key",
		},
		{
			name:      "Standard casing - underscoreDivideNumbers(true) - numbers and commas",
			intrOpts:  intrOptions{useBlendoCasing: false},
			destOpts:  destOptions{underscoreDivideNumbers: true},
			tableName: "path to $1,00,000",
			expected:  "path_to_1_00_000",
		},
		{
			name:      "Standard casing - underscoreDivideNumbers(true) - no valid characters",
			intrOpts:  intrOptions{useBlendoCasing: false},
			destOpts:  destOptions{underscoreDivideNumbers: true},
			tableName: "@#$%",
			expected:  "",
		},
		{
			name:      "Standard casing - underscoreDivideNumbers(true) - underscores between letters and numbers",
			intrOpts:  intrOptions{useBlendoCasing: false},
			destOpts:  destOptions{underscoreDivideNumbers: true},
			tableName: "test123",
			expected:  "test_123",
		},
		{
			name:      "Standard casing - underscoreDivideNumbers(true) - multiple underscore-number sequences",
			intrOpts:  intrOptions{useBlendoCasing: false},
			destOpts:  destOptions{underscoreDivideNumbers: true},
			tableName: "abc123def456",
			expected:  "abc_123_def_456",
		},
		{
			name:      "Standard casing - underscoreDivideNumbers(true) - multiple underscore-number sequences",
			intrOpts:  intrOptions{useBlendoCasing: false},
			destOpts:  destOptions{underscoreDivideNumbers: true},
			tableName: "abc_123_def_456",
			expected:  "abc_123_def_456",
		},
		{
			name:      "Standard casing - underscoreDivideNumbers(true) - single underscore",
			intrOpts:  intrOptions{useBlendoCasing: false},
			destOpts:  destOptions{underscoreDivideNumbers: true},
			tableName: "__abc_123_def_456",
			expected:  "__abc_123_def_456",
		},
		{
			name:      "Standard casing - underscoreDivideNumbers(true) - multiple underscore",
			intrOpts:  intrOptions{useBlendoCasing: false},
			destOpts:  destOptions{underscoreDivideNumbers: true},
			tableName: "_abc_123_def_456",
			expected:  "_abc_123_def_456",
		},

		{
			name:      "Standard casing - underscoreDivideNumbers(false) - remove symbols and join continuous letters and numbers with a single underscore",
			intrOpts:  intrOptions{useBlendoCasing: false},
			destOpts:  destOptions{underscoreDivideNumbers: false},
			tableName: "&4yasdfa(84224_fs9##_____*3q",
			expected:  "_4yasdfa_84224_fs9_3q",
		},
		{
			name:      "Standard casing - underscoreDivideNumbers(false) - omega to omega",
			intrOpts:  intrOptions{useBlendoCasing: false},
			destOpts:  destOptions{underscoreDivideNumbers: false},
			tableName: "omega",
			expected:  "omega",
		},
		{
			name:      "Standard casing - underscoreDivideNumbers(false) - omega v2 to omega_v_2",
			intrOpts:  intrOptions{useBlendoCasing: false},
			destOpts:  destOptions{underscoreDivideNumbers: false},
			tableName: "omega v2",
			expected:  "omega_v2",
		},
		{
			name:      "Standard casing - underscoreDivideNumbers(false) - prepend underscore if name starts with a number",
			intrOpts:  intrOptions{useBlendoCasing: false},
			destOpts:  destOptions{underscoreDivideNumbers: false},
			tableName: "9mega",
			expected:  "_9mega",
		},
		{
			name:      "Standard casing - underscoreDivideNumbers(false) - remove trailing special characters",
			intrOpts:  intrOptions{useBlendoCasing: false},
			destOpts:  destOptions{underscoreDivideNumbers: false},
			tableName: "mega&",
			expected:  "mega",
		},
		{
			name:      "Standard casing - underscoreDivideNumbers(false) - replace special character in the middle with underscore",
			intrOpts:  intrOptions{useBlendoCasing: false},
			destOpts:  destOptions{underscoreDivideNumbers: false},
			tableName: "ome$ga",
			expected:  "ome_ga",
		},
		{
			name:      "Standard casing - underscoreDivideNumbers(false) - remove trailing $ character",
			intrOpts:  intrOptions{useBlendoCasing: false},
			destOpts:  destOptions{underscoreDivideNumbers: false},
			tableName: "omega$",
			expected:  "omega",
		},
		{
			name:      "Standard casing - underscoreDivideNumbers(false) - spaces and special characters by converting to underscores",
			intrOpts:  intrOptions{useBlendoCasing: false},
			destOpts:  destOptions{underscoreDivideNumbers: false},
			tableName: "ome_ ga",
			expected:  "ome_ga",
		},
		{
			name:      "Standard casing - underscoreDivideNumbers(false) - multiple underscores and hyphens by reducing to single underscores",
			intrOpts:  intrOptions{useBlendoCasing: false},
			destOpts:  destOptions{underscoreDivideNumbers: false},
			tableName: "9mega________-________90",
			expected:  "_9mega_90",
		},
		{
			name:      "Standard casing - underscoreDivideNumbers(false) - non-ASCII characters by converting them to underscores",
			intrOpts:  intrOptions{useBlendoCasing: false},
			destOpts:  destOptions{underscoreDivideNumbers: false},
			tableName: "Cízǔ",
			expected:  "c_z",
		},
		{
			name:      "Standard casing - underscoreDivideNumbers(false) - CamelCase123Key to camel_case_123_key",
			intrOpts:  intrOptions{useBlendoCasing: false},
			destOpts:  destOptions{underscoreDivideNumbers: false},
			tableName: "CamelCase123Key",
			expected:  "camel_case123_key",
		},
		{
			name:      "Standard casing - underscoreDivideNumbers(false) - numbers and commas",
			intrOpts:  intrOptions{useBlendoCasing: false},
			destOpts:  destOptions{underscoreDivideNumbers: false},
			tableName: "path to $1,00,000",
			expected:  "path_to_1_00_000",
		},
		{
			name:      "Standard casing - underscoreDivideNumbers(false) - no valid characters",
			intrOpts:  intrOptions{useBlendoCasing: false},
			destOpts:  destOptions{underscoreDivideNumbers: false},
			tableName: "@#$%",
			expected:  "",
		},
		{
			name:      "Standard casing - underscoreDivideNumbers(false) - underscores between letters and numbers",
			intrOpts:  intrOptions{useBlendoCasing: false},
			destOpts:  destOptions{underscoreDivideNumbers: false},
			tableName: "test123",
			expected:  "test123",
		},
		{
			name:      "Standard casing - underscoreDivideNumbers(true) - multiple underscore-number sequences",
			intrOpts:  intrOptions{useBlendoCasing: false},
			destOpts:  destOptions{underscoreDivideNumbers: false},
			tableName: "abc123def456",
			expected:  "abc123_def456",
		},
		{
			name:      "Standard casing - underscoreDivideNumbers(false) - multiple underscore-number sequences",
			intrOpts:  intrOptions{useBlendoCasing: false},
			destOpts:  destOptions{underscoreDivideNumbers: false},
			tableName: "abc_123_def_456",
			expected:  "abc_123_def_456",
		},
		{
			name:      "Standard casing - underscoreDivideNumbers(false) - single underscore",
			intrOpts:  intrOptions{useBlendoCasing: false},
			destOpts:  destOptions{underscoreDivideNumbers: false},
			tableName: "__abc_123_def_456",
			expected:  "__abc_123_def_456",
		},
		{
			name:      "Standard casing - underscoreDivideNumbers(false) - multiple underscore",
			intrOpts:  intrOptions{useBlendoCasing: false},
			destOpts:  destOptions{underscoreDivideNumbers: false},
			tableName: "_abc_123_def_456",
			expected:  "_abc_123_def_456",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tableName := transformTableName(&tc.intrOpts, &tc.destOpts, tc.tableName)
			require.Equal(t, tc.expected, tableName)
		})
	}
}

func TestTransformColumnName(t *testing.T) {
	testCases := []struct {
		name      string
		destType  string
		intrOpts  intrOptions
		destOpts  destOptions
		tableName string
		expected  string
	}{
		{
			name:      "Blendo casing - special characters other than \\ or $ to underscores",
			destType:  whutils.SNOWFLAKE,
			intrOpts:  intrOptions{useBlendoCasing: true},
			destOpts:  destOptions{},
			tableName: "column@Name$1",
			expected:  "column_name$1",
		},
		{
			name:      "Blendo casing - add underscore if name does not start with an alphabet or underscore",
			destType:  whutils.SNOWFLAKE,
			intrOpts:  intrOptions{useBlendoCasing: true},
			destOpts:  destOptions{},
			tableName: "1CComega",
			expected:  "_1ccomega",
		},
		{
			name:      "Blendo casing - non-ASCII characters by converting to underscores",
			destType:  whutils.SNOWFLAKE,
			intrOpts:  intrOptions{useBlendoCasing: true},
			destOpts:  destOptions{},
			tableName: "Cízǔ",
			expected:  "c_z_",
		},
		{
			name:      "Blendo casing - CamelCase123Key to camelcase123key",
			destType:  whutils.SNOWFLAKE,
			intrOpts:  intrOptions{useBlendoCasing: true},
			destOpts:  destOptions{},
			tableName: "CamelCase123Key",
			expected:  "camelcase123key",
		},
		{
			name:      "Blendo casing - preserve \\ and $ characters",
			destType:  whutils.SNOWFLAKE,
			intrOpts:  intrOptions{useBlendoCasing: true},
			destOpts:  destOptions{},
			tableName: "path to $1,00,000",
			expected:  "path_to_$1_00_000",
		},
		{
			name:      "Blendo casing - mix of characters, numbers, and special characters",
			destType:  whutils.SNOWFLAKE,
			intrOpts:  intrOptions{useBlendoCasing: true},
			destOpts:  destOptions{},
			tableName: "CamelCase123Key_with$special\\chars",
			expected:  "camelcase123key_with$special\\chars",
		},
		{
			name:      "Blendo casing - limit length to 63 characters for postgres provider",
			destType:  whutils.POSTGRES,
			intrOpts:  intrOptions{useBlendoCasing: true},
			destOpts:  destOptions{},
			tableName: strings.Repeat("a", 70),
			expected:  strings.Repeat("a", 63),
		},

		{
			name:      "Standard casing - underscoreDivideNumbers(true) - remove symbols and join continuous letters and numbers with a single underscore",
			destType:  whutils.SNOWFLAKE,
			intrOpts:  intrOptions{useBlendoCasing: false},
			destOpts:  destOptions{underscoreDivideNumbers: true},
			tableName: "&4yasdfa(84224_fs9##_____*3q",
			expected:  "_4_yasdfa_84224_fs_9_3_q",
		},
		{
			name:      "Standard casing - underscoreDivideNumbers(true) - omega to omega",
			destType:  whutils.SNOWFLAKE,
			intrOpts:  intrOptions{useBlendoCasing: false},
			destOpts:  destOptions{underscoreDivideNumbers: true},
			tableName: "omega",
			expected:  "omega",
		},
		{
			name:      "Standard casing - underscoreDivideNumbers(true) - omega v2 to omega_v_2",
			destType:  whutils.SNOWFLAKE,
			intrOpts:  intrOptions{useBlendoCasing: false},
			destOpts:  destOptions{underscoreDivideNumbers: true},
			tableName: "omega v2",
			expected:  "omega_v_2",
		},
		{
			name:      "Standard casing - underscoreDivideNumbers(true) - prepend underscore if name starts with a number",
			destType:  whutils.SNOWFLAKE,
			intrOpts:  intrOptions{useBlendoCasing: false},
			destOpts:  destOptions{underscoreDivideNumbers: true},
			tableName: "9mega",
			expected:  "_9_mega",
		},
		{
			name:      "Standard casing - underscoreDivideNumbers(true) - remove trailing special characters",
			destType:  whutils.SNOWFLAKE,
			intrOpts:  intrOptions{useBlendoCasing: false},
			destOpts:  destOptions{underscoreDivideNumbers: true},
			tableName: "mega&",
			expected:  "mega",
		},
		{
			name:      "Standard casing - underscoreDivideNumbers(true) - replace special character in the middle with underscore",
			destType:  whutils.SNOWFLAKE,
			intrOpts:  intrOptions{useBlendoCasing: false},
			destOpts:  destOptions{underscoreDivideNumbers: true},
			tableName: "ome$ga",
			expected:  "ome_ga",
		},
		{
			name:      "Standard casing - underscoreDivideNumbers(true) - remove trailing $ character",
			destType:  whutils.SNOWFLAKE,
			intrOpts:  intrOptions{useBlendoCasing: false},
			destOpts:  destOptions{underscoreDivideNumbers: true},
			tableName: "omega$",
			expected:  "omega",
		},
		{
			name:      "Standard casing - underscoreDivideNumbers(true) - spaces and special characters by converting to underscores",
			destType:  whutils.SNOWFLAKE,
			intrOpts:  intrOptions{useBlendoCasing: false},
			destOpts:  destOptions{underscoreDivideNumbers: true},
			tableName: "ome_ ga",
			expected:  "ome_ga",
		},
		{
			name:      "Standard casing - underscoreDivideNumbers(true) - multiple underscores and hyphens by reducing to single underscores",
			destType:  whutils.SNOWFLAKE,
			intrOpts:  intrOptions{useBlendoCasing: false},
			destOpts:  destOptions{underscoreDivideNumbers: true},
			tableName: "9mega________-________90",
			expected:  "_9_mega_90",
		},
		{
			name:      "Standard casing - underscoreDivideNumbers(true) - non-ASCII characters by converting them to underscores",
			destType:  whutils.SNOWFLAKE,
			intrOpts:  intrOptions{useBlendoCasing: false},
			destOpts:  destOptions{underscoreDivideNumbers: true},
			tableName: "Cízǔ",
			expected:  "c_z",
		},
		{
			name:      "Standard casing - underscoreDivideNumbers(true) - CamelCase123Key to camel_case_123_key",
			destType:  whutils.SNOWFLAKE,
			intrOpts:  intrOptions{useBlendoCasing: false},
			destOpts:  destOptions{underscoreDivideNumbers: true},
			tableName: "CamelCase123Key",
			expected:  "camel_case_123_key",
		},
		{
			name:      "Standard casing - underscoreDivideNumbers(true) - numbers and commas",
			destType:  whutils.SNOWFLAKE,
			intrOpts:  intrOptions{useBlendoCasing: false},
			destOpts:  destOptions{underscoreDivideNumbers: true},
			tableName: "path to $1,00,000",
			expected:  "path_to_1_00_000",
		},
		{
			name:      "Standard casing - underscoreDivideNumbers(true) - no valid characters",
			destType:  whutils.SNOWFLAKE,
			intrOpts:  intrOptions{useBlendoCasing: false},
			destOpts:  destOptions{underscoreDivideNumbers: true},
			tableName: "@#$%",
			expected:  "",
		},
		{
			name:      "Standard casing - underscoreDivideNumbers(true) - underscores between letters and numbers",
			destType:  whutils.SNOWFLAKE,
			intrOpts:  intrOptions{useBlendoCasing: false},
			destOpts:  destOptions{underscoreDivideNumbers: true},
			tableName: "test123",
			expected:  "test_123",
		},
		{
			name:      "Standard casing - underscoreDivideNumbers(true) - multiple underscore-number sequences",
			destType:  whutils.SNOWFLAKE,
			intrOpts:  intrOptions{useBlendoCasing: false},
			destOpts:  destOptions{underscoreDivideNumbers: true},
			tableName: "abc123def456",
			expected:  "abc_123_def_456",
		},
		{
			name:      "Standard casing - underscoreDivideNumbers(true) - multiple underscore-number sequences",
			destType:  whutils.SNOWFLAKE,
			intrOpts:  intrOptions{useBlendoCasing: false},
			destOpts:  destOptions{underscoreDivideNumbers: true},
			tableName: "abc_123_def_456",
			expected:  "abc_123_def_456",
		},
		{
			name:      "Standard casing - underscoreDivideNumbers(true) - single underscore",
			destType:  whutils.SNOWFLAKE,
			intrOpts:  intrOptions{useBlendoCasing: false},
			destOpts:  destOptions{underscoreDivideNumbers: true},
			tableName: "__abc_123_def_456",
			expected:  "__abc_123_def_456",
		},
		{
			name:      "Standard casing - underscoreDivideNumbers(true) - multiple underscore",
			destType:  whutils.SNOWFLAKE,
			intrOpts:  intrOptions{useBlendoCasing: false},
			destOpts:  destOptions{underscoreDivideNumbers: true},
			tableName: "_abc_123_def_456",
			expected:  "_abc_123_def_456",
		},

		{
			name:      "Standard casing - underscoreDivideNumbers(false) - remove symbols and join continuous letters and numbers with a single underscore",
			destType:  whutils.SNOWFLAKE,
			intrOpts:  intrOptions{useBlendoCasing: false},
			destOpts:  destOptions{underscoreDivideNumbers: false},
			tableName: "&4yasdfa(84224_fs9##_____*3q",
			expected:  "_4yasdfa_84224_fs9_3q",
		},
		{
			name:      "Standard casing - underscoreDivideNumbers(false) - omega to omega",
			destType:  whutils.SNOWFLAKE,
			intrOpts:  intrOptions{useBlendoCasing: false},
			destOpts:  destOptions{underscoreDivideNumbers: false},
			tableName: "omega",
			expected:  "omega",
		},
		{
			name:      "Standard casing - underscoreDivideNumbers(false) - omega v2 to omega_v_2",
			destType:  whutils.SNOWFLAKE,
			intrOpts:  intrOptions{useBlendoCasing: false},
			destOpts:  destOptions{underscoreDivideNumbers: false},
			tableName: "omega v2",
			expected:  "omega_v2",
		},
		{
			name:      "Standard casing - underscoreDivideNumbers(false) - prepend underscore if name starts with a number",
			destType:  whutils.SNOWFLAKE,
			intrOpts:  intrOptions{useBlendoCasing: false},
			destOpts:  destOptions{underscoreDivideNumbers: false},
			tableName: "9mega",
			expected:  "_9mega",
		},
		{
			name:      "Standard casing - underscoreDivideNumbers(false) - remove trailing special characters",
			destType:  whutils.SNOWFLAKE,
			intrOpts:  intrOptions{useBlendoCasing: false},
			destOpts:  destOptions{underscoreDivideNumbers: false},
			tableName: "mega&",
			expected:  "mega",
		},
		{
			name:      "Standard casing - underscoreDivideNumbers(false) - replace special character in the middle with underscore",
			destType:  whutils.SNOWFLAKE,
			intrOpts:  intrOptions{useBlendoCasing: false},
			destOpts:  destOptions{underscoreDivideNumbers: false},
			tableName: "ome$ga",
			expected:  "ome_ga",
		},
		{
			name:      "Standard casing - underscoreDivideNumbers(false) - remove trailing $ character",
			destType:  whutils.SNOWFLAKE,
			intrOpts:  intrOptions{useBlendoCasing: false},
			destOpts:  destOptions{underscoreDivideNumbers: false},
			tableName: "omega$",
			expected:  "omega",
		},
		{
			name:      "Standard casing - underscoreDivideNumbers(false) - spaces and special characters by converting to underscores",
			destType:  whutils.SNOWFLAKE,
			intrOpts:  intrOptions{useBlendoCasing: false},
			destOpts:  destOptions{underscoreDivideNumbers: false},
			tableName: "ome_ ga",
			expected:  "ome_ga",
		},
		{
			name:      "Standard casing - underscoreDivideNumbers(false) - multiple underscores and hyphens by reducing to single underscores",
			destType:  whutils.SNOWFLAKE,
			intrOpts:  intrOptions{useBlendoCasing: false},
			destOpts:  destOptions{underscoreDivideNumbers: false},
			tableName: "9mega________-________90",
			expected:  "_9mega_90",
		},
		{
			name:      "Standard casing - underscoreDivideNumbers(false) - non-ASCII characters by converting them to underscores",
			destType:  whutils.SNOWFLAKE,
			intrOpts:  intrOptions{useBlendoCasing: false},
			destOpts:  destOptions{underscoreDivideNumbers: false},
			tableName: "Cízǔ",
			expected:  "c_z",
		},
		{
			name:      "Standard casing - underscoreDivideNumbers(false) - CamelCase123Key to camel_case_123_key",
			destType:  whutils.SNOWFLAKE,
			intrOpts:  intrOptions{useBlendoCasing: false},
			destOpts:  destOptions{underscoreDivideNumbers: false},
			tableName: "CamelCase123Key",
			expected:  "camel_case123_key",
		},
		{
			name:      "Standard casing - underscoreDivideNumbers(false) - numbers and commas",
			destType:  whutils.SNOWFLAKE,
			intrOpts:  intrOptions{useBlendoCasing: false},
			destOpts:  destOptions{underscoreDivideNumbers: false},
			tableName: "path to $1,00,000",
			expected:  "path_to_1_00_000",
		},
		{
			name:      "Standard casing - underscoreDivideNumbers(false) - no valid characters",
			destType:  whutils.SNOWFLAKE,
			intrOpts:  intrOptions{useBlendoCasing: false},
			destOpts:  destOptions{underscoreDivideNumbers: false},
			tableName: "@#$%",
			expected:  "",
		},
		{
			name:      "Standard casing - underscoreDivideNumbers(false) - underscores between letters and numbers",
			destType:  whutils.SNOWFLAKE,
			intrOpts:  intrOptions{useBlendoCasing: false},
			destOpts:  destOptions{underscoreDivideNumbers: false},
			tableName: "test123",
			expected:  "test123",
		},
		{
			name:      "Standard casing - underscoreDivideNumbers(false) - multiple underscore-number sequences",
			destType:  whutils.SNOWFLAKE,
			intrOpts:  intrOptions{useBlendoCasing: false},
			destOpts:  destOptions{underscoreDivideNumbers: false},
			tableName: "abc123def456",
			expected:  "abc123_def456",
		},
		{
			name:      "Standard casing - underscoreDivideNumbers(false) - multiple underscore-number sequences",
			destType:  whutils.SNOWFLAKE,
			intrOpts:  intrOptions{useBlendoCasing: false},
			destOpts:  destOptions{underscoreDivideNumbers: false},
			tableName: "abc_123_def_456",
			expected:  "abc_123_def_456",
		},
		{
			name:      "Standard casing - underscoreDivideNumbers(false) - single underscore",
			destType:  whutils.SNOWFLAKE,
			intrOpts:  intrOptions{useBlendoCasing: false},
			destOpts:  destOptions{underscoreDivideNumbers: false},
			tableName: "__abc_123_def_456",
			expected:  "__abc_123_def_456",
		},
		{
			name:      "Standard casing - underscoreDivideNumbers(false) - multiple underscore",
			destType:  whutils.SNOWFLAKE,
			intrOpts:  intrOptions{useBlendoCasing: false},
			destOpts:  destOptions{underscoreDivideNumbers: false},
			tableName: "_abc_123_def_456",
			expected:  "_abc_123_def_456",
		},
		{
			name:      "Standard casing - underscoreDivideNumbers(false) - multiple underscore",
			destType:  whutils.POSTGRES,
			intrOpts:  intrOptions{useBlendoCasing: false},
			destOpts:  destOptions{underscoreDivideNumbers: false},
			tableName: strings.Repeat("a", 70),
			expected:  strings.Repeat("a", 63),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tableName := transformColumnName(tc.destType, &tc.intrOpts, &tc.destOpts, tc.tableName)
			require.Equal(t, tc.expected, tableName)
		})
	}
}
