package transformer

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/iancoleman/strcase"

	"github.com/rudderlabs/rudder-go-kit/config"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/transformer/internal/response"
	"github.com/rudderlabs/rudder-server/warehouse/transformer/internal/snakecase"
	"github.com/rudderlabs/rudder-server/warehouse/transformer/internal/utils"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

var (
	reLeadingUnderscores           = regexp.MustCompile(`^_*`)
	reNonAlphanumericOrDollar      = regexp.MustCompile(`[^a-zA-Z0-9\\$]`)
	reStartsWithLetterOrUnderscore = regexp.MustCompile(`^[a-zA-Z_].*`)
)

// SafeNamespace returns a safe namespace for the given destination type and input namespace.
// The namespace is transformed by removing special characters, converting to snake case,
// and ensuring its safe (not starting with a digit, not empty, and not a reserved keyword).
func SafeNamespace(conf *config.Config, destType, input string) string {
	namespace := strings.Join(extractAlphanumericValues(input), "_")

	if !shouldSkipSnakeCasing(conf, destType) {
		namespace = strcase.ToSnake(namespace)
	}
	if startsWithDigit(namespace) {
		namespace = "_" + namespace
	}
	if namespace == "" {
		namespace = "stringempty"
	}
	if utils.IsReservedKeywordForNamespaces(destType, namespace) {
		namespace = "_" + namespace
	}
	return misc.TruncateStr(namespace, 127)
}

func extractAlphanumericValues(input string) []string {
	var (
		extractedValues []string
		currentValue    strings.Builder
	)

	for _, c := range input {
		if isAlphaAlphanumeric(c) {
			currentValue.WriteRune(c)
		} else if currentValue.Len() > 0 {
			extractedValues = append(extractedValues, currentValue.String())
			currentValue.Reset()
		}
	}
	if currentValue.Len() > 0 {
		extractedValues = append(extractedValues, currentValue.String())
	}
	return extractedValues
}

func isAlphaAlphanumeric(c int32) bool {
	return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')
}

func shouldSkipSnakeCasing(conf *config.Config, destType string) bool {
	configKey := fmt.Sprintf("Warehouse.%s.skipNamespaceSnakeCasing", whutils.WHDestNameMap[destType])
	return conf.GetBool(configKey, false)
}

// SafeTableName processes the input table name based on the destination type and integration options.
// It applies case conversion, truncation, reserved keyword escaping, and table name length restrictions.
// For data lake providers, it avoids trimming the table name.
func SafeTableName(destType string, options integrationsOptions, tableName string) (string, error) {
	if len(tableName) == 0 {
		return "", response.ErrEmptyTableName
	}
	return safeName(destType, options, tableName), nil
}

// SafeColumnName processes the input column name based on the destination type and integration options.
// It applies case conversion, truncation, reserved keyword escaping, and column name length restrictions.
// For data lake providers, it avoids trimming the column name.
func SafeColumnName(destType string, options integrationsOptions, columnName string) (string, error) {
	if len(columnName) == 0 {
		return "", response.ErrEmptyColumnName
	}
	return safeName(destType, options, columnName), nil
}

func safeName(destType string, options integrationsOptions, name string) string {
	switch destType {
	case whutils.SNOWFLAKE:
		name = strings.ToUpper(name)
	case whutils.POSTGRES:
		name = misc.TruncateStr(name, 63)
		name = strings.ToLower(name)
	default:
		name = strings.ToLower(name)
	}

	if !options.skipReservedKeywordsEscaping && utils.IsReservedKeywordForColumnsTables(destType, name) {
		name = "_" + name
	}
	if utils.IsDataLake(destType) {
		return name
	}
	return misc.TruncateStr(name, 127)
}

var (
	transformTableNameCache = make(map[string]string)
)

// TransformTableName applies transformation to the input table name based on the destination type and configuration options.
// If `useBlendoCasing` is enabled, it converts the table name to lowercase and trims spaces.
// Otherwise, it applies a more general transformation using the `transformName` function.
func TransformTableName(integrationsOptions integrationsOptions, destConfigOptions destConfigOptions, tableName string) string {
	// FIXME: not thread safe, ignores integrations/destConfigOptions
	if cachedName, ok := transformTableNameCache[tableName]; ok {
		return cachedName
	}

	if integrationsOptions.useBlendoCasing {
		return strings.TrimSpace(strings.ToLower(tableName))
	}
	name := strings.Join(extractAlphanumericValues(tableName), "_")

	var snakeCaseFn func(s string) string
	if destConfigOptions.underscoreDivideNumbers {
		snakeCaseFn = snakecase.ToSnakeCase
	} else {
		snakeCaseFn = snakecase.ToSnakeCaseWithNumbers
	}
	if strings.HasPrefix(tableName, "_") {
		name = reLeadingUnderscores.FindString(tableName) + snakeCaseFn(reLeadingUnderscores.ReplaceAllString(name, ""))
	} else {
		name = snakeCaseFn(name)
	}
	if startsWithDigit(name) {
		name = "_" + name
	}
	transformColumnNameCache[tableName] = name

	return name
}

var (
	transformColumnNameCache = make(map[string]string)
)

// TransformColumnName applies transformation to the input column name based on the destination type and configuration options.
// If `useBlendoCasing` is enabled, it transforms the column name into Blendo casing.
// Otherwise, it applies a more general transformation using the `transformName` function.
func TransformColumnName(destType string, integrationsOptions integrationsOptions, destConfigOptions destConfigOptions, columnName string) string {
	// FIXME: not thread safe, ignores integrations/destConfigOptions
	if cachedName, ok := transformColumnNameCache[columnName]; ok {
		return cachedName
	}

	if integrationsOptions.useBlendoCasing {
		return transformNameToBlendoCase(destType, columnName)
	}

	name := strings.Join(extractAlphanumericValues(columnName), "_")

	var snakeCaseFn func(s string) string
	if destConfigOptions.underscoreDivideNumbers {
		snakeCaseFn = snakecase.ToSnakeCase
	} else {
		snakeCaseFn = snakecase.ToSnakeCaseWithNumbers
	}
	if strings.HasPrefix(columnName, "_") {
		name = reLeadingUnderscores.FindString(columnName) + snakeCaseFn(reLeadingUnderscores.ReplaceAllString(name, ""))
	} else {
		name = snakeCaseFn(name)
	}
	if startsWithDigit(name) {
		name = "_" + name
	}
	if destType == whutils.POSTGRES {
		name = misc.TruncateStr(name, 63)
	}

	transformColumnNameCache[columnName] = name
	return name
}

func startsWithDigit(name string) bool {
	if len(name) > 0 && (rune(name[0]) >= '0' && rune(name[0]) <= '9') {
		return true
	}
	return false
}

// transformNameToBlendoCase converts the input string into Blendo case format by replacing non-alphanumeric characters with underscores.
// If the name does not start with a letter or underscore, it adds a leading underscore.
// The name is truncated to 63 characters for Postgres, and the result is converted to lowercase.
func transformNameToBlendoCase(destType, name string) string {
	key := reNonAlphanumericOrDollar.ReplaceAllString(name, "_")

	if !reStartsWithLetterOrUnderscore.MatchString(key) {
		key = "_" + key
	}
	if destType == whutils.POSTGRES {
		key = misc.TruncateStr(name, 63)
	}
	return strings.ToLower(key)
}
