package transformer

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/iancoleman/strcase"

	"github.com/rudderlabs/rudder-go-kit/config"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/transformer/internal/reservedkeywords"
	"github.com/rudderlabs/rudder-server/warehouse/transformer/internal/response"
	"github.com/rudderlabs/rudder-server/warehouse/transformer/internal/snakecase"
	"github.com/rudderlabs/rudder-server/warehouse/transformer/internal/utils"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

const (
	postgresMaxIdentifierLength = 63
)

var (
	reLeadingUnderscores           = regexp.MustCompile(`^_*`)
	reNonAlphanumericOrDollar      = regexp.MustCompile(`[^a-zA-Z0-9\\$]`)
	reStartsWithLetterOrUnderscore = regexp.MustCompile(`^[a-zA-Z_].*`)
)

// safeNamespace returns a safe namespace for the given destination type and input namespace.
// The namespace is transformed by removing special characters, converting to snake case,
// and ensuring its safe (not starting with a digit, not empty, and not a reserved keyword).
func safeNamespace(conf *config.Config, destType, input string) string {
	namespace := strings.Join(extractAlphanumericValues(input), "_")

	if !shouldSkipSnakeCasing(conf, destType) {
		namespace = strcase.ToSnake(namespace)
	}
	if startsWithDigit(namespace) || reservedkeywords.IsNamespace(destType, namespace) {
		namespace = "_" + namespace
	}
	if namespace == "" {
		namespace = "stringempty"
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

func safeTableNameCached(tec *transformEventContext, key string) (string, error) {
	var tableName string
	var err error

	cacheKey := safeTableNameCacheKey{
		destType:                     tec.event.Metadata.DestinationType,
		tableName:                    key,
		skipReservedKeywordsEscaping: tec.intrOpts.skipReservedKeywordsEscaping,
	}
	columnNameCached, ok := tec.cache.safeTableNameCache.Load(cacheKey)
	if !ok {
		tableName, err = safeTableName(tec.event.Metadata.DestinationType, tec.intrOpts, key)
		if err != nil {
			return "", err
		}
		tec.cache.safeTableNameCache.Store(cacheKey, tableName)
	} else {
		tableName = columnNameCached.(string)
	}
	return tableName, nil
}

// safeTableName processes the input table name based on the destination type and integration options.
// It applies case conversion, truncation, reserved keyword escaping, and table name length restrictions.
// For data lake providers, it avoids trimming the table name.
func safeTableName(destType string, intrOpts *intrOptions, tableName string) (string, error) {
	if len(tableName) == 0 {
		return "", response.ErrEmptyTableName
	}
	return safeName(destType, intrOpts, tableName), nil
}

func safeColumnNameCached(tec *transformEventContext, key string) (string, error) {
	var tableName string
	var err error

	cacheKey := safeColumnNameCacheKey{
		destType:                     tec.event.Metadata.DestinationType,
		columnName:                   key,
		skipReservedKeywordsEscaping: tec.intrOpts.skipReservedKeywordsEscaping,
	}
	columnNameCached, ok := tec.cache.safeColumnNameCache.Load(cacheKey)
	if !ok {
		tableName, err = safeColumnName(tec.event.Metadata.DestinationType, tec.intrOpts, key)
		if err != nil {
			return "", err
		}
		tec.cache.safeColumnNameCache.Store(cacheKey, tableName)
	} else {
		tableName = columnNameCached.(string)
	}
	return tableName, nil
}

// safeColumnName processes the input column name based on the destination type and integration options.
// It applies case conversion, truncation, reserved keyword escaping, and column name length restrictions.
// For data lake providers, it avoids trimming the column name.
func safeColumnName(destType string, intrOpts *intrOptions, columnName string) (string, error) {
	if len(columnName) == 0 {
		return "", response.ErrEmptyColumnName
	}
	return safeName(destType, intrOpts, columnName), nil
}

func safeName(destType string, intrOpts *intrOptions, name string) string {
	switch destType {
	case whutils.SNOWFLAKE, whutils.SnowpipeStreaming:
		name = strings.ToUpper(name)
	case whutils.POSTGRES:
		name = misc.TruncateStr(name, postgresMaxIdentifierLength)
		name = strings.ToLower(name)
	default:
		name = strings.ToLower(name)
	}
	if !intrOpts.skipReservedKeywordsEscaping && reservedkeywords.IsTableOrColumn(destType, name) {
		name = "_" + name
	}
	if utils.IsDataLake(destType) {
		return name
	}
	return misc.TruncateStr(name, 127)
}

func transformTableNameCached(tec *transformEventContext, key string) string {
	var tableName string

	cacheKey := transformTableNameCacheKey{
		underscoreDivideNumbers: tec.destOpts.underscoreDivideNumbers,
		useBlendoCasing:         tec.intrOpts.useBlendoCasing,
		tableName:               key,
	}
	columnNameCached, ok := tec.cache.transformTableNameCache.Load(cacheKey)
	if !ok {
		tableName = transformTableName(tec.intrOpts, tec.destOpts, key)
		tec.cache.transformTableNameCache.Store(cacheKey, tableName)
	} else {
		tableName = columnNameCached.(string)
	}
	return tableName
}

// transformTableName applies transformation to the input table name based on the destination type and configuration options.
// If `useBlendoCasing` is enabled, it converts the table name to lowercase and trims spaces.
// Otherwise, it applies a more general transformation using the `transformName` function.
func transformTableName(intrOpts *intrOptions, destOpts *destOptions, tableName string) string {
	if intrOpts.useBlendoCasing {
		return strings.TrimSpace(strings.ToLower(tableName))
	}

	var snakeCaseFn func(s string) string
	if destOpts.underscoreDivideNumbers {
		snakeCaseFn = snakecase.ToSnakeCase
	} else {
		snakeCaseFn = snakecase.ToSnakeCaseWithNumbers
	}

	name := strings.Join(extractAlphanumericValues(tableName), "_")
	if strings.HasPrefix(tableName, "_") {
		name = reLeadingUnderscores.FindString(tableName) + snakeCaseFn(reLeadingUnderscores.ReplaceAllString(name, ""))
	} else {
		name = snakeCaseFn(name)
	}
	if startsWithDigit(name) {
		name = "_" + name
	}
	return name
}

func transformColumnNameCached(tec *transformEventContext, key string) string {
	var columnName string

	cacheKey := transformColumnNameCacheKey{
		destType:                tec.event.Metadata.DestinationType,
		columnName:              key,
		underscoreDivideNumbers: tec.destOpts.underscoreDivideNumbers,
		useBlendoCasing:         tec.intrOpts.useBlendoCasing,
	}
	columnNameCached, ok := tec.cache.transformColumnNameCache.Load(cacheKey)
	if !ok {
		columnName = transformColumnName(tec.event.Metadata.DestinationType, tec.intrOpts, tec.destOpts, key)
		tec.cache.transformColumnNameCache.Store(cacheKey, columnName)
	} else {
		columnName = columnNameCached.(string)
	}
	return columnName
}

// transformColumnName applies transformation to the input column name based on the destination type and configuration options.
// If `useBlendoCasing` is enabled, it transforms the column name into Blendo casing.
// Otherwise, it applies a more general transformation using the `transformName` function.
func transformColumnName(destType string, intrOpts *intrOptions, destOpts *destOptions, columnName string) string {
	if intrOpts.useBlendoCasing {
		return transformNameToBlendoCase(destType, columnName)
	}

	var snakeCaseFn func(s string) string
	if destOpts.underscoreDivideNumbers {
		snakeCaseFn = snakecase.ToSnakeCase
	} else {
		snakeCaseFn = snakecase.ToSnakeCaseWithNumbers
	}

	name := strings.Join(extractAlphanumericValues(columnName), "_")
	if strings.HasPrefix(columnName, "_") {
		name = reLeadingUnderscores.FindString(columnName) + snakeCaseFn(reLeadingUnderscores.ReplaceAllString(name, ""))
	} else {
		name = snakeCaseFn(name)
	}
	if startsWithDigit(name) {
		name = "_" + name
	}
	if destType == whutils.POSTGRES {
		name = misc.TruncateStr(name, postgresMaxIdentifierLength)
	}
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
// The name is truncated to postgresMaxIdentifierLength characters for Postgres, and the result is converted to lowercase.
func transformNameToBlendoCase(destType, name string) string {
	key := reNonAlphanumericOrDollar.ReplaceAllString(name, "_")

	if !reStartsWithLetterOrUnderscore.MatchString(key) {
		key = "_" + key
	}
	if destType == whutils.POSTGRES {
		key = misc.TruncateStr(name, postgresMaxIdentifierLength)
	}
	return strings.ToLower(key)
}
