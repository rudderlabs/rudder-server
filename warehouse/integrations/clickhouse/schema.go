package clickhouse

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

const (
	partitionField = "received_at"
)

// clickhouse doesn't support bool, they recommend to use Uint8 and set 1,0
var rudderDataTypesMapToClickHouse = map[string]string{
	"int":             "Int64",
	"array(int)":      "Array(Int64)",
	"float":           "Float64",
	"array(float)":    "Array(Float64)",
	"string":          "String",
	"array(string)":   "Array(String)",
	"datetime":        "DateTime",
	"array(datetime)": "Array(DateTime)",
	"boolean":         "UInt8",
	"array(boolean)":  "Array(UInt8)",
	"json":            "JSON", // needs ClickHouse 25.3 or newer, so only v2 destinations get json columns
}

var datatypeDefaultValuesMap = map[string]any{
	"int":      0,
	"float":    0.0,
	"boolean":  0,
	"datetime": clickhouseDefaultDateTime,
	"json":     "{}",
}

var clickhouseDataTypesMapToRudder = map[string]string{
	"Int8":                             "int",
	"Int16":                            "int",
	"Int32":                            "int",
	"Int64":                            "int",
	"Array(Int64)":                     "array(int)",
	"Array(Nullable(Int64))":           "array(int)",
	"Float32":                          "float",
	"Float64":                          "float",
	"Array(Float64)":                   "array(float)",
	"Array(Nullable(Float64))":         "array(float)",
	"String":                           "string",
	"JSON":                             "json",
	"Array(String)":                    "array(string)",
	"Array(Nullable(String))":          "array(string)",
	"DateTime":                         "datetime",
	"Array(DateTime)":                  "array(datetime)",
	"Array(Nullable(DateTime))":        "array(datetime)",
	"UInt8":                            "boolean",
	"Array(UInt8)":                     "array(boolean)",
	"Array(Nullable(UInt8))":           "array(boolean)",
	"LowCardinality(String)":           "string",
	"LowCardinality(Nullable(String))": "string",
	"Nullable(Int8)":                   "int",
	"Nullable(Int16)":                  "int",
	"Nullable(Int32)":                  "int",
	"Nullable(Int64)":                  "int",
	"Nullable(Float32)":                "float",
	"Nullable(Float64)":                "float",
	"Nullable(String)":                 "string",
	"Nullable(DateTime)":               "datetime",
	"Nullable(UInt8)":                  "boolean",
	"Nullable(JSON)":                   "json",
	"SimpleAggregateFunction(anyLast, Nullable(Int8))":     "int",
	"SimpleAggregateFunction(anyLast, Nullable(Int16))":    "int",
	"SimpleAggregateFunction(anyLast, Nullable(Int32))":    "int",
	"SimpleAggregateFunction(anyLast, Nullable(Int64))":    "int",
	"SimpleAggregateFunction(anyLast, Nullable(Float32))":  "float",
	"SimpleAggregateFunction(anyLast, Nullable(Float64))":  "float",
	"SimpleAggregateFunction(anyLast, Nullable(String))":   "string",
	"SimpleAggregateFunction(anyLast, Nullable(DateTime))": "datetime",
	"SimpleAggregateFunction(anyLast, Nullable(UInt8))":    "boolean",
	"SimpleAggregateFunction(anyLast, Nullable(JSON))":     "json",
}

var errorsMappings = []model.JobError{
	{
		Type:   model.PermissionError,
		Format: regexp.MustCompile(`code: 516, message: .*: Authentication failed: password is incorrect, or there is no user with such name`),
	},
	{
		Type:   model.InsufficientResourceError,
		Format: regexp.MustCompile(`code: 241, message: Memory limit .* exceeded: would use .*, maximum: .*`),
	},
}

func getClickhouseColumnTypeForSpecificColumn(columnName, columnType string, isNullable bool) string {
	specificColumnType := columnType
	if strings.Contains(specificColumnType, "Array") {
		return specificColumnType
	}
	if isNullable {
		specificColumnType = fmt.Sprintf("Nullable(%s)", specificColumnType)
	}
	if _, ok := clickhouseSpecificColumnNameMappings[columnName]; ok {
		specificColumnType = clickhouseSpecificColumnNameMappings[columnName]
	}

	return specificColumnType
}

// getClickHouseColumnTypeForSpecificTable gets suitable columnType based on the tableName

func generateArgumentString(length int) string {
	var args []string
	for range length {
		args = append(args, "?")
	}
	return strings.Join(args, ",")
}

func getSortKeyTuple(sortKeyFields []string) string {
	var tuple strings.Builder
	tuple.WriteString("(")
	for index, field := range sortKeyFields {
		if index == len(sortKeyFields)-1 {
			tuple.WriteString(warehouseutils.ClickHouseQuoteIdentifier(field))
		} else {
			fmt.Fprintf(&tuple, `%s,`, warehouseutils.ClickHouseQuoteIdentifier(field))
		}
	}
	tuple.WriteString(")")
	return tuple.String()
}

// CreateTable creates table with engine ReplacingMergeTree(), this is used for dedupe event data and replace it will the latest data if duplicate data found. This logic is handled by clickhouse
// The engine differs from MergeTree in that it removes duplicate entries with the same sorting key value.

var clickhouseDefaultDateTime, _ = time.Parse(time.RFC3339, "1970-01-01 00:00:00")

// clickhouse doesn't support bool, they recommend to use Uint8 and set 1,0

var clickhouseSpecificColumnNameMappings = map[string]string{
	"event":      "LowCardinality(String)",
	"event_text": "LowCardinality(String)",
}
