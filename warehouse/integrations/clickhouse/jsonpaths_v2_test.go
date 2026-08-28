package clickhouse

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

const jsonColumn = "context_props"

// How every type is declared, across the tables and flags that change it. JSON
// is not an exception: it takes the Nullable wrapper and the users aggregate
// exactly like the scalars. The rest of the matrix is here so a change to the
// JSON path cannot quietly move another type.
func TestColumnTypesV2(t *testing.T) {
	testCases := []struct {
		name               string
		dataType           string
		tableName          string
		disableNullable    bool
		notNullableColumns []string
		want               string
	}{
		// json takes the Nullable wrapper like any scalar: ClickHouse has
		// allowed Nullable(JSON) since 25.3, and it merges identically to the
		// bare type under both ReplacingMergeTree and AggregatingMergeTree
		{name: "json", dataType: model.JSONDataType, tableName: "product_track", want: "Nullable(JSON)"},
		{name: "json in users takes the aggregate", dataType: model.JSONDataType, tableName: warehouseutils.UsersTable, want: "SimpleAggregateFunction(anyLast, Nullable(JSON))"},
		{name: "json in identifies", dataType: model.JSONDataType, tableName: warehouseutils.IdentifiesTable, want: "Nullable(JSON)"},
		{name: "json with nullable disabled", dataType: model.JSONDataType, tableName: "product_track", disableNullable: true, want: "JSON"},
		{name: "json in identifies keeps nullable when disabled", dataType: model.JSONDataType, tableName: warehouseutils.IdentifiesTable, disableNullable: true, want: "Nullable(JSON)"},
		{name: "json marked not nullable", dataType: model.JSONDataType, tableName: "product_track", notNullableColumns: []string{jsonColumn}, want: "JSON"},
		// nullable disabled short circuits before the users branch, so the
		// aggregate wrapper is dropped for every type, json included
		{name: "json in users with nullable disabled", dataType: model.JSONDataType, tableName: warehouseutils.UsersTable, disableNullable: true, want: "JSON"},

		// arrays: the only never-Nullable type, since ClickHouse rejects
		// Nullable(Array(...)) outright
		{name: "array", dataType: "array(string)", tableName: "product_track", want: "Array(String)"},
		{name: "array in users takes the aggregate", dataType: "array(string)", tableName: warehouseutils.UsersTable, want: "SimpleAggregateFunction(anyLast, Array(String))"},
		{name: "array with nullable disabled", dataType: "array(string)", tableName: "product_track", disableNullable: true, want: "Array(String)"},

		// scalars: still Nullable
		{name: "string", dataType: model.StringDataType, tableName: "product_track", want: "Nullable(String)"},
		{name: "string in users", dataType: model.StringDataType, tableName: warehouseutils.UsersTable, want: "SimpleAggregateFunction(anyLast, Nullable(String))"},
		{name: "string with nullable disabled", dataType: model.StringDataType, tableName: "product_track", disableNullable: true, want: "String"},
		{name: "string in identifies keeps nullable when disabled", dataType: model.StringDataType, tableName: warehouseutils.IdentifiesTable, disableNullable: true, want: "Nullable(String)"},
		{name: "string in users with nullable disabled loses the aggregate", dataType: model.StringDataType, tableName: warehouseutils.UsersTable, disableNullable: true, want: "String"},
		{name: "string marked not nullable", dataType: model.StringDataType, tableName: "product_track", notNullableColumns: []string{jsonColumn}, want: "String"},
		{name: "int", dataType: model.IntDataType, tableName: "product_track", want: "Nullable(Int64)"},
		{name: "boolean", dataType: model.BooleanDataType, tableName: "product_track", want: "Nullable(UInt8)"},
		{name: "datetime", dataType: model.DateTimeDataType, tableName: "product_track", want: "Nullable(DateTime)"},

		// only datetime picks up a codec, so json never carries one
		{name: "datetime with nullable disabled takes a codec", dataType: model.DateTimeDataType, tableName: "product_track", disableNullable: true, want: "DateTime Codec(DoubleDelta, LZ4)"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ch := NewV2(config.New(), logger.NOP, stats.NOP)
			ch.config.disableNullable = tc.disableNullable

			got := ch.ColumnsWithDataTypes(
				tc.tableName,
				model.TableSchema{jsonColumn: tc.dataType},
				tc.notNullableColumns,
			)
			require.Equal(t, `"`+jsonColumn+`" `+tc.want, strings.TrimSpace(got))
		})
	}
}

// A json column alongside others: every column keeps its own declaration. The
// order follows map iteration, so the parts are compared rather than the string.
func TestJSONColumnAmongOthersV2(t *testing.T) {
	ch := NewV2(config.New(), logger.NOP, stats.NOP)

	got := ch.ColumnsWithDataTypes("product_track", model.TableSchema{
		"context_props": model.JSONDataType,
		"name":          model.StringDataType,
		"rating":        model.IntDataType,
		"tags":          "array(string)",
	}, nil)

	parts := strings.Split(got, ",")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	require.ElementsMatch(t, []string{
		`"context_props" Nullable(JSON)`,
		`"name" Nullable(String)`,
		`"rating" Nullable(Int64)`,
		`"tags" Array(String)`,
	}, parts)
}

// What each type binds to, including the types the json case must not disturb.
func TestBindValueV2(t *testing.T) {
	validTime, err := time.Parse(time.RFC3339, "2022-12-15T06:53:49.640Z")
	require.NoError(t, err)

	testCases := []struct {
		name            string
		dataType        string
		data            string
		disableNullable bool
		want            any
	}{
		// json: the payload is handed to the driver as written
		{name: "json object", dataType: model.JSONDataType, data: `{"revenue":10,"currency":"USD"}`, want: `{"revenue":10,"currency":"USD"}`},
		{name: "json array", dataType: model.JSONDataType, data: `[1,2,3]`, want: `[1,2,3]`},
		{name: "json nested", dataType: model.JSONDataType, data: `{"a":{"b":[1,2]}}`, want: `{"a":{"b":[1,2]}}`},
		{name: "json null literal", dataType: model.JSONDataType, data: `null`, want: `null`},
		{name: "json unicode", dataType: model.JSONDataType, data: `{"city":"बेंगलुरु"}`, want: `{"city":"बेंगलुरु"}`},
		{name: "json with embedded quotes", dataType: model.JSONDataType, data: `{"q":"say \"hi\""}`, want: `{"q":"say \"hi\""}`},
		// surrounding whitespace is left alone: only the emptiness check trims
		{name: "json padded", dataType: model.JSONDataType, data: ` {"a":1} `, want: ` {"a":1} `},
		// an empty cell is not valid JSON, so it takes the same fallback as a
		// failed parse: null, or the typed default when nullable is off
		{name: "json empty", dataType: model.JSONDataType, data: "", want: nil},
		{name: "json whitespace", dataType: model.JSONDataType, data: "   ", want: nil},
		{name: "json empty with nullable disabled", dataType: model.JSONDataType, data: "", disableNullable: true, want: "{}"},
		{name: "json whitespace with nullable disabled", dataType: model.JSONDataType, data: "   ", disableNullable: true, want: "{}"},

		// strings keep an empty cell as an empty string, unlike json
		{name: "string", dataType: model.StringDataType, data: "RudderStack", want: "RudderStack"},
		{name: "string empty stays empty", dataType: model.StringDataType, data: "", want: ""},
		{name: "string that looks like json", dataType: model.StringDataType, data: `{"a":1}`, want: `{"a":1}`},

		// scalars fall back to null, or to the type default when nullable is off
		{name: "int", dataType: model.IntDataType, data: "42", want: int64(42)},
		{name: "int invalid", dataType: model.IntDataType, data: "abc", want: nil},
		{name: "int invalid with nullable disabled", dataType: model.IntDataType, data: "abc", disableNullable: true, want: 0},
		{name: "float", dataType: model.FloatDataType, data: "42.5", want: 42.5},
		{name: "float invalid with nullable disabled", dataType: model.FloatDataType, data: "abc", disableNullable: true, want: 0.0},
		{name: "boolean true", dataType: model.BooleanDataType, data: "true", want: uint8(1)},
		{name: "boolean false", dataType: model.BooleanDataType, data: "false", want: uint8(0)},
		{name: "boolean invalid", dataType: model.BooleanDataType, data: "abc", want: nil},
		{name: "datetime", dataType: model.DateTimeDataType, data: "2022-12-15T06:53:49.640Z", want: validTime},
		{name: "datetime invalid", dataType: model.DateTimeDataType, data: "not a time", want: nil},
		{name: "datetime invalid with nullable disabled", dataType: model.DateTimeDataType, data: "not a time", disableNullable: true, want: clickhouseDefaultDateTime},

		// arrays still go through the array cast
		{name: "array of string", dataType: "array(string)", data: `["a","b"]`, want: []string{"a", "b"}},
		{name: "array of int", dataType: "array(int)", data: `[1,2]`, want: []int64{1, 2}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ch := NewV2(config.New(), logger.NOP, stats.NOP)
			ch.config.disableNullable = tc.disableNullable

			require.Equal(t, tc.want, ch.bindValue(tc.data, tc.dataType))
		})
	}
}

// json maps to the native type both ways, and the other mappings are untouched.
func TestJSONTypeMappingsV2(t *testing.T) {
	require.Equal(t, "JSON", rudderDataTypesMapToClickHouse[model.JSONDataType])
	require.Equal(t, model.JSONDataType, clickhouseDataTypesMapToRudder["JSON"])

	// A table this code did not create can still carry the Nullable renderings,
	// so they map back too, the same way the map already tolerates Int8 and
	// Array(Nullable(String)) that CreateTable never emits.
	require.Equal(t, model.JSONDataType, clickhouseDataTypesMapToRudder["Nullable(JSON)"])
	require.Equal(t, model.JSONDataType, clickhouseDataTypesMapToRudder["SimpleAggregateFunction(anyLast, Nullable(JSON))"])

	require.Equal(t, "String", rudderDataTypesMapToClickHouse[model.StringDataType])
	require.Equal(t, model.StringDataType, clickhouseDataTypesMapToRudder["Nullable(String)"])

	// json carries a typed default like every other non-array type, used when
	// nullable columns are disabled and there is no null to fall back to
	require.Equal(t, "{}", datatypeDefaultValuesMap[model.JSONDataType])
}

// FetchSchema looks a rendering up in clickhouseDataTypesMapToRudder as a raw
// string, and a miss drops the column while only bumping a missing-datatype
// counter. So every rendering CreateTable can emit for a json column - across
// the table shapes, both nullable modes and the not-nullable override - has to
// map back.
func TestJSONRenderingsMapBackV2(t *testing.T) {
	tables := []string{"product_track", warehouseutils.UsersTable, warehouseutils.IdentifiesTable}

	for _, disableNullable := range []bool{false, true} {
		for _, notNullable := range []bool{false, true} {
			for _, tableName := range tables {
				name := fmt.Sprintf("%s/disableNullable=%t/notNullable=%t", tableName, disableNullable, notNullable)

				t.Run(name, func(t *testing.T) {
					conf := config.New()
					conf.Set("Warehouse.clickhouse.v2.disableNullable", disableNullable)

					var notNullableColumns []string
					if notNullable {
						notNullableColumns = []string{jsonColumn}
					}

					ch := NewV2(conf, logger.NOP, stats.NOP)
					declared := ch.ColumnsWithDataTypes(tableName, model.TableSchema{jsonColumn: model.JSONDataType}, notNullableColumns)
					columnType := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(declared), `"`+jsonColumn+`"`))

					require.Equal(t, model.JSONDataType, clickhouseDataTypesMapToRudder[columnType],
						"no reverse mapping for %q", columnType)
				})
			}
		}
	}
}
