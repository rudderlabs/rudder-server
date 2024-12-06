package snowflake

var dataTypesMap = map[string]string{
	"boolean":  "boolean",
	"int":      "number",
	"bigint":   "number",
	"float":    "double precision",
	"string":   "varchar",
	"datetime": "timestamp_tz",
	"json":     "variant",
}

var dataTypesMapToRudder = map[string]string{
	"NUMBER":           "int",
	"DECIMAL":          "int",
	"NUMERIC":          "int",
	"INT":              "int",
	"INTEGER":          "int",
	"BIGINT":           "int",
	"SMALLINT":         "int",
	"FLOAT":            "float",
	"FLOAT4":           "float",
	"FLOAT8":           "float",
	"DOUBLE":           "float",
	"REAL":             "float",
	"DOUBLE PRECISION": "float",
	"BOOLEAN":          "boolean",
	"TEXT":             "string",
	"VARCHAR":          "string",
	"CHAR":             "string",
	"CHARACTER":        "string",
	"STRING":           "string",
	"BINARY":           "string",
	"VARBINARY":        "string",
	"TIMESTAMP_NTZ":    "datetime",
	"DATE":             "datetime",
	"DATETIME":         "datetime",
	"TIME":             "datetime",
	"TIMESTAMP":        "datetime",
	"TIMESTAMP_LTZ":    "datetime",
	"TIMESTAMP_TZ":     "datetime",
	"VARIANT":          "json",
}

func CalculateDataType(columnType string, numericScale int64) (string, bool) {
	if datatype, ok := dataTypesMapToRudder[columnType]; ok {
		if datatype == "int" && numericScale > 0 {
			datatype = "float"
		}
		return datatype, true
	}
	return "", false
}
