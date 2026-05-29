package bqstreaming

import (
	"sort"

	"cloud.google.com/go/bigquery"
	"github.com/samber/lo"

	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

// dataTypesMap maps datatype stored in rudder to datatype in bigquery
var dataTypesMap = map[string]bigquery.FieldType{
	"boolean":  bigquery.BooleanFieldType,
	"int":      bigquery.IntegerFieldType,
	"float":    bigquery.FloatFieldType,
	"string":   bigquery.StringFieldType,
	"datetime": bigquery.TimestampFieldType,
}

// toBigQuerySchema converts a rudder table schema into a BigQuery schema.
//
// The fields MUST be emitted in a deterministic (sorted) order. adapt.StorageSchemaToProto2Descriptor
// assigns proto field numbers by position, and the descriptor is built independently for both the
// row encoder (descriptorForSchema) and the managed stream (NewTableWriter). If the order differed
// between those two calls, the encoded field numbers would not line up with the stream's schema
// descriptor and BigQuery would decode every column into the wrong/mismatched field, surfacing as
// all-NULL rows.
func toBigQuerySchema(schema whutils.ModelTableSchema) bigquery.Schema {
	columnNames := lo.Keys(schema)
	sort.Strings(columnNames)
	return lo.Map(columnNames, func(columnName string, _ int) *bigquery.FieldSchema {
		return &bigquery.FieldSchema{Name: columnName, Type: dataTypesMap[schema[columnName]]}
	})
}
