package arrowbased

import "fmt"

var errArrowRowsNoArrowBatches = "databricks: result set contains 0 arrow batches"
var errArrowRowsUnableToReadBatch = "databricks: unable to read arrow batch"
var errArrowRowsNilArrowSchema = "databricks: nil arrow.Schema"
var errArrowRowsUnableToWriteArrowSchema = "databricks: unable to write arrow schema"
var errArrowRowsInvalidDecimalType = "databricks: decimal type with no scale/precision"
var errArrowRowsUnknownDBType = "databricks: unknown data type when converting to arrow type"
var errArrowRowsDateTimeParse = "databrics: arrow row scanner failed to parse date/time"
var errArrowRowsConvertSchema = "databricks: arrow row scanner failed to convert schema"
var errArrowRowsSerializeSchema = "databricks: arrow row scanner failed to serialize schema"
var errArrowRowsToTimestampFn = "databricks: arrow row scanner failed getting toTimestamp function"
var errArrowRowsMakeColumnValueContainers = "databricks: failed creating column value container"

const errArrowRowsCloudFetchDownloadFailure = "cloud fetch batch loader failed to download results"

func errArrowRowsUnsupportedNativeType(t string) string {
	return fmt.Sprintf("databricks: arrow native values not yet supported for %s", t)
}
func errArrowRowsUnsupportedWithHiveSchema(t string) string {
	return fmt.Sprintf("databricks: arrow native values for %s require arrow schema", t)
}
func errArrowRowsInvalidRowIndex(index int64) string {
	return fmt.Sprintf("databricks: row index %d is not contained in any arrow batch", index)
}
func errArrowRowsUnableToCreateDecimalType(scale, precision int32) string {
	return fmt.Sprintf("databricks: unable to create decimal type scale: %d, precision: %d", scale, precision)
}
func errArrowRowsUnhandledArrowType(t any) string {
	return fmt.Sprintf("databricks: arrow row scanner unhandled type %s", t)
}
func errArrowRowsColumnValue(name string) string {
	return fmt.Sprintf("databricks: arrow row scanner failed getting column value for %s", name)
}
