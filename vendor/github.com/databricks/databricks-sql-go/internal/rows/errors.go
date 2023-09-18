package rows

import "fmt"

var errRowsFetchPriorToStart = "databricks: unable to fetch row page prior to start of results"
var errRowsNoClient = "databricks: instance of Rows missing client"
var errRowsNilRows = "databricks: nil Rows instance"
var errRowsUnknowRowType = "databricks: unknown rows representation"
var errRowsCloseFailed = "databricks: Rows instance Close operation failed"
var errRowsMetadataFetchFailed = "databricks: Rows instance failed to retrieve result set metadata"
var errRowsResultFetchFailed = "databricks: Rows instance failed to retrieve results"

func errRowsInvalidColumnIndex(index int) string {
	return fmt.Sprintf("databricks: invalid column index: %d", index)
}

func errRowsUnandledFetchDirection(dir string) string {
	return fmt.Sprintf("databricks: unhandled fetch direction %s", dir)
}
