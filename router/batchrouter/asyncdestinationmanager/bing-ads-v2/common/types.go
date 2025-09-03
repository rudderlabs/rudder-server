package common

import (
	"encoding/csv"
)

// ActionFileInfo contains information about action files for bulk uploads
type ActionFileInfo struct {
	Action           string
	CSVWriter        *csv.Writer
	CSVFilePath      string
	ZipFilePath      string
	SuccessfulJobIDs []int64
	FailedJobIDs     []int64
	FileSize         int64
	EventCount       int64
}

// BaseBingAdsBulkUploader contains common fields for Bing Ads bulk uploaders
type BaseBingAdsBulkUploader struct {
	DestName      string
	Service       interface{} // Will be typed in specific implementations
	Logger        interface{} // Will be typed in specific implementations
	StatsFactory  interface{} // Will be typed in specific implementations
	FileSizeLimit int64
	EventsLimit   int64
}

// Common constants
const (
	CommaSeparator = ","
)
