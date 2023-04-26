package batchrouter

import jsoniter "github.com/json-iterator/go"

var (
	json                                      = jsoniter.ConfigCompatibleWithStandardLibrary
	objectStoreDestinations []string          = []string{"S3", "GCS", "AZURE_BLOB", "MINIO", "DIGITAL_OCEAN_SPACES"}
	asyncDestinations       []string          = []string{"MARKETO_BULK_UPLOAD"}
	dateFormatLayouts       map[string]string = map[string]string{
		"01-02-2006": "MM-DD-YYYY",
		"2006-01-02": "YYYY-MM-DD",
		//"02-01-2006" : "DD-MM-YYYY", //adding this might match with that of MM-DD-YYYY too
	}
)
