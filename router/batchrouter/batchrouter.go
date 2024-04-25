package batchrouter

import (
	jsoniter "github.com/json-iterator/go"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
)

var (
	json                    = jsoniter.ConfigCompatibleWithStandardLibrary
	objectStoreDestinations = []string{"S3", "GCS", "AZURE_BLOB", "MINIO", "DIGITAL_OCEAN_SPACES"}
	asyncDestinations       = common.AsyncDestinations
	dateFormatLayouts       = map[string]string{
		"01-02-2006": "MM-DD-YYYY",
		"2006-01-02": "YYYY-MM-DD",
		//"02-01-2006" : "DD-MM-YYYY", //adding this might match with that of MM-DD-YYYY too
	}
)
