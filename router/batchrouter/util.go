package batchrouter

import (
	"context"
	stdjson "encoding/json"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/logger"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	asynccommon "github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func IsObjectStorageDestination(destType string) bool {
	return slices.Contains(objectStoreDestinations, destType)
}

func IsWarehouseDestination(destType string) bool {
	return slices.Contains(warehouseutils.WarehouseDestinations, destType)
}

func IsBatchRouterDestination(destination string) bool {
	return IsObjectStorageDestination(destination) ||
		IsWarehouseDestination(destination) ||
		asynccommon.IsAsyncDestination(destination)
}

func connectionIdentifier(batchDestination Connection) string {
	return fmt.Sprintf(`source:%s::destination:%s`, batchDestination.Source.ID, batchDestination.Destination.ID)
}

func getNamespace(config interface{}, source backendconfig.SourceT, destType string) string {
	configMap := config.(map[string]interface{})
	var namespace string
	if destType == "CLICKHOUSE" {
		// TODO: Handle if configMap["database"] is nil
		return configMap["database"].(string)
	}
	if configMap["namespace"] != nil {
		namespace = configMap["namespace"].(string)
		if len(strings.TrimSpace(namespace)) > 0 {
			return warehouseutils.ToProviderCase(destType, warehouseutils.ToSafeNamespace(destType, namespace))
		}
	}
	return warehouseutils.ToProviderCase(destType, warehouseutils.ToSafeNamespace(destType, source.Name))
}

func warehouseConnectionIdentifier(destType, connIdentifier string, source backendconfig.SourceT, destination backendconfig.DestinationT) string {
	namespace := getNamespace(destination.Config, source, destType)
	return fmt.Sprintf(`namespace:%s::%s`, namespace, connIdentifier)
}

func getBRTErrorCode(state string) int {
	if state == jobsdb.Succeeded.State {
		return 200
	}

	return 500
}

func getBatchRouterConfigInt(key, destType string, defaultValue int) int {
	destOverrideFound := config.IsSet("BatchRouter." + destType + "." + key)
	if destOverrideFound {
		return config.GetInt("BatchRouter."+destType+"."+key, defaultValue)
	}
	return config.GetInt("BatchRouter."+key, defaultValue)
}

type storageDateFormatProvider struct {
	dateFormatsCacheMu sync.RWMutex
	dateFormatsCache   map[string]string // (sourceId:destinationId) -> dateFormat
}

func (sdfp *storageDateFormatProvider) GetFormat(log logger.Logger, manager filemanager.FileManager, destination *Connection, folderName string) (dateFormat string, err error) {
	connIdentifier := connectionIdentifier(Connection{Destination: destination.Destination, Source: destination.Source})
	sdfp.dateFormatsCacheMu.RLock()
	format, exists := sdfp.dateFormatsCache[connIdentifier]
	sdfp.dateFormatsCacheMu.RUnlock()
	if exists {
		return format, err
	}

	defer func() {
		if err == nil {
			sdfp.dateFormatsCacheMu.Lock()
			sdfp.dateFormatsCache[connIdentifier] = dateFormat
			sdfp.dateFormatsCacheMu.Unlock()
		}
	}()

	dateFormat = "YYYY-MM-DD"
	prefixes := []string{folderName, destination.Source.ID}
	prefix := strings.Join(prefixes[0:2], "/")

	getFullPrefix := func(manager filemanager.FileManager, prefix string) (fullPrefix string) {
		fullPrefix = prefix
		configPrefix := manager.Prefix()

		if configPrefix != "" {
			if configPrefix[len(configPrefix)-1:] == "/" {
				fullPrefix = configPrefix + prefix
			} else {
				fullPrefix = configPrefix + "/" + prefix
			}
		}
		return
	}
	fullPrefix := getFullPrefix(manager, prefix)
	fileObjects, err := manager.ListFilesWithPrefix(context.TODO(), "", fullPrefix, 5).Next()
	if err != nil {
		log.Errorf("[BRT]: Failed to fetch fileObjects with connIdentifier: %s, prefix: %s, Err: %v", connIdentifier, fullPrefix, err)
		// Returning the earlier default as we might not able to fetch the list.
		// because "*:GetObject" and "*:ListBucket" permissions are not available.
		dateFormat = "MM-DD-YYYY"
		return
	}
	if len(fileObjects) == 0 {
		return
	}

	for idx := range fileObjects {
		if fileObjects[idx] == nil {
			log.Errorf("[BRT]: nil occurred in file objects for '%T' filemanager of destination ID : %s", manager, destination.Destination.ID)
			continue
		}
		key := fileObjects[idx].Key
		replacedKey := strings.Replace(key, fullPrefix, "", 1)
		splittedKeys := strings.Split(replacedKey, "/")
		if len(splittedKeys) > 1 {
			date := splittedKeys[1]
			for layout, format := range dateFormatLayouts {
				_, err = time.Parse(layout, date)
				if err == nil {
					dateFormat = format
					return
				}
			}
		}
	}
	return
}

func IsAsyncDestinationLimitNotReached(brt *Handle, destinationID string) bool {
	asyncDest := brt.asyncDestinationStruct[destinationID]
	isSFTP := asynccommon.IsSFTPDestination(brt.destType)
	maxPayloadSizeReached := asyncDest.Size < brt.maxPayloadSizeInBytes
	maxEventsReached := asyncDest.Count < brt.maxEventsInABatch
	uploadNotInProgress := !asyncDest.UploadInProgress
	return (isSFTP && maxPayloadSizeReached && uploadNotInProgress) ||
		(maxEventsReached && maxPayloadSizeReached && uploadNotInProgress)
}

func getFirstSourceJobRunID(params map[int64]stdjson.RawMessage) string {
	for key := range params {
		return gjson.GetBytes(params[key], "source_job_run_id").String()
	}
	return ""
}
