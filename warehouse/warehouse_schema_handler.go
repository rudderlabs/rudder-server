package warehouse

import (
	"fmt"

	"github.com/rudderlabs/rudder-server/utils/misc"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func handleSchemaChange(existingDataType string, columnType string, columnVal interface{}) (newColumnVal interface{}, ok bool) {
	if existingDataType == "string" || existingDataType == "text" {
		newColumnVal = fmt.Sprintf("%v", columnVal)
	} else if (columnType == "int" || columnType == "bigint") && existingDataType == "float" {
		newColumnVal = columnVal
	} else if columnType == "float" && (existingDataType == "int" || existingDataType == "bigint") {
		floatVal, ok := columnVal.(float64)
		if !ok {
			newColumnVal = nil
		} else {
			newColumnVal = int(floatVal)
		}
	} else {
		return nil, false
	}

	return newColumnVal, true
}

func (jobRun *JobRunT) handleDiscardTypes(tableName string, columnName string, columnVal interface{}, columnData DataT, gzWriter misc.GZipWriter) error {
	job := jobRun.job
	rowID, hasID := columnData[job.getColumnName("id")]
	receivedAt, hasReceivedAt := columnData[job.getColumnName("received_at")]
	if hasID && hasReceivedAt {
		eventLoader := warehouseutils.GetNewEventLoader(job.DestinationType)
		eventLoader.AddColumn("column_name", columnName)
		eventLoader.AddColumn("column_value", fmt.Sprintf("%v", columnVal))
		eventLoader.AddColumn("received_at", receivedAt)
		eventLoader.AddColumn("row_id", rowID)
		eventLoader.AddColumn("table_name", tableName)
		if eventLoader.IsLoadTimeColumn("uuid_ts") {
			timestampFormat := eventLoader.GetLoadTimeFomat("uuid_ts")
			eventLoader.AddColumn("uuid_ts", jobRun.uuidTS.Format(timestampFormat))
		}
		if eventLoader.IsLoadTimeColumn("loaded_at") {
			timestampFormat := eventLoader.GetLoadTimeFomat("loaded_at")
			eventLoader.AddColumn("loaded_at", jobRun.uuidTS.Format(timestampFormat))
		}

		eventData, err := eventLoader.WriteToString()
		if err != nil {
			return err
		}
		gzWriter.WriteGZ(eventData)
	}
	return nil
}
