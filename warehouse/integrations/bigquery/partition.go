package bigquery

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"cloud.google.com/go/bigquery"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
)

var (
	errPartitionColumnNotSupported = errors.New("partition column not supported")
	errPartitionTypeNotSupported   = errors.New("partition type not supported")
)

var supportedPartitionColumnMap = map[string]struct{}{
	"_PARTITIONTIME":     {},
	"loaded_at":          {},
	"received_at":        {},
	"sent_at":            {},
	"timestamp":          {},
	"original_timestamp": {},
}

var supportedPartitionTypeMap = map[string]bigquery.TimePartitioningType{
	"hour": bigquery.HourPartitioningType,
	"day":  bigquery.DayPartitioningType,
}

// avoidPartitionDecorator returns true if custom partition is enabled via destination or global config
// if we know beforehand that the data is in a single partition, specifying the partition decorator can improve write performance.
// However, in case of time-unit column and integer-range partitioned tables, the partition ID specified in the decorator must match the data being written. Otherwise, an error occurs.
// Therefore, if we are not sure about the partition decorator, we should not specify it i.e. in case when it is enabled via destination config.
func (bq *BigQuery) avoidPartitionDecorator() bool {
	return bq.customPartitionEnabledViaGlobalConfig() || bq.isTimeUnitPartitionColumn()
}

func (bq *BigQuery) customPartitionEnabledViaGlobalConfig() bool {
	return bq.config.customPartitionsEnabled || slices.Contains(bq.config.customPartitionsEnabledWorkspaceIDs, bq.warehouse.WorkspaceID)
}

func (bq *BigQuery) isTimeUnitPartitionColumn() bool {
	partitionColumn := bq.partitionColumn()

	if partitionColumn == "" {
		return false
	}
	if err := bq.checkValidPartitionColumn(partitionColumn); err != nil {
		return false
	}
	if partitionColumn == "_PARTITIONTIME" {
		return false
	}
	return true
}

func (bq *BigQuery) partitionColumn() string {
	return bq.warehouse.GetStringDestinationConfig(bq.conf, model.PartitionColumnSetting)
}

func (bq *BigQuery) partitionType() string {
	return bq.warehouse.GetStringDestinationConfig(bq.conf, model.PartitionTypeSetting)
}

func (bq *BigQuery) checkValidPartitionColumn(partitionColumn string) error {
	_, ok := supportedPartitionColumnMap[partitionColumn]
	if !ok {
		return errPartitionColumnNotSupported
	}
	return nil
}

func (bq *BigQuery) bigqueryPartitionType(partitionType string) (bigquery.TimePartitioningType, error) {
	pt, ok := supportedPartitionTypeMap[partitionType]
	if !ok {
		return "", errPartitionTypeNotSupported
	}
	return pt, nil
}

func (bq *BigQuery) partitionDate() (string, error) {
	partitionType := bq.partitionType()
	if partitionType == "" {
		return bq.now().Format("2006-01-02"), nil
	}

	bqPartitionType, err := bq.bigqueryPartitionType(partitionType)
	if err != nil {
		return "", fmt.Errorf("bigquery partition type: %w", err)
	}

	switch bqPartitionType {
	case bigquery.HourPartitioningType:
		return bq.now().Format("2006-01-02T15"), nil
	case bigquery.DayPartitioningType:
		return bq.now().Format("2006-01-02"), nil
	default:
		return "", errPartitionTypeNotSupported
	}
}

func partitionedTable(tableName, partitionDate string) string {
	cleanedDate := strings.ReplaceAll(partitionDate, "-", "")
	cleanedDate = strings.ReplaceAll(cleanedDate, "T", "")
	return fmt.Sprintf("%s$%s", tableName, cleanedDate)
}
