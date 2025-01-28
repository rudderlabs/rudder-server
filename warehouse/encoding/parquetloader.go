package encoding

import (
	"errors"
	"fmt"
	"time"

	"github.com/xitongsys/parquet-go/types"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

// parquetLoader is used for generating parquet load files.
type parquetLoader struct {
	destType string
	Schema   []string
	Values   []interface{}
	writer   LoadFileWriter
}

func newParquetLoader(w LoadFileWriter, destType string) *parquetLoader {
	return &parquetLoader{
		destType: destType,
		writer:   w,
	}
}

func (loader *parquetLoader) IsLoadTimeColumn(columnName string) bool {
	return columnName == warehouseutils.ToProviderCase(loader.destType, UUIDTsColumn)
}

func (*parquetLoader) GetLoadTimeFormat(_ string) string {
	return time.RFC3339
}

func (loader *parquetLoader) AddColumn(columnName, colType string, val interface{}) {
	var err error

	if val != nil {
		if val, err = parquetValue(val, colType); err != nil {
			fmt.Println("add col err", columnName, "", err)
			val = nil
		}
	}
	loader.Values = append(loader.Values, val)
}

func (*parquetLoader) AddRow(_, _ []string) {
	// TODO : implement
}

func (loader *parquetLoader) AddEmptyColumn(columnName string) {
	loader.AddColumn(columnName, "", nil)
}

func (*parquetLoader) WriteToString() (string, error) {
	return "", errors.New("not implemented")
}

func (loader *parquetLoader) Write() error {
	return loader.writer.WriteRow(loader.Values)
}

func parquetValue(val interface{}, colType string) (interface{}, error) {
	switch colType {
	case model.BigIntDataType, model.IntDataType:
		return getInt64(val)
	case model.BooleanDataType:
		return getBool(val)
	case model.FloatDataType:
		return getFloat64(val)
	case model.DateTimeDataType:
		return getUnixTimestamp(val)
	case model.StringDataType, model.TextDataType:
		return getString(val)
	}
	return nil, fmt.Errorf("unsupported type for parquet: %s", colType)
}

func getInt64(val interface{}) (int64, error) {
	if intVal, ok := val.(int); !ok {
		return 0, errors.New("failed to convert to int64")
	} else {
		return int64(intVal), nil
	}
}

func getBool(val interface{}) (bool, error) {
	if boolVal, ok := val.(bool); !ok {
		return false, errors.New("failed to convert to bool")
	} else {
		return boolVal, nil
	}
}

func getFloat64(val interface{}) (float64, error) {
	if float64Val, ok := val.(float64); !ok {
		return 0, errors.New("failed to convert to float64")
	} else {
		return float64Val, nil
	}
}

func getUnixTimestamp(val interface{}) (int64, error) {
	tsString, ok := val.(string)
	if !ok {
		return 0, errors.New("not a valid timestamp string")
	}

	parsedTS, err := time.Parse(time.RFC3339, tsString)
	if err != nil {
		return 0, err
	}

	return types.TimeToTIMESTAMP_MICROS(parsedTS, false), nil
}

func getString(val interface{}) (string, error) {
	if stringVal, ok := val.(string); !ok {
		return "", errors.New("failed to convert to string")
	} else {
		return stringVal, nil
	}
}
