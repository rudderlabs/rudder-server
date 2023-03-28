package encoding

import (
	"errors"
	"fmt"
	"time"

	"github.com/rudderlabs/rudder-server/warehouse/utils"

	"github.com/xitongsys/parquet-go/types"
)

// ParquetLoader is used for generating parquet load files.
type ParquetLoader struct {
	destType   string
	Schema     []string
	Values     []interface{}
	FileWriter LoadFileWriter
}

func NewParquetLoader(destType string, w LoadFileWriter) *ParquetLoader {
	loader := &ParquetLoader{
		destType:   destType,
		FileWriter: w,
	}
	return loader
}

func (loader *ParquetLoader) IsLoadTimeColumn(columnName string) bool {
	return columnName == warehouseutils.ToProviderCase(loader.destType, UUIDTsColumn)
}

func (*ParquetLoader) GetLoadTimeFormat(_ string) string {
	return time.RFC3339
}

func (loader *ParquetLoader) AddColumn(columnName, colType string, val interface{}) {
	var err error
	if val != nil {
		val, err = GetParquetValue(val, colType)
		if err != nil {
			// TODO : decide
			// make val nil to avoid writing zero values to the parquet file
			fmt.Println("add col err", columnName, "", err)
			val = nil
		}
	}
	loader.Values = append(loader.Values, val)
}

func (*ParquetLoader) AddRow(_, _ []string) {
	// TODO : implement
}

func (loader *ParquetLoader) AddEmptyColumn(columnName string) {
	loader.AddColumn(columnName, "", nil)
}

func (*ParquetLoader) WriteToString() (string, error) {
	return "", errors.New("not implemented")
}

func (loader *ParquetLoader) Write() error {
	return loader.FileWriter.WriteRow(loader.Values)
}

func getInt64(val interface{}) (int64, error) {
	intVal, ok := val.(int)
	if !ok {
		return 0, fmt.Errorf("failed to convert %v to int64", val)
	}
	return int64(intVal), nil
}

func getBool(val interface{}) (bool, error) {
	boolVal, ok := val.(bool)
	if !ok {
		return false, fmt.Errorf("failed to convert %v to bool", val)
	}
	return boolVal, nil
}

func getFloat64(val interface{}) (float64, error) {
	float64Val, ok := val.(float64)
	if !ok {
		return 0, fmt.Errorf("failed to convert %v to float64", val)
	}
	return float64Val, nil
}

func getUnixTimestamp(val interface{}) (int64, error) {
	tsString, ok := val.(string)
	if !ok {
		return 0, fmt.Errorf("%v is not a valid timestamp string", val)
	}
	parsedTS, err := time.Parse(time.RFC3339, tsString)
	if err != nil {
		return 0, err
	}
	return types.TimeToTIMESTAMP_MICROS(parsedTS, false), nil
}

func getString(val interface{}) (string, error) {
	stringVal, ok := val.(string)
	if !ok {
		return "", fmt.Errorf("failed to convert %v to string", val)
	}
	return stringVal, nil
}

func GetParquetValue(val interface{}, colType string) (retVal interface{}, err error) {
	switch colType {
	case "bigint", "int":
		retVal, err = getInt64(val)
		return
	case "boolean":
		retVal, err = getBool(val)
		return
	case "float":
		retVal, err = getFloat64(val)
		return
	case "datetime":
		retVal, err = getUnixTimestamp(val)
		return
	case "string", "text":
		retVal, err = getString(val)
		return
	}
	return nil, fmt.Errorf("unsupported type for parquet: %s", colType)
}
