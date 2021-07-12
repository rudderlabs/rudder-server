package warehouseutils

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/rudderlabs/rudder-server/utils/misc"
)

var whDataTypeToParquetDataType = map[string]map[string]string{
	"RS": {
		"bigint":   PARQUET_INT_64,
		"int":      PARQUET_INT_64,
		"boolean":  PARQUET_BOOLEAN,
		"float":    PARQUET_DOUBLE,
		"string":   PARQUET_STRING,
		"text":     PARQUET_STRING,
		"datetime": PARQUET_TIMESTAMP_MICROS,
	},
}

func getInt64(val interface{}) (int64, error) {
	switch val.(type) {
	case int:
		return int64(val.(int)), nil
	case int32:
		return int64(val.(int32)), nil
	case int64:
		return val.(int64), nil
	case float32:
		return int64(val.(float32)), nil
	case float64:
		return int64(val.(float64)), nil
	case string:
		return strconv.ParseInt(val.(string), 10, 64)
	default:
		return 0, fmt.Errorf("failed to convert %v to int64", val)
	}
}

func getBool(val interface{}) (bool, error) {
	switch val.(type) {
	case bool:
		return val.(bool), nil
	case string:
		return strconv.ParseBool(val.(string))
	default:
		return false, fmt.Errorf("failed to convert %v to bool", val)
	}
}

func getFloat64(val interface{}) (float64, error) {
	switch val.(type) {
	case float32:
		return float64(val.(float32)), nil
	case float64:
		return val.(float64), nil
	case string:
		return strconv.ParseFloat(val.(string), 64)
	default:
		return 0, fmt.Errorf("failed to convert %v to float64", val)
	}
}

func getUnixTimestamp(val interface{}) (int64, error) {
	switch val.(type) {
	case time.Time:
		return val.(time.Time).UnixNano() / int64(time.Millisecond), nil
	case string:
		// TODO: see what timestamps can be parsed accd to supported types
		parsedTS, err := time.Parse(time.RFC3339, val.(string))
		if err != nil {
			return 0, err
		}
		return parsedTS.UnixNano() / int64(time.Millisecond), nil
	default:
		return 0, fmt.Errorf("failed to convert %v to unix timestamp", val)
	}

}

func getString(val interface{}) (string, error) {
	switch val.(type) {
	case string:
		return val.(string), nil
	default:
		return fmt.Sprintf("%v", val), nil
	}
}

func GetParquetValue(val interface{}, colType string) (retVal interface{}, err error) {
	switch colType {
	case "bigint", "int":
		// string, int, float
		retVal, err = getInt64(val)
		return
	case "boolean":
		// string,bool
		retVal, err = getBool(val)
		return
	case "float":
		// string, float
		retVal, err = getFloat64(val)
		return
	case "datetime":
		// parse into timestamp -> convert to parquet
		retVal, err = getUnixTimestamp(val)
		return
	case "string", "text":
		retVal, err = getString(val)
		return
	}
	return nil, fmt.Errorf("unsupported type for parquet: %s", colType)
}

// ParquetLoader is common for non-BQ warehouses.
// If you need any custom logic, either extend this or use destType and if/else/switch.
type ParquetLoader struct {
	destType   string
	Schema     []string
	Values     []interface{}
	FileWriter LoadFileWriterI
}

func NewParquetLoader(destType string, w LoadFileWriterI) *ParquetLoader {
	loader := &ParquetLoader{
		destType:   destType,
		FileWriter: w,
	}
	return loader
}

func (loader *ParquetLoader) IsLoadTimeColumn(columnName string) bool {
	return columnName == ToProviderCase(loader.destType, UUID_TS_COLUMN)
}

func (loader *ParquetLoader) GetLoadTimeFomat(columnName string) string {
	return misc.RFC3339Milli
}

func (loader *ParquetLoader) AddColumn(columnName string, colType string, val interface{}) {
	var err error
	if val != nil {
		val, err = GetParquetValue(val, colType)
		if err != nil {
			// TODO : decide
		}
	}
	loader.Values = append(loader.Values, val)
}

func (loader *ParquetLoader) AddRow(columnNames []string, row []string) {
	// do nothing
}

func (loader *ParquetLoader) AddEmptyColumn(columnName string) {
	loader.AddColumn(columnName, "", nil)
}

func (loader *ParquetLoader) WriteToString() (string, error) {
	return "", errors.New("not implemented")
}

func (loader *ParquetLoader) Write() error {
	fmt.Println("Writing to file writer", loader.Values)
	return loader.FileWriter.WriteRow(loader.Values)
}
