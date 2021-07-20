package warehouseutils

import (
	"errors"
	"fmt"
	"time"

	"github.com/xitongsys/parquet-go/types"
)

// TODO: will this be rsDataTypetoParquetType
var rudderDataTypeToParquetDataType = map[string]map[string]string{
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
	float64Val, ok := val.(float64)
	if !ok {
		return 0, fmt.Errorf("failed to convert %v to int64", val)
	}
	return int64(float64Val), nil
}

//	bool, for JSON booleans
func getBool(val interface{}) (bool, error) {
	boolVal, ok := val.(bool)
	if !ok {
		return false, fmt.Errorf("failed to convert %v to bool", val)
	}
	return boolVal, nil
}

//	float64, for JSON numbers
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

//	string, for JSON strings
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

// ParquetLoader is used for generating parquet load files.
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
	return time.RFC3339
}

func (loader *ParquetLoader) AddColumn(columnName string, colType string, val interface{}) {
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

func (loader *ParquetLoader) AddRow(columnNames []string, row []string) {
	// TODO : implement
	// do nothing
}

func (loader *ParquetLoader) AddEmptyColumn(columnName string) {
	loader.AddColumn(columnName, "", nil)
}

func (loader *ParquetLoader) WriteToString() (string, error) {
	return "", errors.New("not implemented")
}

func (loader *ParquetLoader) Write() error {
	return loader.FileWriter.WriteRow(loader.Values)
}
