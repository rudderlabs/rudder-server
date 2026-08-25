package clickhouse

import (
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
)

var errEmptyJSONCell = errors.New("empty json cell")

// bindValue converts one raw CSV cell into the exact Go type the column needs.
//
// Exact types matter: the driver accepts loosely-typed values and then
// silently corrupts them — int(300) into a UInt8 column stores 44, float64(1.9)
// into Int64 stores 1 — all without an error.
//
// On a cell that fails to parse the value resolves to nil for a nullable
// column, or to the type's typed default when nullable columns are disabled.
func (ch *ClickhouseV2) bindValue(data, dataType string) any {
	var (
		value any
		err   error
	)

	switch dataType {
	case model.IntDataType:
		value, err = strconv.ParseInt(data, 10, 64)
	case model.FloatDataType:
		value, err = strconv.ParseFloat(data, 64)
	case model.DateTimeDataType:
		value, err = time.Parse(time.RFC3339, data)
	case model.JSONDataType:
		// The driver serialises a JSON column from the string as written, so the
		// payload goes through untouched. An empty cell is the one value that
		// cannot: it is not valid JSON. Treating it as a parse failure resolves
		// it through the same fallback every other type uses.
		value = data
		if strings.TrimSpace(data) == "" {
			err = errEmptyJSONCell
		}
	case model.BooleanDataType:
		var b bool
		if b, err = strconv.ParseBool(data); b {
			value = uint8(1)
		} else {
			value = uint8(0)
		}
	default:
		if strings.Contains(dataType, "array") {
			return ch.castStringToArray(data, dataType)
		}
		return data
	}

	if err != nil {
		if ch.config.disableNullable {
			return datatypeDefaultValuesMap[dataType]
		}
		return nil
	}
	return value
}

// castStringToArray unmarshals a JSON array cell into the typed slice the
// column expects. Never returns a nil slice: the driver rejects nil for array
// columns.
func (ch *ClickhouseV2) castStringToArray(data, dataType string) any {
	switch dataType {
	case "array(int)":
		dataInt := make([]int64, 0)
		if err := jsonrs.Unmarshal([]byte(data), &dataInt); err != nil {
			ch.logger.Errorn("Error while unmarshalling data into array of int", obskit.Error(err))
		}
		return dataInt
	case "array(float)":
		dataFloat := make([]float64, 0)
		if err := jsonrs.Unmarshal([]byte(data), &dataFloat); err != nil {
			ch.logger.Errorn("Error while unmarshalling data into array of float", obskit.Error(err))
		}
		return dataFloat
	case "array(string)":
		dataInterface := make([]any, 0)
		if err := jsonrs.Unmarshal([]byte(data), &dataInterface); err != nil {
			ch.logger.Errorn("Error while unmarshalling data into array of interface", obskit.Error(err))
		}
		dataString := make([]string, 0, len(dataInterface))
		for _, value := range dataInterface {
			if strValue, ok := value.(string); ok {
				dataString = append(dataString, strValue)
				continue
			}
			marshalled, _ := jsonrs.Marshal(value)
			dataString = append(dataString, string(marshalled))
		}
		return dataString
	case "array(datetime)":
		dataTime := make([]time.Time, 0)
		if err := jsonrs.Unmarshal([]byte(data), &dataTime); err != nil {
			ch.logger.Errorn("Error while unmarshalling data into array of date time", obskit.Error(err))
		}
		return dataTime
	case "array(boolean)":
		// Booleans are written as 1/0 by the warehouse slave, so unmarshal into
		// []int32 first and fall back to []bool.
		dataInt := make([]int32, 0)
		dataBool := make([]bool, 0)

		if err := jsonrs.Unmarshal([]byte(data), &dataInt); err == nil {
			for _, value := range dataInt {
				dataBool = append(dataBool, value != 0)
			}
			return dataBool
		}
		if err := jsonrs.Unmarshal([]byte(data), &dataBool); err != nil {
			ch.logger.Errorn("Error while unmarshalling data into array of bool", obskit.Error(err))
			return dataBool
		}
		return dataBool
	}
	return data
}
