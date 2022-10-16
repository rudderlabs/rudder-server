package parquet

import (
	"errors"
	"fmt"
	"time"

	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type ReusableParquetLoader struct {
	columns  []interface{}
	colIdx   int
	destType string
	writer   warehouseutils.LoadFileWriterI
}

func (l *ReusableParquetLoader) IsLoadTimeColumn(columnName string) bool {
	return columnName == warehouseutils.ToProviderCase(l.destType, warehouseutils.UUID_TS_COLUMN)
}

func (l *ReusableParquetLoader) GetLoadTimeFomat(columnName string) string {
	return time.RFC3339
}

func (l *ReusableParquetLoader) AddColumn(colName, colType string, val interface{}) {
	var err error
	if val != nil {
		val, err = warehouseutils.GetParquetValue(val, colType)
		if err != nil {
			// TODO : decide
			// make val nil to avoid writing zero values to the parquet file
			fmt.Printf("Unable to get value for column: %s, err: %s\n", colName, err)
			val = nil
		}
	}

	l.columns[l.colIdx] = val
	l.colIdx++
}

func (l *ReusableParquetLoader) AddEmptyColumn(colName string) {
	l.AddColumn(colName, "", nil)
}

func (l *ReusableParquetLoader) AddRow(columnNames, row []string) {
	// TODO : implement
}

func (l *ReusableParquetLoader) WriteToString() (string, error) {
	return "", errors.New("not implemented")
}

func (l *ReusableParquetLoader) Write() error {
	return l.writer.WriteRow(l.columns)
}

func (l *ReusableParquetLoader) Reset() {
	for id := range l.columns {
		l.columns[id] = nil
	}
	l.colIdx = 0
}

func NewReusableParquetLoader(destType string, writer warehouseutils.LoadFileWriterI, count int) warehouseutils.EventLoader {
	return &ReusableParquetLoader{
		writer:   writer,
		destType: destType,
		columns:  make([]interface{}, count),
		colIdx:   0,
	}
}
