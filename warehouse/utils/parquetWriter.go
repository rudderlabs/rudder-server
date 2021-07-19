package warehouseutils

import (
	"errors"
	"fmt"
	"os"
	"sort"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/xitongsys/parquet-go/writer"
)

const (
	PARQUET_INT_64           = "type=INT64, repetitiontype=OPTIONAL"
	PARQUET_BOOLEAN          = "type=BOOLEAN, repetitiontype=OPTIONAL"
	PARQUET_DOUBLE           = "type=DOUBLE, repetitiontype=OPTIONAL"
	PARQUET_STRING           = "type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL"
	PARQUET_TIMESTAMP_MICROS = "type=INT64, convertedtype=TIMESTAMP_MICROS, repetitiontype=OPTIONAL"
)

// TODO: move to warehouse utils
func getSortedTableColumns(schema TableSchemaT) []string {
	sortedColumns := []string{}
	for col := range schema {
		sortedColumns = append(sortedColumns, col)
	}
	sort.Strings(sortedColumns)
	return sortedColumns
}

func getParquetSchema(schema TableSchemaT, destType string) ([]string, error) {
	whTypeMap, ok := whDataTypeToParquetDataType[destType]
	if !ok {
		return nil, errors.New("unsupported warehouse for parquet load files")
	}
	pSchema := []string{}
	for _, col := range getSortedTableColumns(schema) {
		pType := fmt.Sprintf("name=%s, %s", ToProviderCase(destType, col), whTypeMap[schema[col]])
		pSchema = append(pSchema, pType)
	}
	return pSchema, nil
}

type ParquetWriter struct {
	ParquetWriter *writer.CSVWriter
	FileWriter    misc.BufferedWriter
	Schema        []string
}

// TODOD: should we move parquet writer to misc ?
func CreateParquetWriter(schema TableSchemaT, outputFilePath string, destType string) (*ParquetWriter, error) {
	bufWriter, err := misc.CreateBufferedWriter(outputFilePath)
	if err != nil {
		return nil, err
	}

	pSchema, err := getParquetSchema(schema, destType)
	if err != nil {
		return nil, err
	}
	w, err := writer.NewCSVWriterFromWriter(pSchema, bufWriter, 4)
	if err != nil {
		return nil, err
	}
	return &ParquetWriter{
		ParquetWriter: w,
		Schema:        pSchema,
		FileWriter:    bufWriter,
	}, nil
}

func (p *ParquetWriter) WriteRow(row []interface{}) error {
	return p.ParquetWriter.Write(row)
}

func (p *ParquetWriter) Close() error {
	err := p.ParquetWriter.WriteStop()
	if err != nil {
		return err
	}
	// close the bufWriter
	return p.FileWriter.Close()
}

func (p *ParquetWriter) WriteGZ(s string) error {
	return errors.New("not implemented")
}

func (p *ParquetWriter) Write(b []byte) (int, error) {
	return 0, errors.New("not implemented")
}

func (p *ParquetWriter) GetLoadFile() *os.File {
	return p.FileWriter.GetLoadFile()
}
