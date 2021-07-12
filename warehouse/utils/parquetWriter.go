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

func getSortedTableColumns(schema TableSchemaT) []string {
	sortedColumns := []string{}
	for col := range schema {
		sortedColumns = append(sortedColumns, col)
	}
	sort.Strings(sortedColumns)
	return sortedColumns
}

func getParquetSchema(schema TableSchemaT, destType string) ([]string, error) {
	fmt.Println(destType)
	whTypeMap, ok := whDataTypeToParquetDataType[destType]
	if !ok {
		return nil, errors.New("unsupported warehouse")
	}
	pSchema := []string{}
	for _, col := range getSortedTableColumns(schema) {
		fmt.Println(col)
		pType := fmt.Sprintf("name=%s, %s", ToProviderCase(destType, col), whTypeMap[schema[col]])
		pSchema = append(pSchema, pType)
	}
	return pSchema, nil
}

type ParquetWriter struct {
	ParquetWriter *writer.CSVWriter
	FileWriter    misc.GZipWriter
	Schema        []string
}

func CreateParquetWriter(schema TableSchemaT, fileWriter misc.GZipWriter, destType string) (*ParquetWriter, error) {
	fmt.Println("schema", schema)
	pSchema, err := getParquetSchema(schema, destType)
	if err != nil {
		return nil, err
	}
	fmt.Println("psCheam:", pSchema)
	w, err := writer.NewCSVWriterFromWriter(pSchema, fileWriter, 4)
	if err != nil {
		return nil, err
	}
	return &ParquetWriter{
		ParquetWriter: w,
		Schema:        pSchema,
		FileWriter:    fileWriter,
	}, nil
}

func (p *ParquetWriter) WriteRow(row []interface{}) error {
	return p.ParquetWriter.Write(row)
}

func (p *ParquetWriter) Close() error {
	err := p.ParquetWriter.WriteStop()
	if err != nil {
		fmt.Println("Write stop err", err)
		return err
	}
	// close the gzWriter also
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
