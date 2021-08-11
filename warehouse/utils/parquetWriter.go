package warehouseutils

import (
	"errors"
	"fmt"
	"os"
	"sort"

	"github.com/rudderlabs/rudder-server/config"
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
	"S3_DATALAKE": {
		"bigint":   PARQUET_INT_64,
		"int":      PARQUET_INT_64,
		"boolean":  PARQUET_BOOLEAN,
		"float":    PARQUET_DOUBLE,
		"string":   PARQUET_STRING,
		"text":     PARQUET_STRING,
		"datetime": PARQUET_TIMESTAMP_MICROS,
	},
}

type ParquetWriter struct {
	ParquetWriter *writer.CSVWriter
	FileWriter    misc.BufferedWriter
	Schema        []string
}

func CreateParquetWriter(schema TableSchemaT, outputFilePath string, destType string) (*ParquetWriter, error) {
	bufWriter, err := misc.CreateBufferedWriter(outputFilePath)
	if err != nil {
		return nil, err
	}

	pSchema, err := getParquetSchema(schema, destType)
	if err != nil {
		return nil, err
	}
	parallelReaderWriters := config.GetEnvAsInt("WH_PARQUET_PARALLEL_READER_WRITERS", 4)
	w, err := writer.NewCSVWriterFromWriter(pSchema, bufWriter, int64(parallelReaderWriters))
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

func getSortedTableColumns(schema TableSchemaT) []string {
	sortedColumns := []string{}
	for col := range schema {
		sortedColumns = append(sortedColumns, col)
	}
	sort.Strings(sortedColumns)
	return sortedColumns
}

func getParquetSchema(schema TableSchemaT, destType string) ([]string, error) {
	whTypeMap, ok := rudderDataTypeToParquetDataType[destType]
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
