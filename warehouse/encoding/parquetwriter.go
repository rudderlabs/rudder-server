package encoding

import (
	"errors"
	"fmt"
	"os"
	"sort"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"

	"github.com/rudderlabs/rudder-server/warehouse/utils"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/xitongsys/parquet-go/writer"
)

const (
	ParquetInt64           = "type=INT64, repetitiontype=OPTIONAL"
	ParquetBoolean         = "type=BOOLEAN, repetitiontype=OPTIONAL"
	ParquetDouble          = "type=DOUBLE, repetitiontype=OPTIONAL"
	ParquetString          = "type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL"
	ParquetTimestampMicros = "type=INT64, convertedtype=TIMESTAMP_MICROS, repetitiontype=OPTIONAL"
)

var rudderDataTypeToParquetDataType = map[string]map[string]string{
	warehouseutils.RS: {
		"bigint":   ParquetInt64,
		"int":      ParquetInt64,
		"boolean":  ParquetBoolean,
		"float":    ParquetDouble,
		"string":   ParquetString,
		"text":     ParquetString,
		"datetime": ParquetTimestampMicros,
	},
	warehouseutils.S3_DATALAKE: {
		"bigint":   ParquetInt64,
		"int":      ParquetInt64,
		"boolean":  ParquetBoolean,
		"float":    ParquetDouble,
		"string":   ParquetString,
		"text":     ParquetString,
		"datetime": ParquetTimestampMicros,
	},
	warehouseutils.GCS_DATALAKE: {
		"int":      ParquetInt64,
		"boolean":  ParquetBoolean,
		"float":    ParquetDouble,
		"string":   ParquetString,
		"datetime": ParquetTimestampMicros,
	},
	warehouseutils.AZURE_DATALAKE: {
		"int":      ParquetInt64,
		"boolean":  ParquetBoolean,
		"float":    ParquetDouble,
		"string":   ParquetString,
		"datetime": ParquetTimestampMicros,
	},
}

type ParquetWriter struct {
	writer     *writer.CSVWriter
	fileWriter misc.BufferedWriter
	schema     []string
}

func CreateParquetWriter(schema model.TableSchema, outputFilePath, destType string) (*ParquetWriter, error) {
	bufWriter, err := misc.CreateBufferedWriter(outputFilePath)
	if err != nil {
		return nil, err
	}

	pSchema, err := getParquetSchema(schema, destType)
	if err != nil {
		return nil, err
	}
	w, err := writer.NewCSVWriterFromWriter(pSchema, bufWriter, parquetParallelWriters)
	if err != nil {
		return nil, err
	}
	return &ParquetWriter{
		writer:     w,
		schema:     pSchema,
		fileWriter: bufWriter,
	}, nil
}

func (p *ParquetWriter) WriteRow(row []interface{}) error {
	return p.writer.Write(row)
}

func (p *ParquetWriter) Close() error {
	err := p.writer.WriteStop()
	if err != nil {
		return err
	}
	// close the bufWriter
	return p.fileWriter.Close()
}

func (*ParquetWriter) WriteGZ(_ string) error {
	return errors.New("not implemented")
}

func (*ParquetWriter) Write(_ []byte) (int, error) {
	return 0, errors.New("not implemented")
}

func (p *ParquetWriter) GetLoadFile() *os.File {
	return p.fileWriter.GetFile()
}

func getSortedTableColumns(schema model.TableSchema) []string {
	var sortedColumns []string
	for col := range schema {
		sortedColumns = append(sortedColumns, col)
	}
	sort.Strings(sortedColumns)
	return sortedColumns
}

func getParquetSchema(schema model.TableSchema, destType string) ([]string, error) {
	whTypeMap, ok := rudderDataTypeToParquetDataType[destType]
	if !ok {
		return nil, errors.New("unsupported warehouse for parquet load files")
	}
	var pSchema []string
	for _, col := range getSortedTableColumns(schema) {
		pType := fmt.Sprintf("name=%s, %s", warehouseutils.ToProviderCase(destType, col), whTypeMap[schema[col]])
		pSchema = append(pSchema, pType)
	}
	return pSchema, nil
}
