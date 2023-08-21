package encoding

import (
	"errors"
	"fmt"
	"os"
	"sort"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"

	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"

	"github.com/xitongsys/parquet-go/writer"

	"github.com/rudderlabs/rudder-server/utils/misc"
)

const (
	parquetInt64           = "type=INT64, repetitiontype=OPTIONAL"
	parquetBoolean         = "type=BOOLEAN, repetitiontype=OPTIONAL"
	parquetDouble          = "type=DOUBLE, repetitiontype=OPTIONAL"
	parquetString          = "type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL"
	parquetTimestampMicros = "type=INT64, convertedtype=TIMESTAMP_MICROS, repetitiontype=OPTIONAL"
)

var rudderDataTypeToParquetDataType = map[string]map[string]string{
	warehouseutils.RS: {
		"bigint":   parquetInt64,
		"int":      parquetInt64,
		"boolean":  parquetBoolean,
		"float":    parquetDouble,
		"string":   parquetString,
		"text":     parquetString,
		"datetime": parquetTimestampMicros,
	},
	warehouseutils.S3Datalake: {
		"bigint":   parquetInt64,
		"int":      parquetInt64,
		"boolean":  parquetBoolean,
		"float":    parquetDouble,
		"string":   parquetString,
		"text":     parquetString,
		"datetime": parquetTimestampMicros,
	},
	warehouseutils.GCSDatalake: {
		"int":      parquetInt64,
		"boolean":  parquetBoolean,
		"float":    parquetDouble,
		"string":   parquetString,
		"datetime": parquetTimestampMicros,
	},
	warehouseutils.AzureDatalake: {
		"int":      parquetInt64,
		"boolean":  parquetBoolean,
		"float":    parquetDouble,
		"string":   parquetString,
		"datetime": parquetTimestampMicros,
	},
	warehouseutils.DELTALAKE: {
		"int":      parquetInt64,
		"boolean":  parquetBoolean,
		"float":    parquetDouble,
		"string":   parquetString,
		"datetime": parquetTimestampMicros,
	},
}

type parquetWriter struct {
	writer     *writer.CSVWriter
	fileWriter misc.BufferedWriter
	schema     []string
}

func createParquetWriter(outputFilePath string, schema model.TableSchema, destType string, maxParallelWriters int64) (LoadFileWriter, error) {
	bufWriter, err := misc.CreateBufferedWriter(outputFilePath)
	if err != nil {
		return nil, err
	}

	pSchema, err := parquetSchema(schema, destType)
	if err != nil {
		return nil, err
	}

	w, err := writer.NewCSVWriterFromWriter(pSchema, bufWriter, maxParallelWriters)
	if err != nil {
		return nil, err
	}

	return &parquetWriter{
		writer:     w,
		schema:     pSchema,
		fileWriter: bufWriter,
	}, nil
}

func (p *parquetWriter) WriteRow(row []interface{}) error {
	return p.writer.Write(row)
}

func (p *parquetWriter) Close() error {
	err := p.writer.WriteStop()
	if err != nil {
		return err
	}

	return p.fileWriter.Close()
}

func (*parquetWriter) WriteGZ(_ string) error {
	return errors.New("not implemented")
}

func (*parquetWriter) Write(_ []byte) (int, error) {
	return 0, errors.New("not implemented")
}

func (p *parquetWriter) GetLoadFile() *os.File {
	return p.fileWriter.GetFile()
}

func sortedTableColumns(schema model.TableSchema) []string {
	columns := lo.Keys(schema)
	sort.Strings(columns)
	return columns
}

func parquetSchema(schema model.TableSchema, destType string) ([]string, error) {
	whTypeMap, ok := rudderDataTypeToParquetDataType[destType]
	if !ok {
		return nil, errors.New("unsupported warehouse for parquet load files")
	}

	var pSchema []string
	for _, col := range sortedTableColumns(schema) {
		pType := fmt.Sprintf("name=%s, %s", warehouseutils.ToProviderCase(destType, col), whTypeMap[schema[col]])
		pSchema = append(pSchema, pType)
	}
	return pSchema, nil
}
