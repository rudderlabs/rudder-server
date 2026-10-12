package encoding

import (
	"errors"
	"fmt"
	"os"
	"sort"

	"github.com/samber/lo"
	"github.com/xitongsys/parquet-go/writer"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

const (
	parquetInt64           = "type=INT64, repetitiontype=OPTIONAL"
	parquetBoolean         = "type=BOOLEAN, repetitiontype=OPTIONAL"
	parquetDouble          = "type=DOUBLE, repetitiontype=OPTIONAL"
	parquetString          = "type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL"
	parquetTimestampMicros = "type=INT64, convertedtype=TIMESTAMP_MICROS, repetitiontype=OPTIONAL"
)

// Each destination's Parquet type set is the base set plus the extra rudder types it accepts.
var (
	parquetBaseTypes = map[string]string{
		model.IntDataType:      parquetInt64,
		model.BooleanDataType:  parquetBoolean,
		model.FloatDataType:    parquetDouble,
		model.StringDataType:   parquetString,
		model.DateTimeDataType: parquetTimestampMicros,
	}
	parquetTypesWithText = lo.Assign(parquetBaseTypes, map[string]string{
		model.BigIntDataType: parquetInt64,
		model.TextDataType:   parquetString,
	})
)

var rudderDataTypeToParquetDataType = map[string]map[string]string{
	warehouseutils.RS:              parquetTypesWithText,
	warehouseutils.S3Datalake:      parquetTypesWithText,
	warehouseutils.GCSDatalake:     parquetBaseTypes,
	warehouseutils.AzureDatalake:   parquetBaseTypes,
	warehouseutils.DELTALAKE:       parquetBaseTypes,
	warehouseutils.MicrosoftFabric: lo.Assign(parquetTypesWithText, map[string]string{model.JSONDataType: parquetString}),
}

type parquetWriter struct {
	writer     *writer.CSVWriter
	fileWriter misc.BufferedWriter
}

func createParquetWriter(outputFilePath string, schema model.TableSchema, destType string, maxParallelWriters int64, disableParquetColumnIndex bool) (LoadFileWriter, error) {
	bufWriter, err := misc.CreateBufferedWriter(outputFilePath)
	if err != nil {
		return nil, err
	}

	pSchema, err := parquetSchema(schema, destType)
	if err != nil {
		return nil, err
	}

	// Disable column index to avoid the column index being written to the parquet file.
	w, err := writer.NewCSVWriterFromWriter(pSchema, bufWriter, maxParallelWriters, writer.WithDisableColumnIndex(disableParquetColumnIndex))
	if err != nil {
		return nil, err
	}

	return &parquetWriter{
		writer:     w,
		fileWriter: bufWriter,
	}, nil
}

func (p *parquetWriter) WriteRow(row []any) error {
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
		parquetType, ok := whTypeMap[schema[col]]
		if !ok {
			return nil, fmt.Errorf("unsupported data type %q for parquet load files in warehouse %q", schema[col], destType)
		}
		pType := fmt.Sprintf("name=%s, %s", warehouseutils.ToProviderCase(destType, col), parquetType)
		pSchema = append(pSchema, pType)
	}
	return pSchema, nil
}
