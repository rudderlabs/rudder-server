package encoding

import (
	"errors"
	"fmt"
	"os"
	"sort"

	"github.com/samber/lo"
	"github.com/xitongsys/parquet-go/writer"

	"github.com/rudderlabs/rudder-go-kit/bytesize"

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

// parquetTypesWithText is the Parquet type set used by RS and S3 datalake; Microsoft Fabric extends a copy with json.
var parquetTypesWithText = map[string]string{
	"bigint":   parquetInt64,
	"int":      parquetInt64,
	"boolean":  parquetBoolean,
	"float":    parquetDouble,
	"string":   parquetString,
	"text":     parquetString,
	"datetime": parquetTimestampMicros,
}

var rudderDataTypeToParquetDataType = map[string]map[string]string{
	warehouseutils.RS:         parquetTypesWithText,
	warehouseutils.S3Datalake: parquetTypesWithText,
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
	warehouseutils.MicrosoftFabric: lo.Assign(parquetTypesWithText, map[string]string{"json": parquetString}),
}

type parquetWriter struct {
	writer         *writer.CSVWriter
	fileWriter     misc.BufferedWriter
	maxQueuedBytes int64
	queuedBytes    int64
}

const maxParquetQueuedBytes = 64 * bytesize.MB

func createParquetWriter(outputFilePath string, schema model.TableSchema, destType string, maxParallelWriters int64, disableParquetColumnIndex bool, pageSize int64) (LoadFileWriter, error) {
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
	w.PageSize = pageSize

	pw := &parquetWriter{
		writer:     w,
		fileWriter: bufWriter,
	}
	// The default 8 KiB page path keeps parquet-go's existing flush behavior.
	if pageSize > defaultParquetPageSize {
		pw.maxQueuedBytes = maxParquetQueuedBytes
	}
	return pw, nil
}

func (p *parquetWriter) WriteRow(row []any) error {
	if err := p.writer.Write(row); err != nil {
		return err
	}
	if p.maxQueuedBytes == 0 {
		return nil
	}
	p.queuedBytes += rowSize(row)
	if p.queuedBytes < p.maxQueuedBytes {
		return nil
	}
	p.queuedBytes = 0
	return p.writer.Flush(false)
}

// rowSize counts the real bytes of a row; parquet-go estimates every value as four bytes.
func rowSize(row []any) (n int64) {
	for _, v := range row {
		if s, ok := v.(string); ok {
			n += int64(len(s))
		} else {
			n += 8
		}
	}
	return n
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
