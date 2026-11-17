package encoding

import (
	"errors"
	"fmt"
	"os"
	"sort"

	"github.com/samber/lo"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

const (
	// The fork's automatic flush threshold scales with page size and column count.
	// Keep the row queue bounded for wide schemas while leaving the row-group size unchanged.
	maxParquetQueuedBytes = 64 << 20

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

func createParquetWriter(outputFilePath string, schema model.TableSchema, destType string, maxParallelWriters int64, disableParquetColumnIndex bool, pageSize int64, codec parquet.CompressionCodec) (LoadFileWriter, error) {
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
	w.CompressionType = codec
	// The upstream queue triggers at NP * PageSize * columnCount, but its
	// estimate counts each interface value as four bytes, even for large strings.
	// Bound the queued values' actual sizes when large pages raise that threshold.
	maxQueuedBytes := int64(maxParquetQueuedBytes)
	// Preserve the library's existing flush behavior at small page sizes.
	if w.PageSize <= maxQueuedBytes/w.NP/w.SchemaHandler.GetColumnNum() {
		maxQueuedBytes = 0
	}

	return &parquetWriter{
		writer:         w,
		fileWriter:     bufWriter,
		maxQueuedBytes: maxQueuedBytes,
	}, nil
}

func (p *parquetWriter) WriteRow(row []any) error {
	if err := p.writer.Write(row); err != nil {
		return err
	}
	if p.maxQueuedBytes > 0 {
		if len(p.writer.Objs) == 0 { // the fork flushed automatically
			p.queuedBytes = 0
		} else {
			// The CSV writer queues []any rows, which its SizeOf does not unwrap.
			// Count the string bytes as well as fixed-width values without copying them.
			for _, value := range row {
				switch v := value.(type) {
				case string:
					p.queuedBytes += int64(len(v))
				case []byte:
					p.queuedBytes += int64(len(v))
				default:
					p.queuedBytes += 8
				}
			}
			if p.queuedBytes >= p.maxQueuedBytes {
				if err := p.writer.Flush(false); err != nil {
					return err
				}
				p.queuedBytes = 0
			}
		}
	}
	return nil
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
