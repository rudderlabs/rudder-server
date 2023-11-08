package encoding_test

import (
	"compress/gzip"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/encoding"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestReaderLoader(t *testing.T) {
	misc.Init()

	tmpDir, err := misc.CreateTMPDIR()
	require.NoError(t, err)

	t.Run("Parquet", func(t *testing.T) {
		var (
			outputFilePath  = tmpDir + "/" + uuid.New().String() + ".parquet"
			loadFileType    = warehouseutils.LoadFileTypeParquet
			destinationType = warehouseutils.S3Datalake
			lines           = 100
			schema          = model.TableSchema{
				"column1":  "bigint",
				"column2":  "int",
				"column3":  "float",
				"column4":  "string",
				"column5":  "text",
				"column6":  "boolean",
				"column7":  "boolean",
				"column8":  "datetime",
				"column9":  "string",
				"column10": "string",
				"column11": "string",
				"column12": "int",
				"column13": "float",
				"column14": "string",
				"column15": "boolean",
				"column16": "datetime",
				"column17": "datetime",
			}
		)
		t.Log("Parquet", outputFilePath)

		ef := encoding.NewFactory(config.New())

		writer, err := ef.NewLoadFileWriter(loadFileType, outputFilePath, schema, destinationType)
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, os.Remove(writer.GetLoadFile().Name()))
		})

		for i := 0; i < lines; i++ {
			c := ef.NewEventLoader(writer, loadFileType, destinationType)

			c.AddColumn("column1", "bigint", 1234567890)
			c.AddEmptyColumn("column10")

			// Invalid data type
			c.AddColumn("column11", "test_data_type", "Random Data Type")
			c.AddColumn("column12", "int", 1.11)
			c.AddColumn("column13", "float", 1)
			c.AddColumn("column14", "string", 1)
			c.AddColumn("column15", "boolean", "1")
			c.AddColumn("column16", "datetime", 101)
			c.AddColumn("column17", "datetime", "RudderStack")

			c.AddColumn("column2", "int", 2)
			c.AddColumn("column3", "float", 1.11)
			c.AddColumn("column4", "string", "RudderStack")
			c.AddColumn("column5", "text", "RudderStack")
			c.AddColumn("column6", "boolean", true)
			c.AddColumn("column7", "boolean", false)
			c.AddColumn("column8", "datetime", "2022-01-20T13:39:21.033Z")

			c.AddEmptyColumn("column9")

			require.True(t, c.IsLoadTimeColumn(encoding.UUIDTsColumn))
			require.False(t, c.IsLoadTimeColumn("column1"))

			require.Equal(t, c.GetLoadTimeFormat(encoding.UUIDTsColumn), time.RFC3339)
			require.Equal(t, c.GetLoadTimeFormat("column1"), time.RFC3339)

			require.NoError(t, c.Write())

			val, err := c.WriteToString()
			require.Empty(t, val)
			require.EqualError(t, err, "not implemented")
		}
		require.NoError(t, writer.Close())

		bytesWritten, err := writer.Write([]byte("RudderStack"))
		require.Equal(t, 0, bytesWritten)
		require.Error(t, err, "not implemented")
		require.Error(t, writer.WriteGZ("RudderStack"), "not implemented")

		f, err := local.NewLocalFileReader(outputFilePath)
		require.NoError(t, err)

		t.Cleanup(func() {
			require.NoError(t, f.Close())
		})

		type parquetData struct {
			Column1  *int64
			Column10 *string
			Column11 *int64
			Column12 *int64
			Column13 *float64
			Column14 *string
			Column15 *bool
			Column16 *int64
			Column17 *int64
			Column2  *int64
			Column3  *float64
			Column4  *string
			Column5  *string
			Column6  *bool
			Column7  *bool
			Column8  *int64
			Column9  *string
		}

		pr, err := reader.NewParquetReader(f, nil, int64(lines))
		require.NoError(t, err)

		data := make([]*parquetData, lines)
		require.NoError(t, pr.Read(&data))
		require.Len(t, data, lines)

		t.Cleanup(func() {
			pr.ReadStop()
		})

		for i := 0; i < lines; i++ {
			require.EqualValues(t, data[i].Column9, (*string)(nil))
			require.EqualValues(t, data[i].Column10, (*string)(nil))

			require.EqualValues(t, data[i].Column11, (*int64)(nil))
			require.EqualValues(t, data[i].Column12, (*int64)(nil))
			require.EqualValues(t, data[i].Column13, (*float64)(nil))
			require.EqualValues(t, data[i].Column14, (*string)(nil))
			require.EqualValues(t, data[i].Column15, (*bool)(nil))
			require.EqualValues(t, data[i].Column16, (*int64)(nil))
			require.EqualValues(t, data[i].Column17, (*int64)(nil))

			require.EqualValues(t, *data[i].Column1, int64(1234567890))
			require.EqualValues(t, *data[i].Column2, int64(2))
			require.EqualValues(t, *data[i].Column3, 1.11)
			require.EqualValues(t, *data[i].Column4, "RudderStack")
			require.EqualValues(t, *data[i].Column5, "RudderStack")
			require.EqualValues(t, *data[i].Column6, true)
			require.EqualValues(t, *data[i].Column7, false)
			require.EqualValues(t, *data[i].Column8, int64(1642685961033000))
		}

		nullsMap := lo.MapEntries(pr.ColumnBuffers, func(key string, columnBuffer *reader.ColumnBufferType) (string, int64) {
			metadata := columnBuffer.ChunkHeader.MetaData
			columnName := strings.ToLower(metadata.GetPathInSchema()[0])
			nullCount := metadata.GetStatistics().GetNullCount()
			return columnName, nullCount
		})
		require.Equal(t, nullsMap, map[string]int64{
			"column1":  0,
			"column10": 100,
			"column11": 100,
			"column12": 100,
			"column13": 100,
			"column14": 100,
			"column15": 100,
			"column16": 100,
			"column17": 100,
			"column2":  0,
			"column3":  0,
			"column4":  0,
			"column5":  0,
			"column6":  0,
			"column7":  0,
			"column8":  0,
			"column9":  100,
		})
	})

	t.Run("JSON", func(t *testing.T) {
		var (
			outputFilePath  = tmpDir + "/" + uuid.New().String() + ".json.gz"
			loadFileType    = warehouseutils.LoadFileTypeJson
			destinationType = warehouseutils.BQ
			lines           = 100
		)

		ef := encoding.NewFactory(config.New())

		writer, err := ef.NewLoadFileWriter(loadFileType, outputFilePath, nil, destinationType)
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, os.Remove(writer.GetLoadFile().Name()))
		})

		for i := 0; i < lines; i++ {
			c := ef.NewEventLoader(writer, loadFileType, destinationType)
			c.AddColumn("column1", "bigint", 1234567890)
			c.AddColumn("column2", "int", 2)
			c.AddColumn("column3", "float", 1.11)
			c.AddColumn("column4", "string", "RudderStack")
			c.AddColumn("column5", "text", "RudderStack")
			c.AddColumn("column6", "boolean", true)
			c.AddColumn("column7", "boolean", false)
			c.AddColumn("column8", "datetime", "2022-01-20T13:39:21.033Z")

			c.AddEmptyColumn("column9")
			c.AddEmptyColumn("column10")

			c.AddRow([]string{"colum11", "column12"}, []string{"RudderStack-row", "RudderStack-row"})

			require.True(t, c.IsLoadTimeColumn(encoding.UUIDTsColumn))
			require.True(t, c.IsLoadTimeColumn(encoding.LoadedAtColumn))
			require.False(t, c.IsLoadTimeColumn("column1"))

			require.Equal(t, c.GetLoadTimeFormat(encoding.UUIDTsColumn), encoding.BQUuidTSFormat)
			require.Equal(t, c.GetLoadTimeFormat(encoding.LoadedAtColumn), encoding.BQLoadedAtFormat)
			require.Empty(t, c.GetLoadTimeFormat("column1"))

			require.NoError(t, c.Write())
		}
		require.NoError(t, writer.Close())

		f, err := os.Open(outputFilePath)
		require.NoError(t, err)

		gzipReader, err := gzip.NewReader(f)
		require.NoError(t, err)

		t.Cleanup(func() {
			require.NoError(t, gzipReader.Close())
		})

		r := ef.NewEventReader(gzipReader, destinationType)
		for i := 0; i < lines; i++ {
			output, err := r.Read([]string{"column1", "column2", "column3", "column4", "column5", "column6", "column7", "column8", "column9", "column10", "colum11", "column12"})
			require.NoError(t, err)
			require.Equal(t, output, []string{"1234567890", "2", "1.11", "RudderStack", "RudderStack", "true", "false", "2022-01-20T13:39:21.033Z", "", "", "RudderStack-row", "RudderStack-row"})
		}
	})

	t.Run("CSV", func(t *testing.T) {
		var (
			outputFilePath  = tmpDir + "/" + uuid.New().String() + ".csv.gz"
			loadFileType    = warehouseutils.LoadFileTypeCsv
			destinationType = warehouseutils.RS
			lines           = 100
		)

		ef := encoding.NewFactory(config.New())

		writer, err := ef.NewLoadFileWriter(loadFileType, outputFilePath, nil, destinationType)
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, os.Remove(writer.GetLoadFile().Name()))
		})

		for i := 0; i < lines; i++ {
			c := ef.NewEventLoader(writer, loadFileType, destinationType)
			c.AddColumn("column1", "bigint", 1234567890)
			c.AddColumn("column2", "int", 2)
			c.AddColumn("column3", "float", 1.11)
			c.AddColumn("column4", "string", "RudderStack")
			c.AddColumn("column5", "text", "RudderStack")
			c.AddColumn("column6", "boolean", true)
			c.AddColumn("column7", "boolean", false)
			c.AddColumn("column8", "datetime", "2022-01-20T13:39:21.033Z")

			c.AddEmptyColumn("column9")
			c.AddEmptyColumn("column10")

			c.AddRow([]string{"colum11", "column12"}, []string{"RudderStack-row", "RudderStack-row"})

			require.True(t, c.IsLoadTimeColumn(encoding.UUIDTsColumn))
			require.False(t, c.IsLoadTimeColumn("column1"))

			require.Equal(t, c.GetLoadTimeFormat(encoding.UUIDTsColumn), misc.RFC3339Milli)
			require.Equal(t, c.GetLoadTimeFormat("column1"), misc.RFC3339Milli)

			require.NoError(t, c.Write())
		}
		require.NoError(t, writer.Close())

		f, err := os.Open(outputFilePath)
		require.NoError(t, err)

		gzipReader, err := gzip.NewReader(f)
		require.NoError(t, err)

		t.Cleanup(func() {
			require.NoError(t, gzipReader.Close())
		})

		r := ef.NewEventReader(gzipReader, destinationType)
		for i := 0; i < lines; i++ {
			output, err := r.Read([]string{"column1", "column2", "column3", "column4", "column5", "column6", "column7", "column8", "column9", "column10", "colum11", "column12"})
			require.NoError(t, err)
			require.Equal(t, output, []string{"1234567890", "2", "1.11", "RudderStack", "RudderStack", "true", "false", "2022-01-20T13:39:21.033Z", "", "", "RudderStack-row", "RudderStack-row"})
		}
	})

	t.Run("Empty files", func(t *testing.T) {
		ef := encoding.NewFactory(config.New())

		t.Run("csv", func(t *testing.T) {
			destinationType := warehouseutils.RS
			csvFilePath := tmpDir + "/" + uuid.New().String() + ".csv.gz"
			csvWriter, err := ef.NewLoadFileWriter(warehouseutils.LoadFileTypeCsv, csvFilePath, nil, destinationType)
			require.NoError(t, err)
			require.NoError(t, csvWriter.Close())

			t.Cleanup(func() {
				require.NoError(t, os.Remove(csvFilePath))
			})
			require.NoError(t, csvWriter.Close())

			f, err := os.Open(csvFilePath)
			require.NoError(t, err)

			gzipReader, err := gzip.NewReader(f)
			require.NoError(t, err)

			t.Cleanup(func() {
				require.NoError(t, gzipReader.Close())
			})

			r := ef.NewEventReader(gzipReader, destinationType)

			output, err := r.Read([]string{"column1", "column2", "column3", "column4", "column5", "column6", "column7", "column8", "column9", "column10", "colum11", "column12"})
			require.Error(t, err, io.EOF)
			require.Nil(t, output)
		})

		t.Run("json", func(t *testing.T) {
			destinationType := warehouseutils.BQ
			jsonFilepath := tmpDir + "/" + uuid.New().String() + ".json.gz"
			csvWriter, err := ef.NewLoadFileWriter(warehouseutils.LoadFileTypeJson, jsonFilepath, nil, destinationType)
			require.NoError(t, err)

			t.Cleanup(func() {
				require.NoError(t, os.Remove(jsonFilepath))
			})
			require.NoError(t, csvWriter.Close())

			f, err := os.Open(jsonFilepath)
			require.NoError(t, err)

			gzipReader, err := gzip.NewReader(f)
			require.NoError(t, err)

			t.Cleanup(func() {
				require.NoError(t, gzipReader.Close())
			})

			r := ef.NewEventReader(gzipReader, destinationType)

			output, err := r.Read([]string{"column1", "column2", "column3", "column4", "column5", "column6", "column7", "column8", "column9", "column10", "colum11", "column12"})
			require.Error(t, err, io.EOF)
			require.Equal(t, output, []string{})
		})
	})
}
