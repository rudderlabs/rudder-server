package encoding_test

import (
	"compress/gzip"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"

	"github.com/google/uuid"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/encoding"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/stretchr/testify/require"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

func TestReaderLoader(t *testing.T) {
	t.Parallel()

	misc.Init()
	encoding.Init()

	testCases := []struct {
		name                string
		destType            string
		provider            string
		loadFileType        string
		timeColumnFormatMap map[string]string
	}{
		{
			name:         "CSV",
			destType:     warehouseutils.RS,
			provider:     warehouseutils.RS,
			loadFileType: warehouseutils.LOAD_FILE_TYPE_CSV,
			timeColumnFormatMap: map[string]string{
				encoding.UUIDTsColumn: misc.RFC3339Milli,
			},
		},
		{
			name:         "JSON",
			destType:     warehouseutils.BQ,
			provider:     warehouseutils.BQ,
			loadFileType: warehouseutils.LOAD_FILE_TYPE_JSON,
			timeColumnFormatMap: map[string]string{
				encoding.UUIDTsColumn:   encoding.BQUuidTSFormat,
				encoding.LoadedAtColumn: encoding.BQLoadedAtFormat,
			},
		},
	}
	for _, tc := range testCases {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var (
				outputFilePath = fmt.Sprintf("/tmp/%s.csv.gz", uuid.New().String())
				lines          = 100
			)

			writer, err := misc.CreateGZ(outputFilePath)
			require.NoError(t, err)

			t.Cleanup(func() {
				require.NoError(t, os.Remove(outputFilePath))
			})

			t.Run("add and write", func(t *testing.T) {
				for i := 0; i < lines; i++ {
					c := encoding.GetNewEventLoader(tc.destType, tc.loadFileType, writer)

					c.AddColumn("column1", "string", "value1")
					c.AddColumn("column2", "string", "value2")
					c.AddEmptyColumn("column3")
					c.AddEmptyColumn("column4")
					c.AddColumn("column5", "string", "value5")
					c.AddColumn("column6", "string", "value6")
					c.AddEmptyColumn("column7")
					c.AddEmptyColumn("column8")
					c.AddRow([]string{"column9", "column10"}, []string{"value9", "value10"})

					err := c.Write()
					require.NoError(t, err)
				}

				err = writer.Close()
				require.NoError(t, err)

				f, err := os.Open(outputFilePath)
				require.NoError(t, err)

				t.Cleanup(func() {
					require.NoError(t, f.Close())
				})

				gzipReader, err := gzip.NewReader(f)
				require.NoError(t, err)

				t.Cleanup(func() {
					require.NoError(t, gzipReader.Close())
				})

				r := encoding.NewEventReader(gzipReader, tc.provider)
				for i := 0; i < lines; i++ {
					output, err := r.Read([]string{"column1", "column2", "column3", "column4", "column5", "column6", "column7", "column8", "column9", "column10"})
					require.NoError(t, err)
					require.Equal(t, output, []string{"value1", "value2", "", "", "value5", "value6", "", "", "value9", "value10"})
				}
			})

			t.Run("time column", func(t *testing.T) {
				c := encoding.GetNewEventLoader(tc.destType, tc.loadFileType, writer)

				t.Run("GetLoadTimeFormat", func(t *testing.T) {
					for column, format := range tc.timeColumnFormatMap {
						require.Equal(t, c.GetLoadTimeFormat(column), format)
					}
				})

				t.Run("IsLoadTimeColumn", func(t *testing.T) {
					for column := range tc.timeColumnFormatMap {
						require.True(t, c.IsLoadTimeColumn(column))
					}

					require.False(t, c.IsLoadTimeColumn("default"))
				})
			})
		})
	}

	t.Run("Parquet", func(t *testing.T) {
		t.Parallel()

		var (
			outputFilePath      = fmt.Sprintf("/tmp/%s.parquet", uuid.New().String())
			lines               = 100
			destType            = warehouseutils.S3_DATALAKE
			loadFileType        = warehouseutils.LOAD_FILE_TYPE_PARQUET
			timeColumnFormatMap = map[string]string{
				encoding.UUIDTsColumn: time.RFC3339,
			}
			schema = model.TableSchema{
				"column1":  "bigint",
				"column2":  "int",
				"column3":  "string",
				"column4":  "string",
				"column5":  "boolean",
				"column6":  "float",
				"column7":  "string",
				"column8":  "string",
				"column9":  "string",
				"column10": "text",
				"column11": "datetime",
				"column12": "string",
			}
		)

		writer, err := encoding.CreateParquetWriter(schema, outputFilePath, destType)
		require.NoError(t, err)

		t.Cleanup(func() {
			require.NoError(t, os.Remove(outputFilePath))
		})

		t.Run("add and write", func(t *testing.T) {
			for i := 0; i < lines; i++ {
				c := encoding.GetNewEventLoader(destType, loadFileType, writer)

				// add columns in sorted order
				c.AddColumn("column1", "bigint", 1234567890)
				c.AddColumn("column10", "text", "RudderStack")
				c.AddColumn("column11", "datetime", "2022-01-20T13:39:21.033Z")
				c.AddEmptyColumn("column12")
				c.AddColumn("column2", "int", 123)
				c.AddEmptyColumn("column3")
				c.AddEmptyColumn("column4")
				c.AddColumn("column5", "boolean", true)
				c.AddColumn("column6", "float", 123.123)
				c.AddEmptyColumn("column7")
				c.AddEmptyColumn("column8")
				c.AddColumn("column9", "string", "RudderStack")

				err := c.Write()
				require.NoError(t, err)
			}

			err = writer.Close()
			require.NoError(t, err)

			f, err := local.NewLocalFileReader(outputFilePath)
			require.NoError(t, err)

			t.Cleanup(func() {
				require.NoError(t, f.Close())
			})

			type parquetData struct {
				Column1  *int64
				Column10 *string
				Column11 *int64
				Column12 *string
				Column2  *int64
				Column3  *string
				Column4  *string
				Column5  *bool
				Column6  *float64
				Column7  *string
				Column8  *string
				Column9  *string
			}

			pr, err := reader.NewParquetReader(f, nil, int64(lines))
			require.NoError(t, err)

			t.Cleanup(func() {
				pr.ReadStop()
			})

			data := make([]*parquetData, lines)
			err = pr.Read(&data)
			require.NoError(t, err)

			require.Len(t, data, lines)

			for i := 0; i < lines; i++ {
				require.EqualValues(t, data[i].Column12, (*string)(nil))
				require.EqualValues(t, data[i].Column3, (*string)(nil))
				require.EqualValues(t, data[i].Column4, (*string)(nil))
				require.EqualValues(t, data[i].Column7, (*string)(nil))
				require.EqualValues(t, data[i].Column8, (*string)(nil))

				require.EqualValues(t, *data[i].Column1, int64(1234567890))
				require.EqualValues(t, *data[i].Column10, "RudderStack")
				require.EqualValues(t, *data[i].Column11, int64(1642685961033000))
				require.EqualValues(t, *data[i].Column2, int64(123))
				require.EqualValues(t, *data[i].Column5, true)
				require.EqualValues(t, *data[i].Column6, float64(123.123))
				require.EqualValues(t, *data[i].Column9, "RudderStack")
			}
		})

		t.Run("invalid file path", func(t *testing.T) {
			_, err := encoding.CreateParquetWriter(schema, "", destType)
			require.EqualError(t, err, errors.New("open : no such file or directory").Error())
		})

		t.Run("unsupported dest type", func(t *testing.T) {
			_, err := encoding.CreateParquetWriter(schema, outputFilePath, warehouseutils.BQ)
			require.EqualError(t, err, errors.New("unsupported warehouse for parquet load files").Error())
		})

		t.Run("time column", func(t *testing.T) {
			c := encoding.GetNewEventLoader(destType, loadFileType, writer)

			t.Run("GetLoadTimeFormat", func(t *testing.T) {
				for column, format := range timeColumnFormatMap {
					require.Equal(t, c.GetLoadTimeFormat(column), format)
				}
			})

			t.Run("IsLoadTimeColumn", func(t *testing.T) {
				for column := range timeColumnFormatMap {
					require.True(t, c.IsLoadTimeColumn(column))
				}

				require.False(t, c.IsLoadTimeColumn("default"))
			})
		})
	})
}
