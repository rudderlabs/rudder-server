package testhelper

import (
	"io"
	"os"
	"testing"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/stretchr/testify/require"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/layout"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/reader"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestWriteSampleParquetLoadFile(t *testing.T) {
	schema := model.TableSchema{
		"id": "string", "received_at": "datetime", "test_bool": "boolean",
		"test_datetime": "datetime", "test_float": "float", "test_int": "int", "test_string": "string",
	}
	for _, destType := range []string{whutils.RS, whutils.MicrosoftFabric, whutils.DELTALAKE} {
		t.Run(destType, func(t *testing.T) {
			path := WriteSampleParquetLoadFile(t, destType, schema)
			file, err := local.NewLocalFileReader(path)
			require.NoError(t, err)
			defer func() { require.NoError(t, file.Close()) }()
			pr, err := reader.NewParquetReader(file, nil, 1)
			require.NoError(t, err)
			defer pr.ReadStop()

			require.EqualValues(t, 14, pr.GetNumRows())
			data, err := os.Open(path)
			require.NoError(t, err)
			defer func() { require.NoError(t, data.Close()) }()
			for _, column := range pr.Footer.RowGroups[0].Columns {
				meta := column.MetaData
				require.Equal(t, parquet.CompressionCodec_SNAPPY, meta.Codec)
				require.NotNil(t, meta.Statistics)
				if meta.PathInSchema[0] == "Test_string" {
					_, err = data.Seek(meta.DataPageOffset, io.SeekStart)
					require.NoError(t, err)
					transport := thrift.NewTBufferedTransport(thrift.NewStreamTransportR(data), 4096)
					header, err := layout.ReadPageHeader(transport)
					require.NoError(t, err)
					// The first two 14 KiB strings share a 1 MiB page. With
					// 8 KiB pages, the first page contains only one value.
					require.Equal(t, parquet.PageType_DATA_PAGE, header.Type)
					require.Equal(t, int32(2), header.DataPageHeader.NumValues)
					require.Greater(t, header.UncompressedPageSize, int32(8192))
				}
			}
			type row struct {
				Id            *string
				Received_at   *int64
				Test_bool     *bool
				Test_datetime *int64
				Test_float    *float64
				Test_int      *int64
				Test_string   *string
			}
			rows := make([]*row, 14)
			require.NoError(t, pr.Read(&rows))
			require.Equal(t, "6734e5db-f918-4efe-1421-872f66e235c5", *rows[0].Id)
			require.Equal(t, int64(1671087229000000), *rows[0].Received_at)
			require.Nil(t, rows[0].Test_bool)
			require.Equal(t, int64(125), *rows[0].Test_int)
			require.Equal(t, false, *rows[3].Test_bool)
			require.Equal(t, float64(126.75), *rows[3].Test_float)
			require.Equal(t, SampleParquetTestRecords()[3][6], *rows[3].Test_string)
			require.Equal(t, int64(1671087229000000), *rows[3].Test_datetime)
			require.Nil(t, rows[4].Test_datetime)
		})
	}
}
