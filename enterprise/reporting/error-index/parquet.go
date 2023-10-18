package error_index

import (
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"

	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

type writerParquet struct {
	config struct {
		parquetParallelWriters misc.ValueLoader[int64]
		rowGroupSizeInMB       misc.ValueLoader[int64]
		pageSizeInKB           misc.ValueLoader[int64]
	}
}

func newWriterParquet(conf *config.Config) writerParquet {
	wp := writerParquet{}
	wp.config.parquetParallelWriters = conf.GetReloadableInt64Var(8, 1, "Reporting.errorIndexReporting.parquetParallelWriters")
	wp.config.rowGroupSizeInMB = conf.GetReloadableInt64Var(512, 1, "Reporting.errorIndexReporting.rowGroupSizeInMB")
	wp.config.pageSizeInKB = conf.GetReloadableInt64Var(8, 1, "Reporting.errorIndexReporting.pageSizeInKB")
	return wp
}

// Write writes the given payloads in parquet format. Also, sorts the payloads by sortKey to achieve better encoding.
func (wp writerParquet) Write(w io.Writer, payloads []payload) error {
	pw, err := writer.NewParquetWriterFromWriter(w, new(payloadParquet),
		wp.config.parquetParallelWriters.Load(),
	)
	if err != nil {
		return fmt.Errorf("creating parquet writer: %v", err)
	}

	pw.RowGroupSize = wp.config.rowGroupSizeInMB.Load() * bytesize.MB
	pw.PageSize = wp.config.pageSizeInKB.Load() * bytesize.KB
	pw.CompressionType = parquet.CompressionCodec_SNAPPY

	sortKey := func(p payload) string {
		keys := []string{
			p.DestinationID,
			p.TransformationID,
			p.TrackingPlanID,
			p.FailedStage,
			p.EventType,
			p.EventName,
		}
		return strings.Join(keys, "::")
	}

	sort.Slice(payloads, func(i, j int) bool {
		return sortKey(payloads[i]) < sortKey(payloads[j])
	})

	for _, payload := range payloads {
		if err = pw.Write(payload.toParquet()); err != nil {
			return fmt.Errorf("writing parquet: %v", err)
		}
	}

	if err = pw.WriteStop(); err != nil {
		return fmt.Errorf("stopping parquet writer: %v", err)
	}

	return nil
}

func (wp writerParquet) Extension() string {
	return ".parquet"
}
