package reporting

import (
	"fmt"
	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/types"
	"github.com/xitongsys/parquet-go/writer"
	"io"
)

type payloadParquet struct {
	MessageID        string `parquet:"name=messageId, type=BYTE_ARRAY, convertedtype=UTF8"`
	SourceID         string `parquet:"name=sourceId, type=BYTE_ARRAY, convertedtype=UTF8"`
	DestinationID    string `parquet:"name=destinationId, type=BYTE_ARRAY, convertedtype=UTF8"`
	TransformationID string `parquet:"name=transformationId, type=BYTE_ARRAY, convertedtype=UTF8"`
	TrackingPlanID   string `parquet:"name=trackingPlanId, type=BYTE_ARRAY, convertedtype=UTF8"`
	FailedStage      string `parquet:"name=failedStage, type=BYTE_ARRAY, convertedtype=UTF8"`
	EventType        string `parquet:"name=eventType, type=BYTE_ARRAY, convertedtype=UTF8"`
	EventName        string `parquet:"name=eventName, type=BYTE_ARRAY, convertedtype=UTF8"`
	ReceivedAt       int64  `parquet:"name=receivedAt, type=INT64, convertedtype=TIMESTAMP_MICROS"`
	FailedAt         int64  `parquet:"name=failedAt, type=INT64, convertedtype=TIMESTAMP_MICROS"`
}

func toParquet(p payload) payloadParquet {
	return payloadParquet{
		MessageID:        p.MessageID,
		SourceID:         p.SourceID,
		DestinationID:    p.DestinationID,
		TransformationID: p.TransformationID,
		TrackingPlanID:   p.TrackingPlanID,
		FailedStage:      p.FailedStage,
		EventType:        p.EventType,
		EventName:        p.EventName,
		ReceivedAt:       types.TimeToTIMESTAMP_MICROS(p.ReceivedAt, false),
		FailedAt:         types.TimeToTIMESTAMP_MICROS(p.FailedAt, false),
	}
}

type WriterParquet struct {
	config struct {
		parquetParallelWriters misc.ValueLoader[int64]
	}
}

func newWriterParquet(conf *config.Config) WriterParquet {
	wp := WriterParquet{}
	wp.config.parquetParallelWriters = conf.GetReloadableInt64Var(8, 1, "Reporting.parquetParallelWriters")
	return wp
}

func (wp WriterParquet) Write(w io.Writer, payloads []payload) error {
	pw, err := writer.NewParquetWriterFromWriter(
		w,
		new(payloadParquet),
		wp.config.parquetParallelWriters.Load(),
	)
	if err != nil {
		return fmt.Errorf("creating parquet writer: %v", err)
	}

	pw.RowGroupSize = 128 * bytesize.MB
	pw.PageSize = 8 * bytesize.KB
	pw.CompressionType = parquet.CompressionCodec_SNAPPY

	for _, payload := range payloads {
		if err = pw.Write(toParquet(payload)); err != nil {
			return fmt.Errorf("writing parquet: %v", err)
		}
	}

	if err = pw.WriteStop(); err != nil {
		return fmt.Errorf("stopping parquet writer: %v", err)
	}

	return nil
}
