package reporting

import (
	"bytes"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/stretchr/testify/require"
	"github.com/xitongsys/parquet-go-source/buffer"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/types"
	"strconv"
	"testing"
	"time"
)

func TestWriterParquet(t *testing.T) {
	now := time.Now()

	var payloads []payload
	for i := 0; i < 100; i++ {
		payloads = append(payloads, payload{
			MessageID:        "messageId" + strconv.Itoa(i),
			SourceID:         "sourceId" + strconv.Itoa(i),
			DestinationID:    "destinationId" + strconv.Itoa(i),
			TransformationID: "transformationId" + strconv.Itoa(i),
			TrackingPlanID:   "trackingPlanId" + strconv.Itoa(i),
			FailedStage:      "failedStage" + strconv.Itoa(i),
			EventType:        "eventType" + strconv.Itoa(i),
			EventName:        "eventName" + strconv.Itoa(i),
			ReceivedAt:       now.Add(time.Duration(i) * time.Second),
			FailedAt:         now.Add(time.Duration(i) * time.Second),
		})
	}

	buf := bytes.NewBuffer(make([]byte, 0, 1024))

	wp := newWriterParquet(config.New())
	err := wp.Write(buf, payloads)
	require.NoError(t, err)

	pr, err := reader.NewParquetReader(buffer.NewBufferFileFromBytes(buf.Bytes()), new(payloadParquet), 8)
	require.NoError(t, err)
	require.Equal(t, int64(100), pr.GetNumRows())

	for i := 0; i < int(pr.GetNumRows())/10; i++ {
		payloads := make([]payloadParquet, 10)

		err = pr.Read(&payloads)
		require.NoError(t, err)

		for j := 0; j < 10; j++ {
			p := payloads[j]

			require.Equal(t, "messageId"+strconv.Itoa(i*10+j), p.MessageID)
			require.Equal(t, "sourceId"+strconv.Itoa(i*10+j), p.SourceID)
			require.Equal(t, "destinationId"+strconv.Itoa(i*10+j), p.DestinationID)
			require.Equal(t, "transformationId"+strconv.Itoa(i*10+j), p.TransformationID)
			require.Equal(t, "trackingPlanId"+strconv.Itoa(i*10+j), p.TrackingPlanID)
			require.Equal(t, "failedStage"+strconv.Itoa(i*10+j), p.FailedStage)
			require.Equal(t, "eventType"+strconv.Itoa(i*10+j), p.EventType)
			require.Equal(t, "eventName"+strconv.Itoa(i*10+j), p.EventName)
			require.Equal(t, now.Add(time.Duration(i*10+j)*time.Second).UTC(), types.TIMESTAMP_MICROSToTime(p.ReceivedAt, false).UTC())
			require.Equal(t, now.Add(time.Duration(i*10+j)*time.Second).UTC(), types.TIMESTAMP_MICROSToTime(p.ReceivedAt, false).UTC())
		}
	}
}

// TOOD: Fix this test
func BenchmarkWriteParquet(b *testing.B) {
	now := time.Now()

	var payloads []payload
	for i := 0; i < 100; i++ {
		payloads = append(payloads, payload{
			MessageID:        "messageId" + strconv.Itoa(i),
			SourceID:         "sourceId" + strconv.Itoa(i),
			DestinationID:    "destinationId" + strconv.Itoa(i),
			TransformationID: "transformationId" + strconv.Itoa(i),
			TrackingPlanID:   "trackingPlanId" + strconv.Itoa(i),
			FailedStage:      "failedStage" + strconv.Itoa(i),
			EventType:        "eventType" + strconv.Itoa(i),
			EventName:        "eventName" + strconv.Itoa(i),
			ReceivedAt:       now.Add(time.Duration(i) * time.Second),
			FailedAt:         now.Add(time.Duration(i) * time.Second),
		})
	}

	buf := bytes.NewBuffer(make([]byte, 0, 1024))

	wp := newWriterParquet(config.New())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := wp.Write(buf, payloads)
		require.NoError(b, err)
	}
}
