package reporting

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"path"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/xitongsys/parquet-go-source/buffer"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/types"

	"github.com/rudderlabs/rudder-go-kit/config"

	_ "github.com/marcboeker/go-duckdb"
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
		expectedPayloads := make([]payloadParquet, 10)

		err = pr.Read(&expectedPayloads)
		require.NoError(t, err)

		for j, payload := range expectedPayloads {
			require.Equal(t, payloads[i*10+j].MessageID, payload.MessageID)
			require.Equal(t, payloads[i*10+j].SourceID, payload.SourceID)
			require.Equal(t, payloads[i*10+j].DestinationID, payload.DestinationID)
			require.Equal(t, payloads[i*10+j].TransformationID, payload.TransformationID)
			require.Equal(t, payloads[i*10+j].TrackingPlanID, payload.TrackingPlanID)
			require.Equal(t, payloads[i*10+j].FailedStage, payload.FailedStage)
			require.Equal(t, payloads[i*10+j].EventType, payload.EventType)
			require.Equal(t, payloads[i*10+j].EventName, payload.EventName)
			require.EqualValues(t, payloads[i*10+j].ReceivedAt.UTC(), types.TIMESTAMP_MICROSToTime(payload.ReceivedAt, true).UTC())
			require.EqualValues(t, payloads[i*10+j].FailedAt.UTC(), types.TIMESTAMP_MICROSToTime(payload.FailedAt, true).UTC())
		}
	}
}

func TestFilters(t *testing.T) {
	now := time.Now()

	var payloads []payload
	for i := 0; i < 100; i++ {
		payloads = append(payloads, payload{
			MessageID:        "messageId" + strconv.Itoa(i),
			SourceID:         "sourceId" + strconv.Itoa(i%5),
			DestinationID:    "destinationId" + strconv.Itoa(i%10),
			TransformationID: "transformationId" + strconv.Itoa(i),
			TrackingPlanID:   "trackingPlanId" + strconv.Itoa(i),
			FailedStage:      "failedStage" + strconv.Itoa(i),
			EventType:        "eventType" + strconv.Itoa(i),
			EventName:        "eventName" + strconv.Itoa(i),
			ReceivedAt:       now.Add(time.Duration(i) * time.Second),
			FailedAt:         now.Add(time.Duration(i) * time.Second),
		})
	}

	ctx := context.Background()

	tmpDir := t.TempDir()
	filePath := path.Join(tmpDir, "payloads.parquet")

	fw, err := local.NewLocalFileWriter(filePath)
	require.NoError(t, err)

	wp := newWriterParquet(config.New())
	err = wp.Write(fw, payloads)
	require.NoError(t, err)

	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)

	_, err = db.Exec("INSTALL parquet;")
	require.NoError(t, err)

	_, err = db.Exec("LOAD parquet;")
	require.NoError(t, err)

	t.Run("count(*)", func(t *testing.T) {
		var count int64
		err = db.QueryRowContext(ctx, fmt.Sprintf("SELECT count(*) FROM read_parquet('%s');", filePath)).Scan(&count)
		require.NoError(t, err)
		require.Equal(t, int64(100), count)
	})
	t.Run("count(*) for sourceId", func(t *testing.T) {
		var count int64
		err = db.QueryRowContext(ctx, fmt.Sprintf("SELECT count(*) FROM read_parquet('%s') WHERE sourceId = $1;", filePath), "sourceId3").Scan(&count)
		require.NoError(t, err)
		require.Equal(t, int64(20), count)
	})
	t.Run("count(*) for sourceId", func(t *testing.T) {
		var count int64
		err = db.QueryRowContext(ctx, fmt.Sprintf("SELECT count(*) FROM read_parquet('%s') WHERE destinationId = $1;", filePath), "destinationId3").Scan(&count)
		require.NoError(t, err)
		require.Equal(t, int64(10), count)
	})
	t.Run("select all", func(t *testing.T) {
		var expectedPayloads []payload

		rows, err := db.QueryContext(ctx, fmt.Sprintf("SELECT * FROM read_parquet('%s');", filePath))
		require.NoError(t, err)
		defer func() { _ = rows.Close() }()

		for rows.Next() {
			var p payload

			err := rows.Scan(&p.MessageID, &p.SourceID, &p.DestinationID, &p.TransformationID, &p.TrackingPlanID, &p.FailedStage, &p.EventType, &p.EventName, &p.ReceivedAt, &p.FailedAt)
			require.NoError(t, err)

			expectedPayloads = append(expectedPayloads, p)
		}
		require.NoError(t, rows.Err())

		for i, payload := range expectedPayloads {
			require.Equal(t, payloads[i].MessageID, payload.MessageID)
			require.Equal(t, payloads[i].SourceID, payload.SourceID)
			require.Equal(t, payloads[i].DestinationID, payload.DestinationID)
			require.Equal(t, payloads[i].TransformationID, payload.TransformationID)
			require.Equal(t, payloads[i].TrackingPlanID, payload.TrackingPlanID)
			require.Equal(t, payloads[i].FailedStage, payload.FailedStage)
			require.Equal(t, payloads[i].EventType, payload.EventType)
			require.Equal(t, payloads[i].EventName, payload.EventName)
			require.EqualValues(t, payloads[i].ReceivedAt.UTC(), payload.ReceivedAt.UTC())
			require.EqualValues(t, payloads[i].FailedAt.UTC(), payload.FailedAt.UTC())
		}
	})
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
