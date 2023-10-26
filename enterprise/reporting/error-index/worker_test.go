package error_index

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/rudderlabs/rudder-go-kit/bytesize"

	"github.com/google/uuid"
	"github.com/ory/dockertest/v3"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
	"github.com/xitongsys/parquet-go-source/buffer"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/testhelper/destination"
	"github.com/rudderlabs/rudder-server/utils/misc"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"

	_ "github.com/marcboeker/go-duckdb"
)

func TestWorkerWriter(t *testing.T) {
	sourceID := "test-source-id"
	workspaceID := "test-workspace-id"
	instanceID := "test-instance-id"

	ctx := context.Background()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	t.Run("writer", func(t *testing.T) {
		receivedAt := time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC)
		failedAt := receivedAt.Add(time.Hour)

		count := 100
		eventPayloads := make([]payload, 0, count)

		for i := 0; i < count; i++ {
			eventPayloads = append(eventPayloads, payload{
				MessageID:        "messageId" + strconv.Itoa(i),
				SourceID:         "sourceId" + strconv.Itoa(i%5),
				DestinationID:    "destinationId" + strconv.Itoa(i%10),
				TransformationID: "transformationId" + strconv.Itoa(i),
				TrackingPlanID:   "trackingPlanId" + strconv.Itoa(i),
				FailedStage:      "failedStage" + strconv.Itoa(i),
				EventType:        "eventType" + strconv.Itoa(i),
				EventName:        "eventName" + strconv.Itoa(i),
				ReceivedAt:       receivedAt.Add(time.Duration(i) * time.Second).UnixMilli(),
				FailedAt:         failedAt.Add(time.Duration(i) * time.Second).UnixMilli(),
			})
		}

		t.Run("writes", func(t *testing.T) {
			buf := bytes.NewBuffer(make([]byte, 0, 1024))

			w := worker{}
			w.config.parquetRowGroupSize = misc.SingleValueLoader(512 * bytesize.MB)
			w.config.parquetPageSize = misc.SingleValueLoader(8 * bytesize.KB)
			w.config.parquetParallelWriters = misc.SingleValueLoader(int64(8))

			require.NoError(t, w.write(buf, eventPayloads))

			pr, err := reader.NewParquetReader(buffer.NewBufferFileFromBytes(buf.Bytes()), new(payload), 8)
			require.NoError(t, err)
			require.EqualValues(t, len(eventPayloads), pr.GetNumRows())

			factor := 10

			for i := 0; i < int(pr.GetNumRows())/factor; i++ {
				expectedPayloads := make([]payload, factor)

				err = pr.Read(&expectedPayloads)
				require.NoError(t, err)

				for j, expectedPayload := range expectedPayloads {
					require.Equal(t, eventPayloads[i*factor+j].MessageID, expectedPayload.MessageID)
					require.Equal(t, eventPayloads[i*factor+j].SourceID, expectedPayload.SourceID)
					require.Equal(t, eventPayloads[i*factor+j].DestinationID, expectedPayload.DestinationID)
					require.Equal(t, eventPayloads[i*factor+j].TransformationID, expectedPayload.TransformationID)
					require.Equal(t, eventPayloads[i*factor+j].TrackingPlanID, expectedPayload.TrackingPlanID)
					require.Equal(t, eventPayloads[i*factor+j].FailedStage, expectedPayload.FailedStage)
					require.Equal(t, eventPayloads[i*factor+j].EventType, expectedPayload.EventType)
					require.Equal(t, eventPayloads[i*factor+j].EventName, expectedPayload.EventName)
					require.EqualValues(t, eventPayloads[i*factor+j].ReceivedAt, expectedPayload.ReceivedAt)
					require.EqualValues(t, eventPayloads[i*factor+j].FailedAt, expectedPayload.FailedAt)
				}
			}
		})

		t.Run("filters", func(t *testing.T) {
			tmpDir := t.TempDir()
			filePath := path.Join(tmpDir, "payloads.parquet")
			t.Cleanup(func() {
				_ = os.Remove(filePath)
			})

			fw, err := local.NewLocalFileWriter(filePath)
			require.NoError(t, err)

			w := worker{}
			w.config.parquetRowGroupSize = misc.SingleValueLoader(512 * bytesize.MB)
			w.config.parquetPageSize = misc.SingleValueLoader(8 * bytesize.KB)
			w.config.parquetParallelWriters = misc.SingleValueLoader(int64(8))

			require.NoError(t, w.write(fw, eventPayloads))

			db, err := sql.Open("duckdb", "")
			require.NoError(t, err)
			defer func() { _ = db.Close() }()

			_, err = db.Exec(`INSTALL parquet; LOAD parquet;`)
			require.NoError(t, err)

			t.Run("count all", func(t *testing.T) {
				var count int64
				err = db.QueryRowContext(ctx, fmt.Sprintf("SELECT count(*) FROM read_parquet('%s');", filePath)).Scan(&count)
				require.NoError(t, err)
				require.EqualValues(t, len(eventPayloads), count)
			})
			t.Run("count for sourceId, destinationId", func(t *testing.T) {
				var count int64
				err = db.QueryRowContext(ctx, fmt.Sprintf("SELECT count(*) FROM read_parquet('%s') WHERE source_id = $1 AND destination_id = $2;", filePath), "sourceId3", "destinationId3").Scan(&count)
				require.NoError(t, err)
				require.EqualValues(t, 10, count)
			})
			t.Run("select all", func(t *testing.T) {
				var expectedPayloads []payload

				rows, err := db.QueryContext(ctx, fmt.Sprintf("SELECT * FROM read_parquet('%s') ORDER BY failed_at ASC;", filePath))
				require.NoError(t, err)
				defer func() { _ = rows.Close() }()

				for rows.Next() {
					var p payload

					require.NoError(t, rows.Scan(
						&p.MessageID, &p.SourceID, &p.DestinationID,
						&p.TransformationID, &p.TrackingPlanID, &p.FailedStage,
						&p.EventType, &p.EventName, &p.ReceivedAt,
						&p.FailedAt,
					))

					expectedPayloads = append(expectedPayloads, p)
				}
				require.NoError(t, rows.Err())

				for i, expectedPayload := range expectedPayloads {
					require.Equal(t, expectedPayloads[i].MessageID, expectedPayload.MessageID)
					require.Equal(t, expectedPayloads[i].SourceID, expectedPayload.SourceID)
					require.Equal(t, expectedPayloads[i].DestinationID, expectedPayload.DestinationID)
					require.Equal(t, expectedPayloads[i].TransformationID, expectedPayload.TransformationID)
					require.Equal(t, expectedPayloads[i].TrackingPlanID, expectedPayload.TrackingPlanID)
					require.Equal(t, expectedPayloads[i].FailedStage, expectedPayload.FailedStage)
					require.Equal(t, expectedPayloads[i].EventType, expectedPayload.EventType)
					require.Equal(t, expectedPayloads[i].EventName, expectedPayload.EventName)
					require.EqualValues(t, expectedPayloads[i].ReceivedAt, expectedPayload.ReceivedAt)
					require.EqualValues(t, expectedPayloads[i].FailedAt, expectedPayload.FailedAt)
				}
			})
		})
	})

	t.Run("workers work", func(t *testing.T) {
		t.Run("same hours", func(t *testing.T) {
			receivedAt, failedAt := time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC)

			postgresContainer, err := resource.SetupPostgres(pool, t)
			require.NoError(t, err)
			minioResource, err := destination.SetupMINIO(pool, t)
			require.NoError(t, err)

			c := config.New()
			c.Set("INSTANCE_ID", instanceID)

			errIndexDB := jobsdb.NewForReadWrite("err_idx", jobsdb.WithDBHandle(postgresContainer.DB), jobsdb.WithConfig(c))
			require.NoError(t, errIndexDB.Start())
			defer errIndexDB.TearDown()

			count := 100
			eventPayloads := make([]payload, 0, count)
			jobsList := make([]*jobsdb.JobT, 0, count)

			for i := 0; i < count; i++ {
				eventPayload := prepareEventPayload(i, sourceID, receivedAt, failedAt.Add(time.Duration(i)*time.Second))
				eventPayloads = append(eventPayloads, eventPayload)

				epJSON, err := json.Marshal(eventPayload)
				require.NoError(t, err)

				jobsList = append(jobsList, &jobsdb.JobT{
					UUID:         uuid.New(),
					Parameters:   []byte(`{"source_id":"` + sourceID + `","workspaceId":"` + workspaceID + `"}`),
					EventPayload: epJSON,
					EventCount:   1,
					WorkspaceId:  workspaceID,
				})
			}

			require.NoError(t, errIndexDB.Store(ctx, jobsList))

			cs := newMockConfigFetcher()
			cs.addWorkspaceIDForSourceID(sourceID, workspaceID)

			statsStore := memstats.New()

			fm, err := filemanager.New(&filemanager.Settings{
				Provider: warehouseutils.MINIO,
				Config: map[string]any{
					"bucketName":      minioResource.BucketName,
					"accessKeyID":     minioResource.AccessKey,
					"secretAccessKey": minioResource.SecretKey,
					"endPoint":        minioResource.Endpoint,
				},
			})
			require.NoError(t, err)

			w := newWorker(sourceID, c, logger.NOP, statsStore, errIndexDB, cs, fm)
			defer w.Stop()

			require.True(t, w.Work())
			require.EqualValues(t, len(jobsList), statsStore.Get("erridx_uploaded_jobs", stats.Tags{
				"workspaceId": w.workspaceID,
				"sourceId":    w.sourceID,
			}).LastValue())
			require.EqualValues(t, len(jobsList), statsStore.Get("erridx_processed_jobs", stats.Tags{
				"workspaceId": w.workspaceID,
				"sourceId":    w.sourceID,
				"state":       jobsdb.Succeeded.State,
			}).LastValue())
			require.False(t, w.Work())

			lastFailedAt := failedAt.Add(time.Duration(len(jobsList)-1) * time.Second)
			filePath := fmt.Sprintf("s3://%s/%s/%s/%s/%d_%d_%s.parquet",
				minioResource.BucketName,
				w.sourceID,
				failedAt.Format("2006-01-02"),
				strconv.Itoa(failedAt.Hour()),
				failedAt.Unix(),
				lastFailedAt.Unix(),
				instanceID,
			)
			expectedPayloads := failedMessages(t, ctx, minioResource, filePath)
			require.Len(t, expectedPayloads, len(jobsList))
			require.EqualValues(t, eventPayloads, expectedPayloads)

			jr, err := errIndexDB.GetSucceeded(ctx, jobsdb.GetQueryParams{
				ParameterFilters: []jobsdb.ParameterFilterT{
					{Name: "source_id", Value: w.sourceID},
				},
				PayloadSizeLimit: w.config.payloadLimit.Load(),
				EventsLimit:      int(w.config.eventsLimit.Load()),
				JobsLimit:        int(w.config.eventsLimit.Load()),
			})
			require.NoError(t, err)
			require.Len(t, jr.Jobs, len(jobsList))

			lo.ForEach(jr.Jobs, func(item *jobsdb.JobT, index int) {
				require.EqualValues(t, string(item.LastJobStatus.ErrorResponse), fmt.Sprintf(`{"location": "%s"}`, strings.Replace(filePath, "s3://", fmt.Sprintf("http://%s/", minioResource.Endpoint), 1)))
			})
		})
		t.Run("multiple hours and days", func(t *testing.T) {
			receivedAt, failedAt := time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC)

			postgresContainer, err := resource.SetupPostgres(pool, t)
			require.NoError(t, err)
			minioResource, err := destination.SetupMINIO(pool, t)
			require.NoError(t, err)

			c := config.New()
			c.Set("INSTANCE_ID", instanceID)

			errIndexDB := jobsdb.NewForReadWrite("err_idx", jobsdb.WithDBHandle(postgresContainer.DB), jobsdb.WithConfig(c))
			require.NoError(t, errIndexDB.Start())
			defer errIndexDB.TearDown()

			count := 100
			eventPayloads := make([]payload, 0, count)
			jobsList := make([]*jobsdb.JobT, 0, count)

			for i := 0; i < count; i++ {
				eventPayload := prepareEventPayload(i, sourceID, receivedAt, failedAt.Add(time.Duration(i)*time.Hour))
				eventPayloads = append(eventPayloads, eventPayload)

				epJSON, err := json.Marshal(eventPayload)
				require.NoError(t, err)

				jobsList = append(jobsList, &jobsdb.JobT{
					UUID:         uuid.New(),
					Parameters:   []byte(`{"source_id":"` + sourceID + `","workspaceId":"` + workspaceID + `"}`),
					EventPayload: epJSON,
					EventCount:   1,
					WorkspaceId:  workspaceID,
				})
			}

			require.NoError(t, errIndexDB.Store(ctx, jobsList))

			cs := newMockConfigFetcher()
			cs.addWorkspaceIDForSourceID(sourceID, workspaceID)

			statsStore := memstats.New()

			fm, err := filemanager.New(&filemanager.Settings{
				Provider: warehouseutils.MINIO,
				Config: map[string]any{
					"bucketName":      minioResource.BucketName,
					"accessKeyID":     minioResource.AccessKey,
					"secretAccessKey": minioResource.SecretKey,
					"endPoint":        minioResource.Endpoint,
				},
			})
			require.NoError(t, err)

			w := newWorker(sourceID, c, logger.NOP, statsStore, errIndexDB, cs, fm)
			defer w.Stop()
			require.True(t, w.Work())

			for i := 0; i < count; i++ {
				failedAt := failedAt.Add(time.Duration(i) * time.Hour)
				expectedPayloads := failedMessages(t, ctx, minioResource, fmt.Sprintf("s3://%s/%s/%s/%s/%d_%d_%s.parquet",
					minioResource.BucketName,
					w.sourceID,
					failedAt.Format("2006-01-02"),
					strconv.Itoa(failedAt.Hour()),
					failedAt.Unix(),
					failedAt.Unix(),
					instanceID,
				))
				require.EqualValues(t, []payload{eventPayloads[i]}, expectedPayloads)
			}

			jr, err := errIndexDB.GetSucceeded(ctx, jobsdb.GetQueryParams{
				ParameterFilters: []jobsdb.ParameterFilterT{
					{Name: "source_id", Value: w.sourceID},
				},
				PayloadSizeLimit: w.config.payloadLimit.Load(),
				EventsLimit:      int(w.config.eventsLimit.Load()),
				JobsLimit:        int(w.config.eventsLimit.Load()),
			})
			require.NoError(t, err)
			require.Len(t, jr.Jobs, len(jobsList))

			lo.ForEach(jr.Jobs, func(item *jobsdb.JobT, index int) {
				failedAt := failedAt.Add(time.Duration(index) * time.Hour)
				filePath := fmt.Sprintf("http://%s/%s/%s/%s/%s/%d_%d_%s.parquet",
					minioResource.Endpoint,
					minioResource.BucketName,
					w.sourceID,
					failedAt.Format("2006-01-02"),
					strconv.Itoa(failedAt.Hour()),
					failedAt.Unix(),
					failedAt.Unix(),
					instanceID,
				)

				require.EqualValues(t, string(item.LastJobStatus.ErrorResponse), fmt.Sprintf(`{"location": "%s"}`, strings.Replace(filePath, "s3://", fmt.Sprintf("http://%s/", minioResource.Endpoint), 1)))
			})
		})
		t.Run("limits reached but few left without crossing upload frequency", func(t *testing.T) {
			receivedAt, failedAt := time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC)

			postgresContainer, err := resource.SetupPostgres(pool, t)
			require.NoError(t, err)
			minioResource, err := destination.SetupMINIO(pool, t)
			require.NoError(t, err)

			eventsLimit := 24

			c := config.New()
			c.Set("INSTANCE_ID", instanceID)
			c.Set("Reporting.errorIndexReporting.minWorkerSleep", "1s")
			c.Set("Reporting.errorIndexReporting.uploadFrequency", "600s")
			c.Set("Reporting.errorIndexReporting.eventsLimit", strconv.Itoa(eventsLimit))

			errIndexDB := jobsdb.NewForReadWrite("err_idx", jobsdb.WithDBHandle(postgresContainer.DB), jobsdb.WithConfig(c))
			require.NoError(t, errIndexDB.Start())
			defer errIndexDB.TearDown()

			count := 100
			eventPayloads := make([]payload, 0, count)
			jobsList := make([]*jobsdb.JobT, 0, count)

			for i := 0; i < count; i++ {
				eventPayload := prepareEventPayload(i, sourceID, receivedAt, failedAt.Add(time.Duration(i)*time.Second))
				eventPayloads = append(eventPayloads, eventPayload)

				epJSON, err := json.Marshal(eventPayload)
				require.NoError(t, err)

				jobsList = append(jobsList, &jobsdb.JobT{
					UUID:         uuid.New(),
					Parameters:   []byte(`{"source_id":"` + sourceID + `","workspaceId":"` + workspaceID + `"}`),
					EventPayload: epJSON,
					EventCount:   1,
					WorkspaceId:  workspaceID,
				})
			}
			require.NoError(t, errIndexDB.Store(ctx, jobsList))

			cs := newMockConfigFetcher()
			cs.addWorkspaceIDForSourceID(sourceID, workspaceID)

			statsStore := memstats.New()

			fm, err := filemanager.New(&filemanager.Settings{
				Provider: warehouseutils.MINIO,
				Config: map[string]any{
					"bucketName":      minioResource.BucketName,
					"accessKeyID":     minioResource.AccessKey,
					"secretAccessKey": minioResource.SecretKey,
					"endPoint":        minioResource.Endpoint,
				},
			})
			require.NoError(t, err)

			w := newWorker(sourceID, c, logger.NOP, statsStore, errIndexDB, cs, fm)
			defer w.Stop()

			for i := 0; i < count/eventsLimit; i++ {
				require.True(t, w.Work())
			}
			require.False(t, w.Work())

			jr, err := errIndexDB.GetUnprocessed(ctx, jobsdb.GetQueryParams{
				ParameterFilters: []jobsdb.ParameterFilterT{
					{Name: "source_id", Value: w.sourceID},
				},
				PayloadSizeLimit: w.config.payloadLimit.Load(),
				EventsLimit:      int(w.config.eventsLimit.Load()),
				JobsLimit:        int(w.config.eventsLimit.Load()),
			})
			require.NoError(t, err)
			require.Len(t, jr.Jobs, 4)
		})
	})
}

func failedMessages(t testing.TB, ctx context.Context, mr *destination.MINIOResource, filePath string) []payload {
	t.Helper()

	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	_, err = db.Exec(fmt.Sprintf(`INSTALL parquet; LOAD parquet; INSTALL httpfs; LOAD httpfs;SET s3_region='%s';SET s3_endpoint='%s';SET s3_access_key_id='%s';SET s3_secret_access_key='%s';SET s3_use_ssl= false;SET s3_url_style='path';`,
		mr.SiteRegion,
		mr.Endpoint,
		mr.AccessKey,
		mr.SecretKey,
	))
	require.NoError(t, err)

	rows, err := db.QueryContext(ctx, fmt.Sprintf("SELECT * FROM read_parquet('%s') ORDER BY failed_at ASC;", filePath))
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()

	var expectedPayloads []payload
	for rows.Next() {
		var p payload
		require.NoError(t, rows.Scan(
			&p.MessageID, &p.SourceID, &p.DestinationID,
			&p.TransformationID, &p.TrackingPlanID, &p.FailedStage,
			&p.EventType, &p.EventName, &p.ReceivedAt,
			&p.FailedAt,
		))
		expectedPayloads = append(expectedPayloads, p)
	}
	require.NoError(t, rows.Err())

	return expectedPayloads
}

func prepareEventPayload(i int, sourceID string, receivedAt, failedAt time.Time) payload {
	return payload{
		MessageID:        "message-id-" + strconv.Itoa(i),
		SourceID:         sourceID,
		DestinationID:    "destination-id-" + strconv.Itoa(i),
		TransformationID: "transformation-id-" + strconv.Itoa(i),
		TrackingPlanID:   "tracking-plan-id-" + strconv.Itoa(i),
		FailedStage:      "failed-stage-" + strconv.Itoa(i),
		EventType:        "event-type-" + strconv.Itoa(i),
		EventName:        "event-name-" + strconv.Itoa(i),
		ReceivedAt:       receivedAt.UnixMilli(),
		FailedAt:         failedAt.UnixMilli(),
	}
}

func BenchmarkFileFormat(b *testing.B) {
	now := time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC)

	entries := 1000000

	b.Run("csv", func(b *testing.B) {
		var records [][]string

		for i := 0; i < entries; i++ {
			record := make([]string, 0, 10)
			record = append(record, "messageId"+strconv.Itoa(i))
			record = append(record, "sourceId")
			record = append(record, "destinationId"+strconv.Itoa(i%10))
			record = append(record, "transformationId"+strconv.Itoa(i%10))
			record = append(record, "trackingPlanId"+strconv.Itoa(i%10))
			record = append(record, "failedStage"+strconv.Itoa(i%10))
			record = append(record, "eventType"+strconv.Itoa(i%10))
			record = append(record, "eventName"+strconv.Itoa(i%10))
			record = append(record, now.Add(time.Duration(i)*time.Second).Format(time.RFC3339))
			record = append(record, now.Add(time.Duration(i)*time.Second).Format(time.RFC3339))

			records = append(records, record)
		}

		buf := bytes.NewBuffer(make([]byte, 0, 1024))
		c := csv.NewWriter(buf)

		err := c.WriteAll(records)
		require.NoError(b, err)

		b.Log("csv size:", buf.Len()) // csv size: 150MB
	})
	b.Run("json", func(b *testing.B) {
		var records []payload

		for i := 0; i < entries; i++ {
			records = append(records, payload{
				MessageID:        "messageId" + strconv.Itoa(i),
				SourceID:         "sourceId",
				DestinationID:    "destinationId" + strconv.Itoa(i%10),
				TransformationID: "transformationId" + strconv.Itoa(i%10),
				TrackingPlanID:   "trackingPlanId" + strconv.Itoa(i%10),
				FailedStage:      "failedStage" + strconv.Itoa(i%10),
				EventType:        "eventType" + strconv.Itoa(i%10),
				EventName:        "eventName" + strconv.Itoa(i%10),
				ReceivedAt:       now.Add(time.Duration(i) * time.Second).UnixMilli(),
				FailedAt:         now.Add(time.Duration(i) * time.Second).UnixMilli(),
			})
		}

		buf := bytes.NewBuffer(make([]byte, 0, 1024))
		e := json.NewEncoder(buf)

		for _, record := range records {
			require.NoError(b, e.Encode(record))
		}

		b.Log("json size:", buf.Len()) // json size: 310MB
	})
	b.Run("parquet", func(b *testing.B) {
		var records []payload

		for i := 0; i < entries; i++ {
			records = append(records, payload{
				MessageID:        "messageId" + strconv.Itoa(i),
				SourceID:         "sourceId",
				DestinationID:    "destinationId" + strconv.Itoa(i%10),
				TransformationID: "transformationId" + strconv.Itoa(i%10),
				TrackingPlanID:   "trackingPlanId" + strconv.Itoa(i%10),
				FailedStage:      "failedStage" + strconv.Itoa(i%10),
				EventType:        "eventType" + strconv.Itoa(i%10),
				EventName:        "eventName" + strconv.Itoa(i%10),
				ReceivedAt:       now.Add(time.Duration(i) * time.Second).UnixMilli(),
				FailedAt:         now.Add(time.Duration(i) * time.Second).UnixMilli(),
			})
		}

		w := worker{}
		w.config.parquetRowGroupSize = misc.SingleValueLoader(512 * bytesize.MB)
		w.config.parquetPageSize = misc.SingleValueLoader(8 * bytesize.KB)
		w.config.parquetParallelWriters = misc.SingleValueLoader(int64(8))

		buf := bytes.NewBuffer(make([]byte, 0, 1024))

		require.NoError(b, w.write(buf, records))

		b.Log("parquet size:", buf.Len()) // parquet size: 18.67MB

		// parquet size: 10.39MB (rowGroupSizeInMB=512, pageSizeInKB=8, compression=snappy, encoding=[RLE_DICTIONARY,DELTA_BINARY_PACKED], No sorting)
		// parquet size: 18.67MB (rowGroupSizeInMB=512, pageSizeInKB=8, compression=snappy, encoding=[RLE_DICTIONARY,DELTA_BINARY_PACKED])
		// parquet size: 18.67MB (rowGroupSizeInMB=1024, pageSizeInKB=8, compression=snappy, encoding=[RLE_DICTIONARY,DELTA_BINARY_PACKED])
		// parquet size: 18.67MB (rowGroupSizeInMB=128, pageSizeInKB=8, compression=snappy, encoding=[RLE_DICTIONARY,DELTA_BINARY_PACKED])
		// parquet size: 21.27MB (rowGroupSizeInMB=512, pageSizeInKB=8, compression=snappy, encoding=[RLE_DICTIONARY])
		// parquet size: 22.65MB (rowGroupSizeInMB=512, pageSizeInKB=8, compression=snappy, encoding=[])
	})
}
