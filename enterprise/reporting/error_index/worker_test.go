package error_index

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/marcboeker/go-duckdb"
	miniogo "github.com/minio/minio-go/v7"
	"github.com/ory/dockertest/v3"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"github.com/xitongsys/parquet-go-source/buffer"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"

	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"
	kitsync "github.com/rudderlabs/rudder-go-kit/sync"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/minio"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"

	"github.com/rudderlabs/rudder-server/jobsdb"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestWorkerWriter(t *testing.T) {
	const (
		sourceID    = "test-source-id"
		workspaceID = "test-workspace-id"
		instanceID  = "test-instance-id"
	)

	ctx := context.Background()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	t.Run("writer", func(t *testing.T) {
		receivedAt := time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC)
		failedAt := receivedAt.Add(time.Hour)

		count := 100
		factor := 10
		payloads := make([]payload, 0, count)

		for i := 0; i < count; i++ {
			p := payload{
				MessageID:        "messageId" + strconv.Itoa(i),
				SourceID:         "sourceId" + strconv.Itoa(i%5),
				DestinationID:    "destinationId" + strconv.Itoa(i%10),
				TransformationID: "transformationId" + strconv.Itoa(i),
				TrackingPlanID:   "trackingPlanId" + strconv.Itoa(i),
				FailedStage:      "failedStage" + strconv.Itoa(i),
				EventType:        "eventType" + strconv.Itoa(i),
				EventName:        "eventName" + strconv.Itoa(i),
			}
			p.SetReceivedAt(receivedAt.Add(time.Duration(i) * time.Second))
			p.SetFailedAt(failedAt.Add(time.Duration(i) * time.Second))

			payloads = append(payloads, p)
		}
		toEncode := make([]payload, len(payloads))
		copy(toEncode, payloads)

		t.Run("writes", func(t *testing.T) {
			buf := bytes.NewBuffer(make([]byte, 0, 1024))

			w := worker{}
			w.config.parquetRowGroupSize = config.SingleValueLoader(512 * bytesize.MB)
			w.config.parquetPageSize = config.SingleValueLoader(8 * bytesize.KB)
			w.config.parquetParallelWriters = config.SingleValueLoader(int64(8))

			require.NoError(t, w.encodeToParquet(buf, toEncode))

			pr, err := reader.NewParquetReader(buffer.NewBufferFileFromBytes(buf.Bytes()), new(payload), 8)
			require.NoError(t, err)
			require.EqualValues(t, len(payloads), pr.GetNumRows())

			for i := 0; i < int(pr.GetNumRows())/factor; i++ {
				expectedPayloads := make([]payload, factor)

				err := pr.Read(&expectedPayloads)
				require.NoError(t, err)

				for j, expectedPayload := range expectedPayloads {
					require.Equal(t, payloads[i*factor+j].MessageID, expectedPayload.MessageID)
					require.Equal(t, payloads[i*factor+j].SourceID, expectedPayload.SourceID)
					require.Equal(t, payloads[i*factor+j].DestinationID, expectedPayload.DestinationID)
					require.Equal(t, payloads[i*factor+j].TransformationID, expectedPayload.TransformationID)
					require.Equal(t, payloads[i*factor+j].TrackingPlanID, expectedPayload.TrackingPlanID)
					require.Equal(t, payloads[i*factor+j].FailedStage, expectedPayload.FailedStage)
					require.Equal(t, payloads[i*factor+j].EventType, expectedPayload.EventType)
					require.Equal(t, payloads[i*factor+j].EventName, expectedPayload.EventName)
					require.Equal(t, payloads[i*factor+j].ReceivedAt, expectedPayload.ReceivedAt)
					require.Equal(t, payloads[i*factor+j].FailedAt, expectedPayload.FailedAt)
				}
			}
		})

		t.Run("filters", func(t *testing.T) {
			filePath := path.Join(t.TempDir(), "payloads.parquet")
			t.Cleanup(func() {
				_ = os.Remove(filePath)
			})

			fw, err := local.NewLocalFileWriter(filePath)
			require.NoError(t, err)

			w := worker{}
			w.config.parquetRowGroupSize = config.SingleValueLoader(512 * bytesize.MB)
			w.config.parquetPageSize = config.SingleValueLoader(8 * bytesize.KB)
			w.config.parquetParallelWriters = config.SingleValueLoader(int64(8))

			require.NoError(t, w.encodeToParquet(fw, toEncode))

			t.Run("count all", func(t *testing.T) {
				var count int64
				err := duckDB(t).QueryRowContext(ctx, "SELECT count(*) FROM read_parquet($1);", filePath).Scan(&count)
				require.NoError(t, err)
				require.EqualValues(t, len(payloads), count)
			})
			t.Run("count for sourceId, destinationId", func(t *testing.T) {
				var count int64
				err := duckDB(t).QueryRowContext(ctx, "SELECT count(*) FROM read_parquet($1) WHERE source_id = $2 AND destination_id = $3;", filePath, "sourceId3", "destinationId3").Scan(&count)
				require.NoError(t, err)
				require.EqualValues(t, 10, count)
			})
			t.Run("select all", func(t *testing.T) {
				failedMessages := failedMessagesUsingDuckDB(t, ctx, nil, "SELECT * FROM read_parquet($1) ORDER BY failed_at ASC;", []interface{}{filePath})

				for i, failedMessage := range failedMessages {
					require.Equal(t, payloads[i].MessageID, failedMessage.MessageID)
					require.Equal(t, payloads[i].SourceID, failedMessage.SourceID)
					require.Equal(t, payloads[i].DestinationID, failedMessage.DestinationID)
					require.Equal(t, payloads[i].TransformationID, failedMessage.TransformationID)
					require.Equal(t, payloads[i].TrackingPlanID, failedMessage.TrackingPlanID)
					require.Equal(t, payloads[i].FailedStage, failedMessage.FailedStage)
					require.Equal(t, payloads[i].EventType, failedMessage.EventType)
					require.Equal(t, payloads[i].EventName, failedMessage.EventName)
					require.Equal(t, payloads[i].ReceivedAt, failedMessage.ReceivedAt)
					require.Equal(t, payloads[i].FailedAt, failedMessage.FailedAt)
				}
			})
		})
	})

	t.Run("workers work", func(t *testing.T) {
		t.Run("same hours", func(t *testing.T) {
			receivedAt := time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC)
			failedAt := time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC)

			postgresContainer, err := postgres.Setup(pool, t)
			require.NoError(t, err)
			minioResource, err := minio.Setup(pool, t)
			require.NoError(t, err)

			c := config.New()
			c.Set("INSTANCE_ID", instanceID)

			errIndexDB := jobsdb.NewForReadWrite("err_idx", jobsdb.WithDBHandle(postgresContainer.DB), jobsdb.WithConfig(c), jobsdb.WithStats(stats.NOP))
			require.NoError(t, errIndexDB.Start())
			defer errIndexDB.TearDown()

			count := 100
			payloads := make([]payload, 0, count)
			jobs := make([]*jobsdb.JobT, 0, count)

			for i := 0; i < count; i++ {
				p := payload{
					MessageID:        "message-id-" + strconv.Itoa(i),
					SourceID:         sourceID,
					DestinationID:    "destination-id-" + strconv.Itoa(i),
					TransformationID: "transformation-id-" + strconv.Itoa(i),
					TrackingPlanID:   "tracking-plan-id-" + strconv.Itoa(i),
					FailedStage:      "failed-stage-" + strconv.Itoa(i),
					EventType:        "event-type-" + strconv.Itoa(i),
					EventName:        "event-name-" + strconv.Itoa(i),
				}
				p.SetReceivedAt(receivedAt)
				p.SetFailedAt(failedAt.Add(time.Duration(i) * time.Second))
				payloads = append(payloads, p)

				epJSON, err := json.Marshal(p)
				require.NoError(t, err)

				jobs = append(jobs, &jobsdb.JobT{
					UUID:         uuid.New(),
					Parameters:   []byte(`{"source_id":"` + sourceID + `","workspaceId":"` + workspaceID + `"}`),
					EventPayload: epJSON,
					EventCount:   1,
					WorkspaceId:  workspaceID,
				})
			}

			require.NoError(t, errIndexDB.Store(ctx, jobs))

			cs := newMockConfigSubscriber()
			cs.addWorkspaceIDForSourceID(sourceID, workspaceID)

			statsStore, err := memstats.New()
			require.NoError(t, err)

			fm, err := filemanager.New(&filemanager.Settings{
				Provider: warehouseutils.MINIO,
				Config: map[string]any{
					"bucketName":      minioResource.BucketName,
					"accessKeyID":     minioResource.AccessKeyID,
					"secretAccessKey": minioResource.AccessKeySecret,
					"endPoint":        minioResource.Endpoint,
				},
			})
			require.NoError(t, err)

			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			limiterGroup := sync.WaitGroup{}
			limiter := kitsync.NewLimiter(ctx, &limiterGroup, "erridx_test", 1000, statsStore)
			defer func() {
				cancel()
				limiterGroup.Wait()
			}()

			w := newWorker(sourceID, c, logger.NOP, statsStore, errIndexDB, cs, fm, limiter, limiter, limiter)
			defer w.Stop()
			w.Work()

			require.EqualValues(t, len(jobs), statsStore.Get("erridx_uploaded_jobs", stats.Tags{
				"workspaceId": w.workspaceID,
				"sourceId":    w.sourceID,
			}).LastValue())
			require.EqualValues(t, len(jobs), statsStore.Get("erridx_processed_jobs", stats.Tags{
				"workspaceId": w.workspaceID,
				"sourceId":    w.sourceID,
				"state":       jobsdb.Succeeded.State,
			}).LastValue())

			lastFailedAt := failedAt.Add(time.Duration(len(jobs)-1) * time.Second)
			filePath := fmt.Sprintf("s3://%s/%s/%s/%s/%d_%d_%s**.parquet",
				minioResource.BucketName,
				w.sourceID,
				failedAt.Format("2006-01-02"),
				strconv.Itoa(failedAt.Hour()),
				failedAt.Unix(),
				lastFailedAt.Unix(),
				instanceID,
			)
			failedMessages := failedMessagesUsingDuckDB(t, ctx, minioResource, "SELECT * FROM read_parquet($1) WHERE failed_at >= $2 AND failed_at <= $3 ORDER BY failed_at ASC;", []interface{}{filePath, failedAt.UTC().UnixMicro(), lastFailedAt.UTC().UnixMicro()})
			require.Len(t, failedMessages, len(jobs))
			require.EqualValues(t, payloads, failedMessages)

			s3SelectPathPrefix := fmt.Sprintf("%s/%s/%s/%d_%d_%s",
				w.sourceID,
				failedAt.Format("2006-01-02"),
				strconv.Itoa(failedAt.Hour()),
				failedAt.Unix(),
				lastFailedAt.Unix(),
				instanceID,
			)
			objects := minioObjects(t, ctx, minioResource, s3SelectPathPrefix)
			require.Len(t, objects, 1)

			s3SelectQuery := fmt.Sprintf("SELECT message_id, source_id, destination_id, transformation_id, tracking_plan_id, failed_stage, event_type, event_name, received_at, failed_at FROM S3Object WHERE failed_at >= %d AND failed_at <= %d", failedAt.UTC().UnixMicro(), lastFailedAt.UTC().UnixMicro())
			failedMessagesUsing3Select := failedMessagesUsingMinioS3Select(t, ctx, minioResource, objects[0], s3SelectQuery)
			slices.SortFunc(failedMessagesUsing3Select, func(a, b payload) int {
				return a.FailedAtTime().Compare(b.FailedAtTime())
			})
			require.Equal(t, len(failedMessages), len(failedMessagesUsing3Select))
			require.Equal(t, failedMessages, failedMessagesUsing3Select)

			jr, err := errIndexDB.GetSucceeded(ctx, jobsdb.GetQueryParams{
				ParameterFilters: []jobsdb.ParameterFilterT{
					{Name: "source_id", Value: w.sourceID},
				},
				PayloadSizeLimit: w.config.payloadLimit.Load(),
				EventsLimit:      int(w.config.eventsLimit.Load()),
				JobsLimit:        int(w.config.eventsLimit.Load()),
			})
			require.NoError(t, err)
			require.Len(t, jr.Jobs, len(jobs))

			lo.ForEach(jr.Jobs, func(item *jobsdb.JobT, index int) {
				filePath := fmt.Sprintf("http://%s/%s/%s/%s/%s/%d_%d_%s.+.parquet",
					minioResource.Endpoint,
					minioResource.BucketName,
					w.sourceID,
					failedAt.Format("2006-01-02"),
					strconv.Itoa(failedAt.Hour()),
					failedAt.Unix(),
					lastFailedAt.Unix(),
					instanceID,
				)
				require.Regexp(t, filePath, gjson.GetBytes(item.LastJobStatus.ErrorResponse, "location").String())
			})
		})
		t.Run("multiple hours and days", func(t *testing.T) {
			receivedAt := time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC)
			failedAt := time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC)

			postgresContainer, err := postgres.Setup(pool, t)
			require.NoError(t, err)
			minioResource, err := minio.Setup(pool, t)
			require.NoError(t, err)

			c := config.New()
			c.Set("INSTANCE_ID", instanceID)

			errIndexDB := jobsdb.NewForReadWrite("err_idx", jobsdb.WithDBHandle(postgresContainer.DB), jobsdb.WithConfig(c), jobsdb.WithStats(stats.NOP))
			require.NoError(t, errIndexDB.Start())
			defer errIndexDB.TearDown()

			count := 100
			payloads := make([]payload, 0, count)
			jobs := make([]*jobsdb.JobT, 0, count)

			for i := 0; i < count; i++ {
				p := payload{
					MessageID:        "message-id-" + strconv.Itoa(i),
					SourceID:         sourceID,
					DestinationID:    "destination-id-" + strconv.Itoa(i),
					TransformationID: "transformation-id-" + strconv.Itoa(i),
					TrackingPlanID:   "tracking-plan-id-" + strconv.Itoa(i),
					FailedStage:      "failed-stage-" + strconv.Itoa(i),
					EventType:        "event-type-" + strconv.Itoa(i),
					EventName:        "event-name-" + strconv.Itoa(i),
				}
				p.SetReceivedAt(receivedAt)
				p.SetFailedAt(failedAt.Add(time.Duration(i) * time.Hour))
				payloads = append(payloads, p)

				epJSON, err := json.Marshal(p)
				require.NoError(t, err)

				jobs = append(jobs, &jobsdb.JobT{
					UUID:         uuid.New(),
					Parameters:   []byte(`{"source_id":"` + sourceID + `","workspaceId":"` + workspaceID + `"}`),
					EventPayload: epJSON,
					EventCount:   1,
					WorkspaceId:  workspaceID,
				})
			}

			require.NoError(t, errIndexDB.Store(ctx, jobs))

			cs := newMockConfigSubscriber()
			cs.addWorkspaceIDForSourceID(sourceID, workspaceID)

			fm, err := filemanager.New(&filemanager.Settings{
				Provider: warehouseutils.MINIO,
				Config: map[string]any{
					"bucketName":      minioResource.BucketName,
					"accessKeyID":     minioResource.AccessKeyID,
					"secretAccessKey": minioResource.AccessKeySecret,
					"endPoint":        minioResource.Endpoint,
				},
			})
			require.NoError(t, err)

			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			limiterGroup := sync.WaitGroup{}
			limiter := kitsync.NewLimiter(ctx, &limiterGroup, "erridx_test", 1000, stats.NOP)
			defer func() {
				cancel()
				limiterGroup.Wait()
			}()

			w := newWorker(sourceID, c, logger.NOP, stats.NOP, errIndexDB, cs, fm, limiter, limiter, limiter)
			defer w.Stop()
			w.Work()

			for i := 0; i < count; i++ {
				failedAt := failedAt.Add(time.Duration(i) * time.Hour)
				filePath := fmt.Sprintf("s3://%s/%s/%s/%s/%d_%d_%s**.parquet",
					minioResource.BucketName,
					w.sourceID,
					failedAt.Format("2006-01-02"),
					strconv.Itoa(failedAt.Hour()),
					failedAt.Unix(),
					failedAt.Unix(),
					instanceID,
				)
				failedMessages := failedMessagesUsingDuckDB(t, ctx, minioResource, "SELECT * FROM read_parquet($1) WHERE failed_at >= $2 AND failed_at <= $3 ORDER BY failed_at ASC;", []interface{}{filePath, failedAt.UTC().UnixMicro(), failedAt.UTC().UnixMicro()})
				require.EqualValues(t, []payload{payloads[i]}, failedMessages)
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
			require.Len(t, jr.Jobs, len(jobs))

			lo.ForEach(jr.Jobs, func(item *jobsdb.JobT, index int) {
				failedAt := failedAt.Add(time.Duration(index) * time.Hour)
				filePath := fmt.Sprintf("http://%s/%s/%s/%s/%s/%d_%d_%s.+.parquet",
					minioResource.Endpoint,
					minioResource.BucketName,
					w.sourceID,
					failedAt.Format("2006-01-02"),
					strconv.Itoa(failedAt.Hour()),
					failedAt.Unix(),
					failedAt.Unix(),
					instanceID,
				)
				require.Regexp(t, filePath, gjson.GetBytes(item.LastJobStatus.ErrorResponse, "location").String())
			})
		})
		t.Run("limits reached but few left without crossing upload frequency", func(t *testing.T) {
			receivedAt := time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC)
			failedAt := time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC)

			postgresContainer, err := postgres.Setup(pool, t)
			require.NoError(t, err)
			minioResource, err := minio.Setup(pool, t)
			require.NoError(t, err)

			eventsLimit := 24

			c := config.New()
			c.Set("INSTANCE_ID", instanceID)
			c.Set("Reporting.errorIndexReporting.minWorkerSleep", "1s")
			c.Set("Reporting.errorIndexReporting.uploadFrequency", "600s")
			c.Set("Reporting.errorIndexReporting.eventsLimit", strconv.Itoa(eventsLimit))

			errIndexDB := jobsdb.NewForReadWrite("err_idx", jobsdb.WithDBHandle(postgresContainer.DB), jobsdb.WithConfig(c), jobsdb.WithStats(stats.NOP))
			require.NoError(t, errIndexDB.Start())
			defer errIndexDB.TearDown()

			count := 100
			jobs := make([]*jobsdb.JobT, 0, count)

			for i := 0; i < count; i++ {
				p := payload{
					MessageID:        "message-id-" + strconv.Itoa(i),
					SourceID:         sourceID,
					DestinationID:    "destination-id-" + strconv.Itoa(i),
					TransformationID: "transformation-id-" + strconv.Itoa(i),
					TrackingPlanID:   "tracking-plan-id-" + strconv.Itoa(i),
					FailedStage:      "failed-stage-" + strconv.Itoa(i),
					EventType:        "event-type-" + strconv.Itoa(i),
					EventName:        "event-name-" + strconv.Itoa(i),
				}
				p.SetReceivedAt(receivedAt)
				p.SetFailedAt(failedAt.Add(time.Duration(i) * time.Second))

				epJSON, err := json.Marshal(p)
				require.NoError(t, err)

				jobs = append(jobs, &jobsdb.JobT{
					UUID:         uuid.New(),
					Parameters:   []byte(`{"source_id":"` + sourceID + `","workspaceId":"` + workspaceID + `"}`),
					EventPayload: epJSON,
					EventCount:   1,
					WorkspaceId:  workspaceID,
				})
			}
			require.NoError(t, errIndexDB.Store(ctx, jobs))

			cs := newMockConfigSubscriber()
			cs.addWorkspaceIDForSourceID(sourceID, workspaceID)

			fm, err := filemanager.New(&filemanager.Settings{
				Provider: warehouseutils.MINIO,
				Config: map[string]any{
					"bucketName":      minioResource.BucketName,
					"accessKeyID":     minioResource.AccessKeyID,
					"secretAccessKey": minioResource.AccessKeySecret,
					"endPoint":        minioResource.Endpoint,
				},
			})
			require.NoError(t, err)

			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			limiterGroup := sync.WaitGroup{}
			limiter := kitsync.NewLimiter(ctx, &limiterGroup, "erridx_test", 1000, stats.NOP)
			defer func() {
				cancel()
				limiterGroup.Wait()
			}()

			w := newWorker(sourceID, c, logger.NOP, stats.NOP, errIndexDB, cs, fm, limiter, limiter, limiter)
			defer w.Stop()
			w.Work()

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

func minioObjects(t testing.TB, ctx context.Context, mr *minio.Resource, prefix string) (objects []string) {
	t.Helper()

	for objInfo := range mr.Client.ListObjects(ctx, mr.BucketName, miniogo.ListObjectsOptions{
		Recursive: true,
		Prefix:    prefix,
	}) {
		objects = append(objects, objInfo.Key)
	}
	return
}

func failedMessagesUsingMinioS3Select(t testing.TB, ctx context.Context, mr *minio.Resource, filePath, query string) []payload {
	t.Helper()

	r, err := mr.Client.SelectObjectContent(ctx, mr.BucketName, filePath, miniogo.SelectObjectOptions{
		Expression:     query,
		ExpressionType: miniogo.QueryExpressionTypeSQL,
		InputSerialization: miniogo.SelectObjectInputSerialization{
			CompressionType: miniogo.SelectCompressionNONE,
			Parquet:         &miniogo.ParquetInputOptions{},
		},
		OutputSerialization: miniogo.SelectObjectOutputSerialization{
			CSV: &miniogo.CSVOutputOptions{
				RecordDelimiter: "\n",
				FieldDelimiter:  ",",
			},
		},
	})
	require.NoError(t, err)
	defer func() { _ = r.Close() }()

	buf := bytes.NewBuffer(make([]byte, 0, bytesize.MB))

	_, err = io.Copy(buf, r)
	require.NoError(t, err)

	c := csv.NewReader(buf)
	records, err := c.ReadAll()
	require.NoError(t, err)

	payloads := make([]payload, 0, len(records))
	for _, r := range records {
		p := payload{
			MessageID:        r[0],
			SourceID:         r[1],
			DestinationID:    r[2],
			TransformationID: r[3],
			TrackingPlanID:   r[4],
			FailedStage:      r[5],
			EventType:        r[6],
			EventName:        r[7],
		}

		receivedAt, err := strconv.Atoi(r[8])
		require.NoError(t, err)
		failedAt, err := strconv.Atoi(r[9])
		require.NoError(t, err)

		p.SetReceivedAt(time.UnixMicro(int64(receivedAt)))
		p.SetFailedAt(time.UnixMicro(int64(failedAt)))

		payloads = append(payloads, p)
	}
	return payloads
}

func failedMessagesUsingDuckDB(t testing.TB, ctx context.Context, mr *minio.Resource, query string, queryArgs []interface{}) []payload {
	t.Helper()

	db := duckDB(t)

	if mr != nil {
		_, err := db.Exec(fmt.Sprintf(`INSTALL httpfs; LOAD httpfs;SET s3_region='%s';SET s3_endpoint='%s';SET s3_access_key_id='%s';SET s3_secret_access_key='%s';SET s3_use_ssl= false;SET s3_url_style='path';`,
			mr.Region,
			mr.Endpoint,
			mr.AccessKeyID,
			mr.AccessKeySecret,
		))
		require.NoError(t, err)
	}

	rows, err := db.QueryContext(ctx, query, queryArgs...)
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

func duckDB(t testing.TB) *sql.DB {
	t.Helper()

	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	_, err = db.Exec(`INSTALL parquet; LOAD parquet;`)
	require.NoError(t, err)
	return db
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

		b.Log("csv size:", buf.Len()) // csv size: 150 MB
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
				ReceivedAt:       now.Add(time.Duration(i) * time.Second).UnixMicro(),
				FailedAt:         now.Add(time.Duration(i) * time.Second).UnixMicro(),
			})
		}

		buf := bytes.NewBuffer(make([]byte, 0, 1024))
		e := json.NewEncoder(buf)

		for _, record := range records {
			require.NoError(b, e.Encode(record))
		}

		b.Log("json size:", buf.Len()) // json size: 292 MB
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
				ReceivedAt:       now.Add(time.Duration(i) * time.Second).UnixMicro(),
				FailedAt:         now.Add(time.Duration(i) * time.Second).UnixMicro(),
			})
		}

		w := worker{}
		w.config.parquetRowGroupSize = config.SingleValueLoader(512 * bytesize.MB)
		w.config.parquetPageSize = config.SingleValueLoader(8 * bytesize.KB)
		w.config.parquetParallelWriters = config.SingleValueLoader(int64(8))

		buf := bytes.NewBuffer(make([]byte, 0, 1024))

		require.NoError(b, w.encodeToParquet(buf, records))

		b.Log("parquet size:", buf.Len()) // parquet size: 13.8 MB
	})
}
