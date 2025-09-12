package processor

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/minio"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/processor/isolation"
	"github.com/rudderlabs/rudder-server/processor/transformer"
	"github.com/rudderlabs/rudder-server/processor/types"
	testutils "github.com/rudderlabs/rudder-server/utils/tests"
)

func TestUTMirroring(t *testing.T) {
	messages := map[string]mockEventData{
		// this message should only be delivered to destination B
		"message-1": {
			id:                        "1",
			jobid:                     1010,
			originalTimestamp:         "2000-01-02T01:23:45",
			expectedOriginalTimestamp: "2000-01-02T01:23:45.000Z",
			sentAt:                    "2000-01-02 01:23",
			expectedSentAt:            "2000-01-02T01:23:00.000Z",
			expectedReceivedAt:        "2001-01-02T02:23:45.000Z",
			integrations:              map[string]bool{"All": false, "enabled-destination-b-definition-display-name": true},
		},
		// this message should not be delivered to destination B
		"message-2": {
			id:                        "2",
			jobid:                     1010,
			originalTimestamp:         "2000-02-02T01:23:45",
			expectedOriginalTimestamp: "2000-02-02T01:23:45.000Z",
			expectedReceivedAt:        "2001-01-02T02:23:45.000Z",
			integrations:              map[string]bool{"All": true, "enabled-destination-b-definition-display-name": false},
		},
		// this message should be delivered to all destinations
		"message-3": {
			id:                 "3",
			jobid:              2010,
			originalTimestamp:  "malformed timestamp",
			sentAt:             "2000-03-02T01:23:15",
			expectedSentAt:     "2000-03-02T01:23:15.000Z",
			expectedReceivedAt: "2002-01-02T02:23:45.000Z",
			integrations:       map[string]bool{"All": true},
		},
		// this message should be delivered to all destinations (default All value)
		"message-4": {
			id:                        "4",
			jobid:                     2010,
			originalTimestamp:         "2000-04-02T02:23:15.000Z", // missing sentAt
			expectedOriginalTimestamp: "2000-04-02T02:23:15.000Z",
			expectedReceivedAt:        "2002-01-02T02:23:45.000Z",
			integrations:              map[string]bool{},
		},
		// this message should not be delivered to any destination
		"message-5": {
			id:                 "5",
			jobid:              2010,
			expectedReceivedAt: "2002-01-02T02:23:45.000Z",
			integrations:       map[string]bool{"All": false},
		},
	}

	unprocessedJobsList := []*jobsdb.JobT{
		{
			UUID:      uuid.New(),
			JobID:     1010,
			CreatedAt: time.Date(2020, 0o4, 28, 23, 26, 0o0, 0o0, time.UTC),
			ExpireAt:  time.Date(2020, 0o4, 28, 23, 26, 0o0, 0o0, time.UTC),
			CustomVal: gatewayCustomVal[0],
			EventPayload: createBatchPayload(
				WriteKeyEnabledOnlyUT,
				"2001-01-02T02:23:45.000Z",
				[]mockEventData{
					messages["message-1"],
					messages["message-2"],
				},
				createMessagePayloadWithoutSources,
			),
			EventCount:    1,
			LastJobStatus: jobsdb.JobStatusT{},
			Parameters:    createBatchParameters(SourceIDEnabledOnlyUT),
		},
		{
			UUID:      uuid.New(),
			JobID:     2010,
			CreatedAt: time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
			ExpireAt:  time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
			CustomVal: gatewayCustomVal[0],
			EventPayload: createBatchPayload(
				WriteKeyEnabledOnlyUT,
				"2002-01-02T02:23:45.000Z",
				[]mockEventData{
					messages["message-3"],
					messages["message-4"],
					messages["message-5"],
				},
				createMessagePayloadWithoutSources,
			),
			EventCount: 1,
			Parameters: createBatchParameters(SourceIDEnabledOnlyUT),
		},
	}

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = time.Minute

	assertStoreJob := func(t testing.TB, job *jobsdb.JobT, i int, destination string) {
		isValidUUID, err := testutils.BeValidUUID().Match(job.UUID.String())
		require.NoError(t, err)
		require.True(t, isValidUUID)
		require.EqualValues(t, 0, job.JobID)
		requireTimeCirca(t, time.Now(), job.CreatedAt, 200*time.Millisecond)
		requireTimeCirca(t, time.Now(), job.ExpireAt, 200*time.Millisecond)
		require.JSONEq(t, fmt.Sprintf(`{"int-value":%d,"string-value":%q}`, i, destination), string(job.EventPayload))
		require.Len(t, job.LastJobStatus.JobState, 0)
		require.JSONEq(t, fmt.Sprintf(`{
			"source_id": "source-from-transformer",
			"source_name": "%s",
			"destination_id": "destination-from-transformer",
			"received_at": "",
			"transform_at": "processor",
			"message_id": "",
			"gateway_job_id": 0,
			"source_task_run_id": "",
			"source_job_id": "",
			"source_job_run_id": "",
			"event_name": "",
			"event_type": "",
			"source_definition_id": "",
			"destination_definition_id": "",
			"source_category": "",
			"record_id": null,
			"workspaceId": "",
			"traceparent": "",
			"connection_id": "source-from-transformer:destination-from-transformer"
		}`, sourceIDToName[SourceIDEnabledOnlyUT]), string(job.Parameters))
	}

	transformExpectations := map[string]transformExpectation{
		DestinationIDEnabledB: {
			events:                    3,
			messageIds:                "message-1,message-3,message-4",
			destinationDefinitionName: "minio",
		},
	}

	type confSetting struct{ key, value string }

	prepareProcessor := func(
		t testing.TB, tcs *transformer.SimpleClients, sanitySampling float64, fireAndForget bool,
		confSettings ...confSetting,
	) (*minio.Resource, *Handle, *testContext) {
		minioContainer, err := minio.Setup(pool, t)
		require.NoError(t, err)

		conf := config.New()
		conf.Set("Processor.userTransformationMirroring.sanitySampling", strconv.FormatFloat(sanitySampling, 'f', 0, 64))
		conf.Set("Processor.userTransformationMirroring.fireAndForget", strconv.FormatBool(fireAndForget))
		// conf.Set("USER_TRANSFORM_MIRROR_URL", "TODO")
		conf.Set("UTSampling.Bucket", minioContainer.BucketName)
		conf.Set("UTSampling.Endpoint", fmt.Sprintf("http://%s", minioContainer.Endpoint))
		conf.Set("UTSampling.AccessKeyId", minioContainer.AccessKeyID)
		conf.Set("UTSampling.AccessKey", minioContainer.AccessKeySecret)
		conf.Set("UTSampling.S3ForcePathStyle", "true")
		conf.Set("UTSampling.DisableSsl", "true")
		conf.Set("UTSampling.EnableSse", "false")
		conf.Set("UTSampling.Region", minioContainer.Region)
		for _, setting := range confSettings {
			conf.Set(setting.key, setting.value)
		}

		processor := NewHandle(conf, tcs)
		isolationStrategy, err := isolation.GetStrategy(isolation.ModeNone)
		require.NoError(t, err)
		processor.isolationStrategy = isolationStrategy
		processor.config.enableConcurrentStore = config.SingleValueLoader(false)

		c := &testContext{}
		c.Setup(t)
		c.mockGatewayJobsDB.EXPECT().DeleteExecuting().Times(1)
		t.Cleanup(c.Finish)

		return minioContainer, processor, c
	}

	setupMocksExpectations := func(t testing.TB, c *testContext, processor *Handle) {
		c.mockGatewayJobsDB.EXPECT().GetUnprocessed(gomock.Any(), jobsdb.GetQueryParams{
			CustomValFilters: gatewayCustomVal,
			JobsLimit:        processor.config.maxEventsToProcess.Load(),
			EventsLimit:      processor.config.maxEventsToProcess.Load(),
			PayloadSizeLimit: processor.payloadLimit.Load(),
		}).Return(jobsdb.JobsResult{Jobs: unprocessedJobsList}, nil).Times(1)
		c.mockBatchRouterJobsDB.EXPECT().WithStoreSafeTx(gomock.Any(), gomock.Any()).Times(1).Do(func(ctx context.Context, f func(tx jobsdb.StoreSafeTx) error) {
			_ = f(jobsdb.EmptyStoreSafeTx())
		}).Return(nil)
		callStoreBatchRouter := c.mockBatchRouterJobsDB.EXPECT().StoreInTx(gomock.Any(), gomock.Any(), gomock.Any()).Times(1).
			Do(func(ctx context.Context, tx jobsdb.StoreSafeTx, jobs []*jobsdb.JobT) {
				require.Len(t, jobs, 2)
				for i, job := range jobs {
					assertStoreJob(t, job, i, "value-enabled-destination-b")
				}
			})
		c.mockArchivalDB.EXPECT().WithStoreSafeTx(gomock.Any(), gomock.Any()).AnyTimes().Do(func(ctx context.Context, f func(tx jobsdb.StoreSafeTx) error) {
			_ = f(jobsdb.EmptyStoreSafeTx())
		}).Return(nil)
		c.mockArchivalDB.EXPECT().
			StoreInTx(gomock.Any(), gomock.Any(), gomock.Any()).
			AnyTimes()
		c.mockGatewayJobsDB.EXPECT().WithUpdateSafeTx(gomock.Any(), gomock.Any()).Do(
			func(ctx context.Context, f func(tx jobsdb.UpdateSafeTx) error) {
				_ = f(jobsdb.EmptyUpdateSafeTx())
			}).Return(nil).Times(1)
		c.mockGatewayJobsDB.EXPECT().UpdateJobStatusInTx(
			gomock.Any(), gomock.Any(), gomock.Len(len(unprocessedJobsList)), gatewayCustomVal, nil,
		).Times(1).After(callStoreBatchRouter).
			Do(func(ctx context.Context, txn jobsdb.UpdateSafeTx, statuses []*jobsdb.JobStatusT, _, _ interface{}) {
				for i := range unprocessedJobsList {
					require.Equal(t, statuses[i].JobID, unprocessedJobsList[i].JobID)
					require.Equal(t, statuses[i].JobState, jobsdb.Succeeded.State)
					requireTimeCirca(t, time.Now(), statuses[i].RetryTime, 200*time.Millisecond)
					requireTimeCirca(t, time.Now(), statuses[i].ExecTime, 200*time.Millisecond)
				}
			})
	}

	handlePendingGatewayJobs := func(t testing.TB, processor *Handle, c *testContext, opts ...func(*Handle)) {
		Setup(processor, c, false, false, t)
		for _, opt := range opts {
			opt(processor)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		require.NoError(t, processor.config.asyncInit.WaitContext(ctx))
		t.Log("Processor setup and init done")
		didWork := processor.handlePendingGatewayJobs("")
		require.True(t, didWork)
	}

	t.Run("mirror does not return anything", func(t *testing.T) {
		mockTransformerClients := transformer.NewSimpleClients()
		minioContainer, processor, tc := prepareProcessor(t, mockTransformerClients, 100, false)

		setupMocksExpectations(t, tc, processor)

		mockTransformerClients.WithDynamicUserTransform(func(ctx context.Context, clientEvents []types.TransformerEvent) types.Response {
			outputEvents := make([]types.TransformerResponse, 0)
			for _, event := range clientEvents {
				event.Message["user-transform"] = "value"
				outputEvents = append(outputEvents, types.TransformerResponse{
					Output: event.Message,
				})
			}
			return types.Response{Events: outputEvents}
		})
		mockTransformerClients.WithDynamicDestinationTransform(func(ctx context.Context, clientEvents []types.TransformerEvent) types.Response {
			return assertDestinationTransform(
				messages, SourceIDEnabledOnlyUT, DestinationIDEnabledB, transformExpectations[DestinationIDEnabledB], t,
			)(ctx, clientEvents, 1)
		})

		memStats, err := memstats.New()
		require.NoError(t, err)
		handlePendingGatewayJobs(t, processor, tc, func(h *Handle) {
			h.statsFactory = memStats
		})

		require.Eventually(t, func() bool {
			metric := memStats.Get("processor_ut_mirroring_responses_count", stats.Tags{
				"equal":     "false",
				"partition": "",
			})
			return metric != nil && metric.LastValue() == 1
		}, 10*time.Second, 10*time.Millisecond, "Expected different response from UserMirrorTransform")

		var files []minio.File
		require.Eventually(t, func() bool {
			files, err = minioContainer.Contents(context.Background(), "")
			return err == nil && len(files) == 2
		}, 10*time.Second, 50*time.Millisecond, "Expected two files in the bucket")

		// Find which file is the diff and which is the clientEvents
		var diffFile, clientEventsFile minio.File
		for _, file := range files {
			if strings.HasSuffix(file.Key, "-diff") {
				diffFile = file
			} else {
				clientEventsFile = file
			}
		}

		require.NotNil(t, diffFile, "Diff file not found")
		require.NotNil(t, clientEventsFile, "ClientEvents file not found")
		require.Equal(t, "Expected Events length 3, got 0", diffFile.Content)

		// Verify clientEvents file contains valid JSON that can be unmarshalled into a slice of types.TransformerEvent
		var clientEvents []types.TransformerEvent
		require.NoError(t, jsonrs.Unmarshal([]byte(clientEventsFile.Content), &clientEvents), "Failed to unmarshal clientEvents")
		require.NotEmpty(t, clientEvents, "ClientEvents should not be empty")

		// Verify clientEvents file content matches the expected content
		expectedClientEvents, err := os.ReadFile("./testdata/goldenUtMirrorClientEvents.json")
		require.NoError(t, err)
		require.JSONEq(t, string(expectedClientEvents), clientEventsFile.Content)
	})

	t.Run("mirror returns the same events", func(t *testing.T) {
		mockTransformerClients := transformer.NewSimpleClients()
		minioContainer, processor, tc := prepareProcessor(t, mockTransformerClients, 100, false)

		setupMocksExpectations(t, tc, processor)

		userTransformation := func(ctx context.Context, clientEvents []types.TransformerEvent) types.Response {
			copiedEvents := copyClientEvents(t, clientEvents)
			outputEvents := make([]types.TransformerResponse, 0)
			for _, event := range copiedEvents {
				event.Message["user-transform"] = "value"
				outputEvents = append(outputEvents, types.TransformerResponse{
					Output:     event.Message,
					StatusCode: 200,
					Metadata: types.Metadata{
						SourceID:        SourceIDEnabledOnlyUT,
						SourceName:      sourceIDToName[SourceIDEnabledOnlyUT],
						DestinationID:   DestinationIDEnabledB,
						DestinationType: "MINIO",
					},
				})
			}
			return types.Response{Events: outputEvents}
		}
		mockTransformerClients.WithDynamicUserTransform(userTransformation)
		mockTransformerClients.WithDynamicUserMirrorTransform(userTransformation)
		mockTransformerClients.WithDynamicDestinationTransform(func(ctx context.Context, clientEvents []types.TransformerEvent) types.Response {
			return assertDestinationTransform(
				messages, SourceIDEnabledOnlyUT, DestinationIDEnabledB, transformExpectations[DestinationIDEnabledB], t,
			)(ctx, clientEvents, 1)
		})

		memStats, err := memstats.New()
		require.NoError(t, err)
		handlePendingGatewayJobs(t, processor, tc, func(h *Handle) {
			h.statsFactory = memStats
		})

		require.Eventually(t, func() bool {
			metric := memStats.Get("processor_ut_mirroring_responses_count", stats.Tags{
				"equal":     "true",
				"partition": "",
			})
			return metric != nil && metric.LastValue() == 1
		}, 10*time.Second, 10*time.Millisecond, "Expected same response from UserMirrorTransform")

		var files []minio.File
		require.Eventually(t, func() bool {
			files, err = minioContainer.Contents(context.Background(), "")
			return err == nil && len(files) == 0
		}, 10*time.Second, 50*time.Millisecond, "Expected zero files in the bucket")
	})

	t.Run("mirror returns slightly different events", func(t *testing.T) {
		mockTransformerClients := transformer.NewSimpleClients()
		minioContainer, processor, tc := prepareProcessor(t, mockTransformerClients, 100, false)

		setupMocksExpectations(t, tc, processor)

		mockTransformerClients.WithDynamicUserTransform(func(ctx context.Context, clientEvents []types.TransformerEvent) types.Response {
			outputEvents := make([]types.TransformerResponse, 0)
			for _, event := range clientEvents {
				event.Message["user-transform"] = "value"
				outputEvents = append(outputEvents, types.TransformerResponse{
					Output:     event.Message,
					StatusCode: 200,
					Metadata: types.Metadata{
						SourceID:        SourceIDEnabledOnlyUT,
						SourceName:      sourceIDToName[SourceIDEnabledOnlyUT],
						DestinationID:   DestinationIDEnabledB,
						DestinationType: "MINIO",
					},
				})
			}
			return types.Response{Events: outputEvents}
		})
		mockTransformerClients.WithDynamicUserMirrorTransform(func(ctx context.Context, clientEvents []types.TransformerEvent) types.Response {
			outputEvents := make([]types.TransformerResponse, 0)
			for _, event := range clientEvents {
				messageCopy := make(map[string]any)
				for k, v := range event.Message {
					messageCopy[k] = v
				}
				messageCopy["user-transform"] = "value-mirror"
				outputEvents = append(outputEvents, types.TransformerResponse{
					Output:     messageCopy,
					StatusCode: 200,
					Metadata: types.Metadata{
						SourceID:        SourceIDEnabledOnlyUT,
						SourceName:      sourceIDToName[SourceIDEnabledOnlyUT],
						DestinationID:   DestinationIDEnabledB,
						DestinationType: "MINIO",
					},
				})
			}
			return types.Response{Events: outputEvents}
		})
		mockTransformerClients.WithDynamicDestinationTransform(func(ctx context.Context, clientEvents []types.TransformerEvent) types.Response {
			return assertDestinationTransform(
				messages, SourceIDEnabledOnlyUT, DestinationIDEnabledB, transformExpectations[DestinationIDEnabledB], t,
			)(ctx, clientEvents, 1)
		})

		memStats, err := memstats.New()
		require.NoError(t, err)
		handlePendingGatewayJobs(t, processor, tc, func(h *Handle) {
			h.statsFactory = memStats
		})

		require.Eventually(t, func() bool {
			metric := memStats.Get("processor_ut_mirroring_responses_count", stats.Tags{
				"equal":     "false",
				"partition": "",
			})
			return metric != nil && metric.LastValue() == 1
		}, 10*time.Second, 10*time.Millisecond, "Expected different response from UserMirrorTransform")

		var files []minio.File
		require.Eventually(t, func() bool {
			files, err = minioContainer.Contents(context.Background(), "")
			return err == nil && len(files) == 2
		}, 10*time.Second, 50*time.Millisecond, "Expected two files in the bucket")

		// Find which file is the diff and which is the clientEvents
		var diffFile, clientEventsFile minio.File
		for _, file := range files {
			if strings.HasSuffix(file.Key, "-diff") {
				diffFile = file
			} else {
				clientEventsFile = file
			}
		}

		require.NotNil(t, diffFile, "Diff file not found")
		require.NotNil(t, clientEventsFile, "ClientEvents file not found")

		expectedDiff, err := os.ReadFile("./testdata/goldenUtMirrorDiff.txt")
		require.NoError(t, err)
		require.Equal(t, string(expectedDiff), diffFile.Content)

		// Verify clientEvents file contains valid JSON that can be unmarshalled into a slice of types.TransformerEvent
		var clientEvents []types.TransformerEvent
		require.NoError(t, jsonrs.Unmarshal([]byte(clientEventsFile.Content), &clientEvents), "Failed to unmarshal clientEvents")
		require.NotEmpty(t, clientEvents, "ClientEvents should not be empty")

		// Verify clientEvents file content matches the expected content
		expectedClientEvents, err := os.ReadFile("./testdata/goldenUtMirrorClientEvents.json")
		require.NoError(t, err)
		require.JSONEq(t, string(expectedClientEvents), clientEventsFile.Content)
	})

	t.Run("fire and forget", func(t *testing.T) {
		mockTransformerClients := transformer.NewSimpleClients()
		minioContainer, processor, tc := prepareProcessor(t, mockTransformerClients, 0, true)

		setupMocksExpectations(t, tc, processor)

		done := make(chan struct{})
		userTransformation := func(mirror bool) func(ctx context.Context, clientEvents []types.TransformerEvent) types.Response {
			return func(ctx context.Context, clientEvents []types.TransformerEvent) types.Response {
				if mirror {
					defer close(done)
				}
				outputEvents := make([]types.TransformerResponse, 0)
				for _, event := range clientEvents {
					event.Message["user-transform"] = "value"
					outputEvents = append(outputEvents, types.TransformerResponse{
						Output:     event.Message,
						StatusCode: 200,
						Metadata: types.Metadata{
							SourceID:        SourceIDEnabledOnlyUT,
							SourceName:      sourceIDToName[SourceIDEnabledOnlyUT],
							DestinationID:   DestinationIDEnabledB,
							DestinationType: "MINIO",
						},
					})
				}
				return types.Response{Events: outputEvents}
			}
		}
		mockTransformerClients.WithDynamicUserTransform(userTransformation(false))
		mockTransformerClients.WithDynamicUserMirrorTransform(userTransformation(true))
		mockTransformerClients.WithDynamicDestinationTransform(func(ctx context.Context, clientEvents []types.TransformerEvent) types.Response {
			return assertDestinationTransform(
				messages, SourceIDEnabledOnlyUT, DestinationIDEnabledB, transformExpectations[DestinationIDEnabledB], t,
			)(ctx, clientEvents, 1)
		})

		memStats, err := memstats.New()
		require.NoError(t, err)
		handlePendingGatewayJobs(t, processor, tc, func(h *Handle) {
			h.statsFactory = memStats
		})

		select {
		case <-done:
			t.Log("Mirror request completed")
		case <-time.After(20 * time.Second):
			t.Fatal("Timeout waiting for mirror request")
		}

		require.Eventually(t, func() bool {
			metric := memStats.Get("proc_transform_stage_out_count", stats.Tags{
				"destType":                  "MINIO",
				"destination":               DestinationIDEnabledB,
				"error":                     "false",
				"mirroring":                 "true",
				"module":                    "batch_router",
				"source":                    SourceIDEnabledOnlyUT,
				"transformationType":        "USER_TRANSFORMATION",
				"transformation_id":         "",
				"transformation_version_id": "transformation-version-id",
				"workspaceId":               "",
			})
			return metric != nil && metric.LastValue() == 3
		}, 10*time.Second, 10*time.Millisecond, "Expected 3 events to be sent via mirroring")

		var files []minio.File
		require.Eventually(t, func() bool {
			files, err = minioContainer.Contents(context.Background(), "")
			return err == nil && len(files) == 0
		}, 10*time.Second, 50*time.Millisecond, "Expected zero files in the bucket")
	})
}

func TestShouldSample(t *testing.T) {
	var (
		timesPerTest      = 1000
		repeatEachTestFor = 10
		errorMargin       = float64(8) // %
		testCases         = []float64{10, 20, 30, 40, 50, 60, 70, 80, 90}
	)

	for k := 0; k < repeatEachTestFor; k++ {
		for i := 0; i < timesPerTest; i++ { // no error margin for 0% and 100%
			require.True(t, shouldSample(100))
			require.False(t, shouldSample(0))
		}
	}

	for k := 0; k < repeatEachTestFor; k++ {
		for _, testCase := range testCases {
			var hits int64
			for i := 0; i < timesPerTest; i++ {
				if shouldSample(testCase) {
					hits++
				}
			}
			actualPercentage := float64(hits) * 100 / float64(timesPerTest)
			require.InDeltaf(t, testCase, actualPercentage, errorMargin,
				"Expected  %.0f%% (±5%%), got %.2f%%", testCase, actualPercentage,
			)
		}
	}
}

func requireTimeCirca(t require.TestingT, expected, actual time.Time, difference time.Duration) {
	require.InDelta(t, float64(expected.UnixMilli()), float64(actual.UnixMilli()), float64(difference))
}

func copyClientEvents(t *testing.T, clientEvents []types.TransformerEvent) []types.TransformerEvent {
	marshalledEvents, err := jsonrs.Marshal(clientEvents)
	require.NoError(t, err)
	var copiedEvents []types.TransformerEvent
	require.NoError(t, jsonrs.Unmarshal(marshalledEvents, &copiedEvents))
	return copiedEvents
}
