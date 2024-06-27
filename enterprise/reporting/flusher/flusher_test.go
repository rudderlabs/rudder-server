package flusher

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/enterprise/reporting/flusher/client"
	"github.com/rudderlabs/rudder-server/enterprise/reporting/flusher/db"
	"github.com/rudderlabs/rudder-server/enterprise/reporting/flusher/handler"
	"github.com/rudderlabs/rudder-server/enterprise/reporting/flusher/report"
)

func setup(t *testing.T) (*gomock.Controller, *db.MockDB, *client.MockClient, *handler.MockHandler, *Flusher) {
	ctrl := gomock.NewController(t)
	mockDB := db.NewMockDB(ctrl)
	mockClient := client.NewMockClient(ctrl)
	mockHandler := handler.NewMockHandler(ctrl)

	ctx := context.Background()
	log := logger.NOP
	inputStats := stats.NOP
	table := "test_table"
	labels := []string{"label1", "label2"}
	values := []string{"value1", "value2"}
	reportingURL := "http://localhost"
	inAppAggregationEnabled := true

	f := NewFlusher(ctx, mockDB, log, inputStats, table, labels, values, reportingURL, inAppAggregationEnabled, mockHandler)
	f.client = mockClient

	f.initStats(f.commonTags)

	return ctrl, mockDB, mockClient, mockHandler, f
}

func setupFlusher(f *Flusher) {
	f.aggWindowMins = config.SingleValueLoader(1 * time.Minute)
	f.batchSizeFromDB = config.SingleValueLoader(4)
	f.batchSizeToReporting = config.SingleValueLoader(2)
	f.minConcurrentRequests = config.SingleValueLoader(2)
	f.maxConcurrentRequests = config.SingleValueLoader(5)
	f.lagThresholdForAggresiveFlushInMins = config.SingleValueLoader(10 * time.Minute)
	f.flushInterval = config.SingleValueLoader(1 * time.Second)
	f.maxOpenConnections = 2
	f.recentExclusionWindow = config.SingleValueLoader(30 * time.Second)
}

func createReports(numItems int) []report.RawReport {
	items := make([]report.RawReport, numItems)
	for i := 0; i < numItems; i++ {
		items[i] = report.RawReport{
			"column1": fmt.Sprintf("value%d", i*2+1),
			"column2": fmt.Sprintf("value%d", i*2+2),
		}
	}
	return items
}

func TestNewFlusher(t *testing.T) {
	ctrl, mockDB, _, mockHandler, f := setup(t)
	defer ctrl.Finish()

	t.Run("flusher creation", func(t *testing.T) {
		assert.NotNil(t, f)
		assert.Equal(t, mockDB, f.db)
		assert.Equal(t, mockHandler, f.handler)
		assert.Equal(t, "test_table", f.table)
		assert.Equal(t, "http://localhost", f.reportingURL)
	})

	t.Run("return same instance for a table", func(t *testing.T) {
		_, _, _, _, f2 := setup(t)
		defer ctrl.Finish()

		assert.Equal(t, f, f2)
	})
}

func TestStartAndStop(t *testing.T) {
	ctrl, _, _, _, f := setup(t)
	defer ctrl.Finish()

	setupFlusher(f)
	f.db = &db.NOP{}

	t.Run("start and stop flusher", func(t *testing.T) {
		go f.Start()

		require.Eventually(t, func() bool {
			return f.started.Load()
		}, 5*time.Second, 50*time.Millisecond, "Flusher should have started")

		assert.True(t, f.started.Load())
		f.Stop()
		assert.False(t, f.started.Load())

		// Check that f.ctx is cancelled.
		select {
		case <-f.ctx.Done():
		default:
			t.Fatal("Expected context to be cancelled, but it was not")
		}
	})
}

func TestStartFlushing(t *testing.T) {
	ctrl, _, _, _, f := setup(t)
	defer ctrl.Finish()

	setupFlusher(f)
	f.db = &db.NOP{}

	t.Run("start flushing loop", func(t *testing.T) {
		go func() {
			if err := f.startFlushing(f.ctx); err != nil {
				t.Errorf("Error starting flushing: %v", err)
			}
		}()
		f.cancel()

		// Check that f.ctx is cancelled.
		select {
		case <-f.ctx.Done():
		default:
			t.Fatal("Expected context to be cancelled, but it was not")
		}
	})
}

func TestStartLagCapture(t *testing.T) {
	ctrl, _, _, _, f := setup(t)
	defer ctrl.Finish()

	setupFlusher(f)
	f.db = &db.NOP{}

	t.Run("start flushing loop", func(t *testing.T) {
		go func() {
			if err := f.startLagCapture(f.ctx); err != nil {
				t.Errorf("Error starting lag capture: %v", err)
			}
		}()
		f.cancel()

		// Check that f.ctx is cancelled.
		select {
		case <-f.ctx.Done():
		default:
			t.Fatal("Expected context to be cancelled, but it was not")
		}
	})
}

func TestFlush(t *testing.T) {
	ctrl, mockDB, mockClient, mockHandler, f := setup(t)
	defer ctrl.Finish()

	currentTime := time.Now().UTC()
	start := currentTime.Add(-5 * time.Minute)
	end := start.Add(1 * time.Minute)
	reports := createReports(10)
	setupFlusher(f)

	t.Run("main loop once in-app aggregation", func(t *testing.T) {
		// 1. Get time range
		mockDB.EXPECT().GetStart(gomock.Any(), f.table).Return(start, nil)

		// 2. Fetch and aggregate in-app or aggregate in db
		batchSize := f.batchSizeFromDB.Load()
		mockDB.EXPECT().FetchBatch(gomock.Any(), f.table, start, end, batchSize, 0).Return(reports[:batchSize], nil)
		mockDB.EXPECT().FetchBatch(gomock.Any(), f.table, start, end, batchSize, batchSize).Return(reports[batchSize:2*batchSize], nil)
		mockDB.EXPECT().FetchBatch(gomock.Any(), f.table, start, end, batchSize, 2*batchSize).Return(reports[2*batchSize:], nil)
		mockHandler.EXPECT().Decode(gomock.Any()).Return(nil, nil).Times(len(reports))
		mockHandler.EXPECT().Aggregate(gomock.Any(), gomock.Any()).Return(nil).Times(len(reports) - 1)

		// 3. Send reports
		mockClient.EXPECT().MakePOSTRequest(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

		// 4. Delete reports
		mockDB.EXPECT().Delete(gomock.Any(), f.table, start, end).Return(nil)

		err := f.flush(f.ctx)
		require.NoError(t, err)
	})
}

func TestGetRange(t *testing.T) {
	ctrl, mockDB, _, _, f := setup(t)
	defer ctrl.Finish()

	aggWindowMins := 2 * time.Minute
	recentExclusionWindow := 60 * time.Second
	currentTime := time.Now().UTC()

	t.Run("getRange", func(t *testing.T) {
		startTime := currentTime.Add(-aggWindowMins - recentExclusionWindow - 1*time.Second)
		expectedEnd := startTime.Add(aggWindowMins)

		mockDB.EXPECT().GetStart(f.ctx, f.table).Return(startTime, nil).Times(1)

		start, end, err := f.getRange(f.ctx, aggWindowMins, recentExclusionWindow)
		assert.NoError(t, err)
		assert.Equal(t, startTime, start)
		assert.Equal(t, expectedEnd, end)
	})

	t.Run("getRange with recent window excluded", func(t *testing.T) {
		startTime := currentTime.Add(-aggWindowMins + recentExclusionWindow)
		expectedEnd := startTime.Add(aggWindowMins)

		mockDB.EXPECT().GetStart(f.ctx, f.table).Return(startTime, nil).Times(1)

		start, end, err := f.getRange(f.ctx, aggWindowMins, recentExclusionWindow)
		assert.NoError(t, err)
		assert.Equal(t, startTime, start)
		assert.True(t, end.Before(expectedEnd))
		assert.True(t, end.Before(currentTime))
	})

	t.Run("failed getRange", func(t *testing.T) {
		mockDB.EXPECT().GetStart(f.ctx, f.table).Return(time.Time{}, errors.New("db error")).Times(1)

		_, _, err := f.getRange(f.ctx, aggWindowMins, recentExclusionWindow)
		assert.Error(t, err)
	})
}

func TestAggregate(t *testing.T) {
	ctrl, mockDB, _, mockHandler, f := setup(t)
	defer ctrl.Finish()

	currentTime := time.Now().UTC()
	start := currentTime.Add(-5 * time.Minute)
	end := start.Add(5 * time.Minute)
	batchSize := 2

	t.Run("in-app aggregation with multiple batches", func(t *testing.T) {
		inAppAggregationEnabled := true
		reports := createReports(batchSize + 1)
		mockDB.EXPECT().FetchBatch(f.ctx, f.table, start, end, batchSize, 0).Return(reports[:batchSize], nil)
		mockDB.EXPECT().FetchBatch(f.ctx, f.table, start, end, batchSize, batchSize).Return(reports[batchSize:], nil)
		mockHandler.EXPECT().Decode(gomock.Any()).Return(nil, nil).Times(len(reports))
		mockHandler.EXPECT().Aggregate(gomock.Any(), gomock.Any()).Return(nil).Times(len(reports) - 1)

		aggReports, err := f.aggregate(f.ctx, start, end, inAppAggregationEnabled, batchSize)
		require.NoError(t, err)
		assert.Len(t, aggReports, 1)
	})

	t.Run("in-app aggregation with single batch", func(t *testing.T) {
		inAppAggregationEnabled := true
		items := createReports(batchSize - 1)
		mockDB.EXPECT().FetchBatch(f.ctx, f.table, start, end, batchSize, 0).Return(items[:], nil)
		mockHandler.EXPECT().Decode(gomock.Any()).Return(nil, nil).Times(len(items))
		mockHandler.EXPECT().Aggregate(gomock.Any(), gomock.Any()).Return(nil).Times(len(items) - 1)

		aggReports, err := f.aggregate(f.ctx, start, end, inAppAggregationEnabled, batchSize)
		require.NoError(t, err)
		assert.Len(t, aggReports, 1)
	})

	t.Run("in-app aggregation failed", func(t *testing.T) {
		inAppAggregationEnabled := true
		mockDB.EXPECT().FetchBatch(f.ctx, f.table, start, end, batchSize, 0).Return(nil, errors.New("db error")).Times(1)

		_, err := f.aggregate(f.ctx, start, end, inAppAggregationEnabled, batchSize)
		assert.Error(t, err)
	})

	t.Run("aggregation in db not implemented", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("The code did not panic")
			}
		}()

		inAppAggregationEnabled := false

		_, err := f.aggregate(f.ctx, start, end, inAppAggregationEnabled, batchSize)
		require.Error(t, err)
	})
}

func TestSend(t *testing.T) {
	ctrl, _, mockClient, _, f := setup(t)
	defer ctrl.Finish()

	aggReports := make([]*report.DecodedReport, 5)
	concurrency := 2

	// should call sendInBatches
	t.Run("send in batches", func(t *testing.T) {
		batchSize := 2
		mockClient.EXPECT().MakePOSTRequest(gomock.Any(), gomock.Any()).Return(nil).Times(3)
		err := f.send(f.ctx, aggReports, batchSize, concurrency)
		require.NoError(t, err)
	})
	t.Run("send in individually", func(t *testing.T) {
		batchSize := 1
		mockClient.EXPECT().MakePOSTRequest(gomock.Any(), gomock.Any()).Return(nil).Times(5)
		err := f.send(f.ctx, aggReports, batchSize, concurrency)
		require.NoError(t, err)
	})
}

func TestSendInBatches(t *testing.T) {
	ctrl, _, mockClient, _, f := setup(t)
	defer ctrl.Finish()

	aggReports := make([]*report.DecodedReport, 25)
	concurrency := 2

	t.Run("send in multiple batches", func(t *testing.T) {
		batchSize := 4
		mockClient.EXPECT().MakePOSTRequest(gomock.Any(), gomock.Any()).Return(nil).Times(7)

		err := f.sendInBatches(f.ctx, aggReports, batchSize, concurrency)
		require.NoError(t, err)
	})

	t.Run("send in multiple batches", func(t *testing.T) {
		batchSize := 30
		mockClient.EXPECT().MakePOSTRequest(gomock.Any(), gomock.Any()).Return(nil).Times(1)

		err := f.sendInBatches(f.ctx, aggReports, batchSize, concurrency)
		require.NoError(t, err)
	})

	// test error case
	t.Run("error in sendInBatches", func(t *testing.T) {
		batchSize := 4
		mockClient.EXPECT().MakePOSTRequest(gomock.Any(), gomock.Any()).Return(errors.New("client error")).Times(7)

		err := f.sendInBatches(f.ctx, aggReports, batchSize, concurrency)
		require.Error(t, err)
	})
}

func TestSendIndividually(t *testing.T) {
	ctrl, _, mockClient, _, f := setup(t)
	defer ctrl.Finish()

	aggReports := make([]*report.DecodedReport, 5)
	concurrency := 2

	t.Run("sendIndividually success", func(t *testing.T) {
		mockClient.EXPECT().MakePOSTRequest(gomock.Any(), gomock.Any()).Return(nil).Times(5)

		err := f.sendIndividually(f.ctx, aggReports, concurrency)
		require.NoError(t, err)
	})

	t.Run("error in sendIndividually", func(t *testing.T) {
		mockClient.EXPECT().MakePOSTRequest(gomock.Any(), gomock.Any()).Return(errors.New("client error")).AnyTimes()
		err := f.sendIndividually(f.ctx, aggReports, concurrency)
		require.Error(t, err)
	})
}

func TestDelete(t *testing.T) {
	ctrl, mockDB, _, _, f := setup(t)
	defer ctrl.Finish()

	currentTime := time.Now().UTC()
	start := currentTime.Add(-5 * time.Minute)
	end := currentTime.Add(-1 * time.Minute)

	t.Run("delete success", func(t *testing.T) {
		mockDB.EXPECT().Delete(f.ctx, f.table, gomock.Any(), gomock.Any()).Return(nil).Times(1)
		err := f.delete(f.ctx, start, end)
		require.NoError(t, err)
	})

	t.Run("error in delete", func(t *testing.T) {
		mockDB.EXPECT().Delete(f.ctx, f.table, gomock.Any(), gomock.Any()).Return(errors.New("db error")).Times(1)
		err := f.delete(f.ctx, start, end)
		require.Error(t, err)
	})
}
