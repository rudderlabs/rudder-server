package flusher

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/enterprise/reporting/flusher/aggregator"
	"github.com/rudderlabs/rudder-server/enterprise/reporting/flusher/db"
)

func resetFlusherInstances() {
	flusherMu.Lock()
	defer flusherMu.Unlock()
	for k := range flusherInstances {
		delete(flusherInstances, k)
	}
}

func setup(t *testing.T) (*gomock.Controller, *db.MockDB, *aggregator.MockAggregator, *Flusher) {
	resetFlusherInstances()

	ctrl := gomock.NewController(t)
	mockDB := db.NewMockDB(ctrl)
	mockAggregator := aggregator.NewMockAggregator(ctrl)
	conf := config.New()

	ctx := context.Background()
	log := logger.NOP
	inputStats := stats.NOP
	table := "test_table"
	reportingURL := "http://localhost"

	f := NewFlusher(ctx, mockDB, log, inputStats, conf, table, reportingURL, mockAggregator, "core")

	f.initStats(f.commonTags)

	return ctrl, mockDB, mockAggregator, f
}

func setupFlusher(f *Flusher) {
	f.batchSizeFromDB = config.SingleValueLoader(4)
	f.batchSizeToReporting = config.SingleValueLoader(2)
	f.minConcurrentRequests = config.SingleValueLoader(2)
	f.maxConcurrentRequests = config.SingleValueLoader(5)
	f.lagThresholdForAggresiveFlushInMins = config.SingleValueLoader(10 * time.Minute)
	f.flushWindow = config.SingleValueLoader(1 * time.Minute)
	f.maxOpenConnections = 2
	f.recentExclusionWindow = config.SingleValueLoader(30 * time.Second)
}

func TestNewFlusher(t *testing.T) {
	ctrl, mockDB, mockAggregator, f := setup(t)
	defer ctrl.Finish()

	t.Run("flusher creation", func(t *testing.T) {
		assert.NotNil(t, f)
		assert.Equal(t, mockDB, f.db)
		assert.Equal(t, mockAggregator, f.aggregator)
		assert.Equal(t, "test_table", f.table)
		assert.Equal(t, "http://localhost", f.reportingURL)
	})
}

func TestFlush(t *testing.T) {
	ctrl, mockDB, mockAggregator, f := setup(t)
	defer ctrl.Finish()

	midOfCurHour := time.Now().UTC().Add(-time.Hour).Truncate(time.Hour).Add(30 * time.Minute)
	start := midOfCurHour.Add(-5 * time.Minute)
	end := start.Add(1 * time.Minute)
	// reports := createReports(10)
	setupFlusher(f)

	jsonPayloads := []json.RawMessage{
		json.RawMessage(`{"key1":"value1"}`),
		json.RawMessage(`{"key2":"value2"}`),
	}

	t.Run("flush", func(t *testing.T) {
		// 1. Get time range
		mockDB.EXPECT().GetStart(gomock.Any(), f.table).Return(start, nil)

		// 2. Get aggregate reports
		mockAggregator.EXPECT().Aggregate(gomock.Any(), start, end).Return(jsonPayloads, 5, 2, nil).Times(1)

		// 3. Send reports is testing that http server is called
		called := false
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			called = true
			rw.WriteHeader(http.StatusOK)
		}))
		defer server.Close()
		f.reportingURL = server.URL

		// 4. Delete reports
		mockDB.EXPECT().Delete(gomock.Any(), f.table, start, end).Return(nil)

		err := f.Flush(context.Background())
		require.NoError(t, err)
		assert.True(t, called)
	})
}

func TestGetRange(t *testing.T) {
	ctrl, mockDB, _, f := setup(t)
	defer ctrl.Finish()

	recentExclusionWindow := 30 * time.Second
	flushWindow := 2 * time.Minute
	f.recentExclusionWindow = config.SingleValueLoader(30 * time.Second)
	f.flushWindow = config.SingleValueLoader(2 * time.Minute)

	currentTime := time.Now().UTC()
	startOfHour := currentTime.Truncate(time.Hour)
	midOfHour := startOfHour.Add(30 * time.Minute)
	ctx := context.Background()

	t.Run("getRange with valid range with full flush window", func(t *testing.T) {
		startTime := midOfHour.Add(-flushWindow - recentExclusionWindow - 1*time.Second)
		expectedEnd := startTime.Add(flushWindow)

		mockDB.EXPECT().GetStart(ctx, f.table).Return(startTime, nil).Times(1)

		start, end, valid, err := f.getRange(ctx, midOfHour)
		assert.NoError(t, err)
		assert.True(t, valid)
		assert.Equal(t, startTime, start)
		assert.Equal(t, expectedEnd, end)
	})

	t.Run("getRange with valid range with partial flush window near end of hour", func(t *testing.T) {
		startTime := startOfHour.Add(-flushWindow + 30*time.Second)
		expectedEnd := startOfHour

		mockDB.EXPECT().GetStart(ctx, f.table).Return(startTime, nil).Times(1)

		start, end, valid, err := f.getRange(ctx, startOfHour.Add(1*time.Minute))
		assert.NoError(t, err)
		assert.True(t, valid)
		assert.Equal(t, startTime, start)
		assert.Equal(t, expectedEnd, end)
	})

	t.Run("getRange with invalid range because entire flush window is not covered ", func(t *testing.T) {
		startTime := midOfHour.Add(-flushWindow + recentExclusionWindow)
		mockDB.EXPECT().GetStart(ctx, f.table).Return(startTime, nil).Times(1)

		_, _, valid, err := f.getRange(ctx, midOfHour)
		assert.NoError(t, err)
		assert.False(t, valid)
	})

	t.Run("failed getRange", func(t *testing.T) {
		mockDB.EXPECT().GetStart(ctx, f.table).Return(time.Time{}, errors.New("db error")).Times(1)

		_, _, _, err := f.getRange(ctx, currentTime)
		assert.Error(t, err)
	})
}
