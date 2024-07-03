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

func setup(t *testing.T) (*gomock.Controller, *db.MockDB, *config.Config, *aggregator.MockAggregator, *Flusher) {
	ctrl := gomock.NewController(t)
	mockDB := db.NewMockDB(ctrl)
	mockAggregator := aggregator.NewMockAggregator(ctrl)
	conf := config.New()

	log := logger.NOP
	inputStats := stats.NOP
	table := "test_table"
	reportingURL := "http://localhost"

	conf.Set("Reporting.flusher.maxOpenConnections", 2)
	conf.Set("Reporting.flusher.sleepInterval", 1*time.Second)
	conf.Set("Reporting.flusher.flushWindow", 1*time.Minute)
	conf.Set("Reporting.flusher.recentExclusionWindowInSeconds", 30*time.Second)
	conf.Set("Reporting.flusher.minConcurrentRequests", 2)
	conf.Set("Reporting.flusher.maxConcurrentRequests", 5)
	conf.Set("Reporting.flusher.batchSizeFromDB", 4)
	conf.Set("Reporting.flusher.batchSizeToReporting", 2)
	conf.Set("Reporting.flusher.aggressiveFlushEnabled", false)
	conf.Set("Reporting.flusher.lagThresholdForAggresiveFlushInMins", 10*time.Minute)

	f := NewFlusher(mockDB, log, inputStats, conf, table, reportingURL, mockAggregator, "core")

	return ctrl, mockDB, conf, mockAggregator, f
}

func TestNewFlusher(t *testing.T) {
	ctrl, mockDB, _, mockAggregator, f := setup(t)
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
	ctrl, mockDB, _, mockAggregator, f := setup(t)
	defer ctrl.Finish()

	midOfCurHour := time.Now().UTC().Add(-time.Hour).Truncate(time.Hour).Add(30 * time.Minute)
	start := midOfCurHour.Add(-5 * time.Minute)
	end := start.Add(1 * time.Minute)

	jsonPayloads := []json.RawMessage{
		json.RawMessage(`{"key1":"value1"}`),
		json.RawMessage(`{"key2":"value2"}`),
	}

	t.Run("flush", func(t *testing.T) {
		// 1. Get time range
		mockDB.EXPECT().GetStart(gomock.Any(), f.table).Return(start, nil).Times(2)

		// 2. Get aggregate reports
		mockAggregator.EXPECT().Aggregate(gomock.Any(), start, end).Return(jsonPayloads, nil).Times(1)

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

func TestFlushAggressively(t *testing.T) {
	ctrl, mockDB, conf, _, f := setup(t)
	defer ctrl.Finish()

	ctx := context.Background()

	t.Run("flush aggresively", func(t *testing.T) {
		conf.Set("Reporting.flusher.aggressiveFlushEnabled", true)
		oneHourAgo := time.Now().UTC().Add(-time.Hour)

		mockDB.EXPECT().GetStart(gomock.Any(), f.table).Return(oneHourAgo, nil).Times(1)

		flushAggresively := f.FlushAggressively(ctx)
		assert.True(t, flushAggresively)
	})
}

func TestGetRange(t *testing.T) {
	ctrl, mockDB, conf, _, f := setup(t)
	defer ctrl.Finish()

	recentExclusionWindow := 30 * time.Second
	flushWindow := 2 * time.Minute

	conf.Set("Reporting.flusher.recentExclusionWindowInSeconds", recentExclusionWindow)
	conf.Set("Reporting.flusher.flushWindow", flushWindow)

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
