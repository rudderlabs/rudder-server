package flusher

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
)

func TestCreateFlusher(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockLogger := logger.NOP
	mockStats := stats.NOP

	ctx := context.Background()

	t.Run("successful creation", func(t *testing.T) {
		table := "tracked_users_reports"
		f := CreateFlusher(ctx, table, mockLogger, mockStats)
		assert.NotNil(t, f)
		assert.Equal(t, table, f.table)
	})

	t.Run("should return same flusher for a table", func(t *testing.T) {
		table := "tracked_users_reports"
		f1 := CreateFlusher(ctx, table, mockLogger, mockStats)
		f2 := CreateFlusher(ctx, table, mockLogger, mockStats)
		assert.Equal(t, f1, f2)
	})

	t.Run("should return nil for invalid tables", func(t *testing.T) {
		table := "invalid_table"
		f := CreateFlusher(ctx, table, mockLogger, mockStats)
		assert.Equal(t, f, nil)
	})
}
