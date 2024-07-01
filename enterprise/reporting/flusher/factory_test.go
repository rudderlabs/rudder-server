package flusher

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
)

func TestCreateFlusher(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockLogger := logger.NOP
	mockStats := stats.NOP
	conf := config.New()

	ctx := context.Background()

	t.Run("successful creation", func(t *testing.T) {
		table := "tracked_users_reports"
		f, _ := CreateFlusher(ctx, table, mockLogger, mockStats, conf)
		assert.NotNil(t, f)
		assert.Equal(t, table, f.table)
	})

	t.Run("should return same flusher for a table", func(t *testing.T) {
		table := "tracked_users_reports"
		f1, _ := CreateFlusher(ctx, table, mockLogger, mockStats, conf)
		f2, _ := CreateFlusher(ctx, table, mockLogger, mockStats, conf)
		assert.Equal(t, f1, f2)
	})

	t.Run("should return error for invalid tables", func(t *testing.T) {
		table := "invalid_table"
		_, err := CreateFlusher(ctx, table, mockLogger, mockStats, conf)
		assert.Error(t, err)
	})
}
