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
		r, _ := CreateRunner(ctx, table, mockLogger, mockStats, conf)
		assert.NotNil(t, r)
		assert.Equal(t, table, r.table)
	})

	t.Run("should use same flusher for a table", func(t *testing.T) {
		table := "tracked_users_reports"
		r1, _ := CreateRunner(ctx, table, mockLogger, mockStats, conf)
		r2, _ := CreateRunner(ctx, table, mockLogger, mockStats, conf)
		assert.Equal(t, r1.flusher, r2.flusher)
	})

	t.Run("should return error for invalid tables", func(t *testing.T) {
		table := "invalid_table"
		_, err := CreateRunner(ctx, table, mockLogger, mockStats, conf)
		assert.Error(t, err)
	})
}
