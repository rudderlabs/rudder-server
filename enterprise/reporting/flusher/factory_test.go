package flusher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

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
	module := "core"

	ctx := context.Background()

	t.Run("successful creation", func(t *testing.T) {
		table := "tracked_users_reports"
		r, _ := CreateRunner(ctx, table, mockLogger, mockStats, conf, module)
		assert.NotNil(t, r)
	})

	t.Run("should return error for invalid tables", func(t *testing.T) {
		table := "invalid_table"
		_, err := CreateRunner(ctx, table, mockLogger, mockStats, conf, module)
		assert.Error(t, err)
	})
}
