package query_wrapper_test

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/ory/dockertest/v3"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource"
	"github.com/rudderlabs/rudder-server/mocks/utils/logger"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/query_wrapper"
	"github.com/rudderlabs/rudder-server/warehouse/logfield"
	"github.com/stretchr/testify/require"
)

func TestQueryWrapper(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pgResource, err := resource.SetupPostgres(pool, t)
	require.NoError(t, err)

	testCases := []struct {
		name               string
		executionTimeInSec time.Duration
		wantLog            bool
	}{
		{
			name:               "slow query",
			executionTimeInSec: 500 * time.Second,
			wantLog:            true,
		},
		{
			name:               "fast query",
			executionTimeInSec: 1 * time.Second,
			wantLog:            false,
		},
	}

	var (
		ctx            = context.Background()
		query          = "SELECT 1"
		queryThreshold = 300 * time.Second
		keysAndValues  = []any{"key1", "value2", "key2", "value2"}
	)

	for _, tc := range testCases {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			mockCtrl := gomock.NewController(t)
			defer mockCtrl.Finish()

			mockLogger := mock_logger.NewMockLogger(mockCtrl)

			qw := query_wrapper.NewSQLQueryWrapper(
				pgResource.DB,
				query_wrapper.WithQueryThresholdInSec(queryThreshold),
				query_wrapper.WithLogger(mockLogger),
				query_wrapper.WithSince(func(time.Time) time.Duration {
					return tc.executionTimeInSec
				}),
				query_wrapper.WithKeyAndValues(keysAndValues...),
			)

			kvs := []any{
				logfield.Query, query,
				logfield.QueryExecutionTimeInSec, int64(tc.executionTimeInSec.Seconds()),
			}
			kvs = append(kvs, keysAndValues...)

			if tc.wantLog {
				mockLogger.EXPECT().Infow(query_wrapper.ExecutingQuery, kvs).Times(6)
			} else {
				mockLogger.EXPECT().Infow(query_wrapper.ExecutingQuery, kvs).Times(0)
			}

			_, err = qw.Exec(query)
			require.NoError(t, err)

			_, err = qw.ExecContext(ctx, query)
			require.NoError(t, err)

			_, err = qw.Query(query)
			require.NoError(t, err)

			_, err = qw.QueryContext(ctx, query)
			require.NoError(t, err)

			_ = qw.QueryRow(query)
			require.NoError(t, err)

			_ = qw.QueryRowContext(ctx, query)
			require.NoError(t, err)
		})
	}
}
