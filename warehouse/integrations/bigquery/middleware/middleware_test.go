package middleware_test

import (
	"context"
	"testing"
	"time"

	bqHelper "github.com/rudderlabs/rudder-server/warehouse/integrations/bigquery/testhelper"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/logger/mock_logger"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/bigquery"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/bigquery/middleware"
	"github.com/rudderlabs/rudder-server/warehouse/logfield"
)

func TestQueryWrapper(t *testing.T) {
	if !bqHelper.IsBQTestCredentialsAvailable() {
		t.Skipf("Skipping %s as %s is not set", t.Name(), bqHelper.TestKey)
	}

	bqTestCredentials, err := bqHelper.GetBQTestCredentials()
	require.NoError(t, err)

	ctx := context.Background()

	db, err := bigquery.Connect(ctx, &bigquery.BQCredentials{
		ProjectID:   bqTestCredentials.ProjectID,
		Credentials: bqTestCredentials.Credentials,
	})
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

			qw := middleware.New(
				db,
				middleware.WithSlowQueryThreshold(queryThreshold),
				middleware.WithLogger(mockLogger),
				middleware.WithKeyAndValues(keysAndValues...),
				middleware.WithSince(func(time.Time) time.Duration {
					return tc.executionTimeInSec
				}),
			)

			queryStatement := "SELECT 1;"
			query := db.Query(queryStatement)

			kvs := []any{
				logfield.Query, queryStatement,
				logfield.QueryExecutionTime, tc.executionTimeInSec,
			}
			kvs = append(kvs, keysAndValues...)

			if tc.wantLog {
				mockLogger.EXPECT().Infow("executing query", kvs).Times(2)
			} else {
				mockLogger.EXPECT().Infow("executing query", kvs).Times(0)
			}

			_, err := qw.Run(ctx, query)
			require.NoError(t, err)

			_, err = qw.Read(ctx, query)
			require.NoError(t, err)
		})
	}
}
