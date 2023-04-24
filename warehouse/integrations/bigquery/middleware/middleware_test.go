package middleware_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	mock_logger "github.com/rudderlabs/rudder-server/mocks/utils/logger"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/bigquery"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/bigquery/middleware"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/testhelper"
	"github.com/rudderlabs/rudder-server/warehouse/logfield"
	"github.com/stretchr/testify/require"
)

func TestQueryWrapper(t *testing.T) {
	if _, exists := os.LookupEnv(testhelper.BigqueryIntegrationTestCredentials); !exists {
		t.Skipf("Skipping %s as %s is not set", t.Name(), testhelper.BigqueryIntegrationTestCredentials)
	}

	credentials, err := testhelper.BigqueryCredentials()
	require.NoError(t, err)

	db, err := bigquery.Connect(context.TODO(), &credentials)
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
