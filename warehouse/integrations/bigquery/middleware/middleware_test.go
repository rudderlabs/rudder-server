package middleware

import (
	"context"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/api/option"

	"github.com/rudderlabs/rudder-go-kit/logger/mock_logger"

	"github.com/rudderlabs/rudder-server/warehouse/logfield"
)

func TestQueryWrapper(t *testing.T) {
	ctx := context.Background()
	db, err := bigquery.NewClient(ctx, "test-project", option.WithoutAuthentication())
	require.NoError(t, err)

	testCases := []struct {
		name          string
		executionTime time.Duration
		wantLog       bool
	}{
		{
			name:          "slow query",
			executionTime: 500 * time.Second,
			wantLog:       true,
		},
		{
			name:          "fast query",
			executionTime: 1 * time.Second,
			wantLog:       false,
		},
	}

	var (
		queryThreshold = 300 * time.Second
		keysAndValues  = []any{"key1", "value2", "key2", "value2"}
	)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockCtrl := gomock.NewController(t)
			defer mockCtrl.Finish()

			mockLogger := mock_logger.NewMockLogger(mockCtrl)

			qw := New(
				nil,
				WithSlowQueryThreshold(queryThreshold),
				WithLogger(mockLogger),
				WithKeyAndValues(keysAndValues...),
				WithSince(func(time.Time) time.Duration {
					return tc.executionTime
				}),
				func(client *Client) {
					client.runQuery = func(context.Context, *bigquery.Query) (*bigquery.Job, error) {
						return nil, nil
					}
					client.readQuery = func(context.Context, *bigquery.Query) (*bigquery.RowIterator, error) {
						return nil, nil
					}
				},
			)

			queryStatement := "SELECT 1;"
			query := db.Query(queryStatement)

			kvs := []any{
				logfield.Query, queryStatement,
				logfield.QueryExecutionTime, tc.executionTime,
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
