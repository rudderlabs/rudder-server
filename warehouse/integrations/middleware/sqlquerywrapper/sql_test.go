package sqlquerywrapper

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	rslogger "github.com/rudderlabs/rudder-go-kit/logger"

	"github.com/google/uuid"

	"github.com/golang/mock/gomock"
	"github.com/ory/dockertest/v3"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource"
	mock_logger "github.com/rudderlabs/rudder-server/mocks/utils/logger"
	"github.com/rudderlabs/rudder-server/warehouse/logfield"
	"github.com/stretchr/testify/require"
)

func TestQueryWrapper(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pgResource, err := resource.SetupPostgres(pool, t)
	require.NoError(t, err)

	t.Log("db:", pgResource.DBDsn)

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
			mockCtrl := gomock.NewController(t)
			defer mockCtrl.Finish()

			mockLogger := mock_logger.NewMockLogger(mockCtrl)

			qw := New(
				pgResource.DB,
				WithSlowQueryThreshold(queryThreshold),
				WithLogger(mockLogger),
				WithKeyAndValues(keysAndValues...),
			)
			qw.since = func(time.Time) time.Duration {
				return tc.executionTimeInSec
			}

			query := "SELECT 1;"

			kvs := []any{
				logfield.Query, query,
				logfield.QueryExecutionTime, tc.executionTimeInSec,
			}
			kvs = append(kvs, keysAndValues...)

			if tc.wantLog {
				mockLogger.EXPECT().Infow("executing query", kvs).Times(6)
			} else {
				mockLogger.EXPECT().Infow("executing query", kvs).Times(0)
			}

			_, err := qw.Exec(query)
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

		t.Run(tc.name+" with secrets", func(t *testing.T) {
			mockCtrl := gomock.NewController(t)
			defer mockCtrl.Finish()

			mockLogger := mock_logger.NewMockLogger(mockCtrl)

			qw := New(
				pgResource.DB,
				WithSlowQueryThreshold(queryThreshold),
				WithLogger(mockLogger),
				WithKeyAndValues(keysAndValues...),
				WithSecretsRegex(map[string]string{
					"PASSWORD '[^']*'": "PASSWORD '***'",
				}),
			)
			qw.since = func(time.Time) time.Duration {
				return tc.executionTimeInSec
			}

			t.Run("DB", func(t *testing.T) {
				user := fmt.Sprintf("test_user_%d", uuid.New().ID())

				createKvs := []any{
					logfield.Query, fmt.Sprintf("CREATE USER %s;", user),
					logfield.QueryExecutionTime, tc.executionTimeInSec,
				}
				alterKvs := []any{
					logfield.Query, fmt.Sprintf("ALTER USER %s WITH PASSWORD '***';", user),
					logfield.QueryExecutionTime, tc.executionTimeInSec,
				}

				createKvs = append(createKvs, keysAndValues...)
				alterKvs = append(alterKvs, keysAndValues...)

				if tc.wantLog {
					mockLogger.EXPECT().Infow("executing query", createKvs).Times(1)
					mockLogger.EXPECT().Infow("executing query", alterKvs).Times(1)
				} else {
					mockLogger.EXPECT().Infow("executing query", []any{}).Times(0)
				}

				_, err := qw.Exec(fmt.Sprintf("CREATE USER %s;", user))
				require.NoError(t, err)

				_, err = qw.Exec(fmt.Sprintf("ALTER USER %s WITH PASSWORD 'test_password';", user))
				require.NoError(t, err)
			})

			t.Run("Tx", func(t *testing.T) {
				tx, err := qw.Begin()
				require.NoError(t, err)

				txWithOptions, err := qw.BeginTx(ctx, &sql.TxOptions{})
				require.NoError(t, err)

				subTestCases := []struct {
					name string
					tx   *Tx
				}{
					{
						name: "Without options",
						tx:   tx,
					},
					{
						name: "With options",
						tx:   txWithOptions,
					},
				}

				for _, stc := range subTestCases {
					stc := stc

					t.Run(stc.name, func(t *testing.T) {
						user := fmt.Sprintf("test_user_%d", uuid.New().ID())

						createKvs := []any{
							logfield.Query, fmt.Sprintf("CREATE USER %s;", user),
							logfield.QueryExecutionTime, tc.executionTimeInSec,
						}
						alterKvs := []any{
							logfield.Query, fmt.Sprintf("ALTER USER %s WITH PASSWORD '***';", user),
							logfield.QueryExecutionTime, tc.executionTimeInSec,
						}

						createKvs = append(createKvs, keysAndValues...)
						alterKvs = append(alterKvs, keysAndValues...)

						if tc.wantLog {
							mockLogger.EXPECT().Infow("executing query", createKvs).Times(1)
							mockLogger.EXPECT().Infow("executing query", alterKvs).Times(1)
							mockLogger.EXPECT().Warnw("commit threshold exceeded", keysAndValues...).Times(1)
						} else {
							mockLogger.EXPECT().Infow("executing query", []any{}).Times(0)
							mockLogger.EXPECT().Warnw("commit threshold exceeded", keysAndValues...).Times(0)
						}

						_, err = stc.tx.Exec(fmt.Sprintf("CREATE USER %s;", user))
						require.NoError(t, err)

						_, err = stc.tx.Exec(fmt.Sprintf("ALTER USER %s WITH PASSWORD 'test_password';", user))
						require.NoError(t, err)

						err = stc.tx.Commit()
						require.NoError(t, err)
					})
				}
			})
		})

		t.Run(tc.name+" with transaction", func(t *testing.T) {
			mockCtrl := gomock.NewController(t)
			defer mockCtrl.Finish()

			mockLogger := mock_logger.NewMockLogger(mockCtrl)

			qw := New(
				pgResource.DB,
				WithSlowQueryThreshold(queryThreshold),
				WithLogger(mockLogger),
				WithKeyAndValues(keysAndValues...),
			)
			qw.since = func(time.Time) time.Duration {
				return tc.executionTimeInSec
			}

			query := "SELECT 1;"

			kvs := []any{
				logfield.Query, query,
				logfield.QueryExecutionTime, tc.executionTimeInSec,
			}
			kvs = append(kvs, keysAndValues...)

			if tc.wantLog {
				mockLogger.EXPECT().Infow("executing query", kvs).Times(6)
				mockLogger.EXPECT().Warnw("rollback threshold exceeded", keysAndValues...).Times(1)
				mockLogger.EXPECT().Warnw("commit threshold exceeded", keysAndValues...).Times(6)
			} else {
				mockLogger.EXPECT().Infow("executing query", kvs).Times(0)
				mockLogger.EXPECT().Warnw("rollback threshold exceeded", keysAndValues...).Times(0)
				mockLogger.EXPECT().Warnw("commit threshold exceeded", keysAndValues...).Times(0)
			}

			t.Run("Exec", func(t *testing.T) {
				tx, err := qw.Begin()
				require.NoError(t, err)

				_, err = tx.Exec(query)
				require.NoError(t, err)

				err = tx.Commit()
				require.NoError(t, err)
			})

			t.Run("ExecContext", func(t *testing.T) {
				tx, err := qw.Begin()
				require.NoError(t, err)

				_, err = tx.ExecContext(ctx, query)
				require.NoError(t, err)

				err = tx.Commit()
				require.NoError(t, err)
			})

			t.Run("Query", func(t *testing.T) {
				tx, err := qw.Begin()
				require.NoError(t, err)

				_, err = tx.Query(query)
				require.NoError(t, err)

				err = tx.Commit()
				require.NoError(t, err)
			})

			t.Run("QueryContext", func(t *testing.T) {
				tx, err := qw.Begin()
				require.NoError(t, err)

				_, err = tx.QueryContext(ctx, query)
				require.NoError(t, err)

				err = tx.Commit()
				require.NoError(t, err)
			})

			t.Run("QueryRow", func(t *testing.T) {
				tx, err := qw.Begin()
				require.NoError(t, err)

				_ = tx.QueryRow(query)
				require.NoError(t, err)

				err = tx.Commit()
				require.NoError(t, err)
			})

			t.Run("QueryRowContext", func(t *testing.T) {
				tx, err := qw.Begin()
				require.NoError(t, err)

				_ = tx.QueryRowContext(ctx, query)
				require.NoError(t, err)

				err = tx.Commit()
				require.NoError(t, err)
			})

			t.Run("Rollback", func(t *testing.T) {
				tx, err := qw.Begin()
				require.NoError(t, err)

				_ = tx.Rollback()
				require.NoError(t, err)
			})
		})

		t.Run("Round trip", func(t *testing.T) {
			qw := New(
				pgResource.DB,
				WithSlowQueryThreshold(queryThreshold),
				WithLogger(rslogger.NOP),
			)
			qw.since = func(time.Time) time.Duration {
				return tc.executionTimeInSec
			}

			table := fmt.Sprintf("test_table_%d", uuid.New().ID())

			t.Run("Normal operations", func(t *testing.T) {
				_, err := qw.ExecContext(ctx, fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (id INT);", table))
				require.NoError(t, err)

				_, err = qw.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s (id) VALUES (1);", table))
				require.NoError(t, err)

				var count int
				err = qw.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s;", table)).Scan(&count)
				require.NoError(t, err)
				require.Equal(t, 1, count)
			})

			t.Run("On Rollback", func(t *testing.T) {
				tx, err := qw.BeginTx(ctx, &sql.TxOptions{})
				require.NoError(t, err)

				_, err = tx.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s (id) VALUES (2);", table))
				require.NoError(t, err)

				var count int
				err = tx.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s;", table)).Scan(&count)
				require.NoError(t, err)
				require.Equal(t, 2, count)

				err = tx.Rollback()
				require.NoError(t, err)

				err = qw.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s;", table)).Scan(&count)
				require.NoError(t, err)
				require.Equal(t, 1, count)
			})

			t.Run("On Commit", func(t *testing.T) {
				tx, err := qw.BeginTx(ctx, &sql.TxOptions{})
				require.NoError(t, err)

				_, err = tx.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s (id) VALUES (2);", table))
				require.NoError(t, err)

				var count int
				err = tx.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s;", table)).Scan(&count)
				require.NoError(t, err)
				require.Equal(t, 2, count)

				err = tx.Commit()
				require.NoError(t, err)

				err = qw.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s;", table)).Scan(&count)
				require.NoError(t, err)
				require.Equal(t, 2, count)
			})
		})
	}
}
