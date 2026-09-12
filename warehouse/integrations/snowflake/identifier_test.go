package snowflake

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	sqlmw "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestIdentifierQuoting(t *testing.T) {
	require.Equal(t, `"schema"";drop schema public;--"`, quoteIdentifier(`schema";drop schema public;--`))
	require.Equal(t, `"schema""x"."table"";drop table x;--"`, quoteQualifiedIdentifier(`schema"x`, `table";drop table x;--`))
	require.Equal(t, `"ROW_ID", "COLUMN""NAME", "TABLE_NAME"`, quoteColumnList(`ROW_ID, COLUMN"NAME, TABLE_NAME`))
}

func TestTableManagerQuotesIdentifiers(t *testing.T) {
	manager := newStandardTableManager()
	query := manager.createTableQuery(quoteIdentifier(`schema"x`), `schema"x`, `table";drop table x;--`, model.TableSchema{
		`id";drop table x;--`: "string",
	})
	require.Contains(t, query, `"schema""x"."table"";drop table x;--"`)
	require.Contains(t, query, `"id"";drop table x;--" varchar`)
	require.False(t, strings.Contains(query, `%q`))
}

func TestDeleteByQuotesTableAndColumnIdentifiers(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	conf := config.New()
	conf.Set("Warehouse.snowflake.enableDeleteByJobs", true)
	sf := New(conf, logger.NOP, stats.NOP)
	sf.DB = sqlmw.New(db)
	sf.Namespace = `schema";drop schema public;--`

	params := whutils.DeleteByParams{
		JobRunId:  "job-run-id",
		TaskRunId: "task-run-id",
		SourceId:  "source-id",
		StartTime: time.Unix(100, 0),
	}
	tableName := `x";DROP TABLE y;--`
	expectedQuery := fmt.Sprintf(`DELETE FROM %s
		WHERE
			%s <> ? AND
			%s <> ? AND
			%s = ? AND
			%s < ?`,
		quoteQualifiedIdentifier(sf.Namespace, tableName),
		quoteIdentifier("CONTEXT_SOURCES_JOB_RUN_ID"),
		quoteIdentifier("CONTEXT_SOURCES_TASK_RUN_ID"),
		quoteIdentifier("CONTEXT_SOURCE_ID"),
		quoteIdentifier("RECEIVED_AT"),
	)

	mock.ExpectExec(regexp.QuoteMeta(expectedQuery)).
		WithArgs(params.JobRunId, params.TaskRunId, params.SourceId, params.StartTime).
		WillReturnResult(sqlmock.NewResult(0, 1))

	require.NoError(t, sf.DeleteBy(context.Background(), []string{tableName}, params))
	require.NoError(t, mock.ExpectationsWereMet())
}
