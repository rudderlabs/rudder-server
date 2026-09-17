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
	require.Equal(t, `'evil\\'`, quoteStringLiteral(`evil\`))
	require.Equal(t, `'evil\\''; DROP TABLE x; --'`, quoteStringLiteral(`evil\'; DROP TABLE x; --`))
}

func TestTableManagerQuotesIdentifiers(t *testing.T) {
	manager := newStandardTableManager()
	query := manager.createTableQuery(quoteIdentifier(`schema"x`), `table";drop table x;--`, model.TableSchema{
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
			"CONTEXT_SOURCES_JOB_RUN_ID" <> ? AND
			"CONTEXT_SOURCES_TASK_RUN_ID" <> ? AND
			"CONTEXT_SOURCE_ID" = ? AND
			"RECEIVED_AT" < ?`,
		quoteQualifiedIdentifier(sf.Namespace, tableName),
	)

	mock.ExpectExec(regexp.QuoteMeta(expectedQuery)).
		WithArgs(params.JobRunId, params.TaskRunId, params.SourceId, params.StartTime).
		WillReturnResult(sqlmock.NewResult(0, 1))

	require.NoError(t, sf.DeleteBy(context.Background(), []string{tableName}, params))
	require.NoError(t, mock.ExpectationsWereMet())
}
