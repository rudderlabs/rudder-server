package mssql

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/logger"

	sqlmw "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestIdentifierQuoting(t *testing.T) {
	require.Equal(t, `[schema]];drop schema public;--]`, quoteIdentifier(`schema];drop schema public;--`))
	require.Equal(t, `[schema]]x].[table]];drop table x;--]`, quoteQualifiedIdentifier(`schema]x`, `table];drop table x;--`))
	require.Equal(t, `[row_id], [column]]name], [table_name]`, quoteColumnList(`row_id, column]name, table_name`))

	columns := ColumnsWithDataTypes(model.TableSchema{
		`id];drop table x;--`: "string",
		`received]at`:         "datetime",
	}, "")
	require.Contains(t, columns, `[id]];drop table x;--] nvarchar(512)`)
	require.Contains(t, columns, `[received]]at] datetimeoffset`)
	require.False(t, strings.Contains(columns, `"id];drop table x;--"`))
}

func TestDropDanglingStagingTablesUsesSingleQuotedLiterals(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	ms := &MSSQL{
		db:        sqlmw.New(db),
		namespace: `namespace'with-quote`,
		logger:    logger.NOP,
	}
	expectedQuery := fmt.Sprintf(`
		select
		  table_name
		from
		  information_schema.tables
		where
		  table_schema = %s
		  AND table_name like %s;
	`,
		quoteStringLiteral(ms.namespace),
		quoteStringLiteral(fmt.Sprintf(`%s%%`, warehouseutils.StagingTablePrefix(provider))),
	)

	mock.ExpectQuery(regexp.QuoteMeta(expectedQuery)).
		WillReturnRows(sqlmock.NewRows([]string{"table_name"}))

	require.NoError(t, ms.dropDanglingStagingTables(context.Background()))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCreateTableGuardUsesQuotedName(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	ms := &MSSQL{db: sqlmw.New(db), namespace: `ns]x`, logger: logger.NOP}

	mock.ExpectExec(regexp.QuoteMeta(`IF NOT EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[ns]]x].[my.table]]x]') AND type = N'U')
		CREATE TABLE [ns]]x].[my.table]]x] ( [id] nvarchar(512) )`)).
		WillReturnResult(sqlmock.NewResult(0, 0))

	require.NoError(t, ms.createTable(context.Background(), `my.table]x`, model.TableSchema{"id": "string"}))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestAddColumnsGuardUsesQuotedName(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	ms := &MSSQL{db: sqlmw.New(db), namespace: `ns]x`, logger: logger.NOP}

	mock.ExpectExec(`OBJECT_ID = OBJECT_ID\(N'\[ns\]\]x\]\.\[my\.table\]\]x\]'\)\s+AND name = N'col''x'\s+\)\s+ALTER TABLE\s+\[ns\]\]x\]\.\[my\.table\]\]x\]\s+ADD \[col'x\] nvarchar\(512\);`).
		WillReturnResult(sqlmock.NewResult(0, 0))

	require.NoError(t, ms.AddColumns(context.Background(), `my.table]x`, []warehouseutils.ColumnInfo{{Name: `col'x`, Type: "string"}}))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestDropTableQuotesQualifiedName(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	ms := &MSSQL{db: sqlmw.New(db), namespace: `ns]x`, logger: logger.NOP}

	mock.ExpectExec(regexp.QuoteMeta(`DROP TABLE [ns]]x].[my.table]]x]`)).
		WillReturnResult(sqlmock.NewResult(0, 0))

	require.NoError(t, ms.DropTable(context.Background(), `my.table]x`))
	require.NoError(t, mock.ExpectationsWereMet())
}
