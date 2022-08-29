package postgres

import (
	"database/sql"
	"fmt"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"strings"
)

type QueryParams struct {
	txn                 *sql.Tx
	db                  *sql.DB
	query               string
	enableWithQueryPlan bool
}

func (q *QueryParams) validate() (err error) {
	if q.txn == nil && q.db == nil {
		return fmt.Errorf("both txn and db are nil")
	}
	return
}

// handleExec
// Print execution plan if enableWithQueryPlan is set to true else return result set.
// Currently, these statements are supported by EXPLAIN
// Any INSERT, UPDATE, DELETE whose execution plan you wish to see.
func handleExec(e *QueryParams) (err error) {
	sqlStatement := e.query

	if err = e.validate(); err != nil {
		err = fmt.Errorf("[WH][POSTGRES] Not able to handle query execution for statement: %s as both txn and db are nil", sqlStatement)
		return
	}

	if e.enableWithQueryPlan {
		sqlStatement := "EXPLAIN " + e.query

		var rows *sql.Rows
		if e.txn != nil {
			rows, err = e.txn.Query(sqlStatement)
		} else if e.db != nil {
			rows, err = e.db.Query(sqlStatement)
		}
		if err != nil {
			err = fmt.Errorf("[WH][POSTGRES] error occurred while handling transaction for query: %s with err: %w", sqlStatement, err)
			return
		}
		defer rows.Close()

		var response []string
		for rows.Next() {
			var s string
			if err = rows.Scan(&s); err != nil {
				err = fmt.Errorf("[WH][POSTGRES] Error occurred while processing destination revisionID query %+v with err: %w", e, err)
				return
			}
			response = append(response, s)
		}
		pkgLogger.Infof(fmt.Sprintf(`[WH][POSTGRES] Execution Query plan for statement: %s is %s`, sqlStatement, strings.Join(response, `
`)))
	}
	if e.txn != nil {
		_, err = e.txn.Exec(sqlStatement)
	} else if e.db != nil {
		_, err = e.db.Exec(sqlStatement)
	}
	return
}

func dropDanglingTablesSQLStatement(namespace string) string {
	return fmt.Sprintf(`
		select
		  table_name
		from
		  information_schema.tables
		where
		  table_schema = '%s'
		  AND table_name like '%s%s';
	`,
		namespace,
		warehouseutils.StagingTablePrefix(provider),
		"%",
	)
}

func fetchSchemaSQLStatement(namespace string) string {
	return fmt.Sprintf(`
		select
		  t.table_name,
		  c.column_name,
		  c.data_type
		from
		  INFORMATION_SCHEMA.TABLES t
		  LEFT JOIN INFORMATION_SCHEMA.COLUMNS c ON (
			t.table_name = c.table_name
			and t.table_schema = c.table_schema
		  )
		WHERE
		  t.table_schema = '%s'
		  and t.table_name not like '%s%s'
	`,
		namespace,
		warehouseutils.StagingTablePrefix(provider),
		"%",
	)
}
