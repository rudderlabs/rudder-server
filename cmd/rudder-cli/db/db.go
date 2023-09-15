package db

import (
	"database/sql"
	"fmt"
	"os"
	"strconv"

	_ "github.com/lib/pq"
	"github.com/olekukonko/tablewriter"

	"github.com/rudderlabs/rudder-server/cmd/rudder-cli/util"
)

func getRowCount(db *sql.DB, tableName string) int64 {
	totalCount := int64(-1)
	sqlStatement := fmt.Sprintf(`SELECT COUNT(*) FROM %s`, tableName)
	row := db.QueryRow(sqlStatement)
	_ = row.Scan(&totalCount)
	return totalCount
}

func DisplayAllTables() {
	table := tablewriter.NewWriter(os.Stdout)

	table.SetAutoWrapText(false)
	table.SetHeader([]string{"Name", "Count"})

	db, err := util.GetDbHandle()
	if err != nil {
		// Write to console
		fmt.Println(err)
	}

	pgTableNames, err := util.GetAllTableNames(db)
	if err != nil {
		// Write to console
		fmt.Println(err)
		return
	}

	for i := 0; i < len(pgTableNames); i++ {
		tableName := pgTableNames[i]
		table.Append([]string{
			tableName,
			strconv.FormatInt(getRowCount(db, tableName), 10),
		})
	}

	table.Render()
}
