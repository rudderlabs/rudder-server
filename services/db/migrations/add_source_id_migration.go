package main

import (
	"database/sql"
	"fmt"

	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/tests/helpers"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

func main() {
	psqlInfo := jobsdb.GetConnectionString()
	dbHandle, err := sql.Open("postgres", psqlInfo)
	misc.AssertError(err)

	gatewayTables := helpers.GetTableNamesWithPrefix(dbHandle, "gw_jobs_")

	for _, gwtable := range gatewayTables {
		_, err := dbHandle.Exec(fmt.Sprintf(`ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s JSONB NOT NULL DEFAULT '{"source_id": ""}'::jsonb;`, gwtable, "parameters"))
		misc.AssertError(err)
		_, err = dbHandle.Exec(fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN %s DROP DEFAULT;`, gwtable, "parameters"))
		misc.AssertError(err)
	}

	routerTables := helpers.GetTableNamesWithPrefix(dbHandle, "rt_jobs_")

	for _, rttable := range routerTables {
		_, err := dbHandle.Exec(fmt.Sprintf(`ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s JSONB NOT NULL DEFAULT ''{"source_id": ""}'::jsonb;`, rttable, "parameters"))
		misc.AssertError(err)
		_, err = dbHandle.Exec(fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN %s DROP DEFAULT;`, rttable, "parameters"))
		misc.AssertError(err)
	}
}
