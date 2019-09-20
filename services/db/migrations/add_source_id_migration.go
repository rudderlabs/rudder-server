package main

import (
	"database/sql"
	"time"

	"github.com/lib/pq"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/utils/misc"
	uuid "github.com/satori/go.uuid"
)

func main() {
	psqlInfo := jobsdb.GetConnectionString()
	dbHandle, _ := sql.Open("postgres", psqlInfo)

	// gatewayTables := helpers.GetTableNamesWithPrefix(dbHandle, "gw_jobs_")

	// set a for loop
	for i := 1; i <= 800000; i++ {
		txn, _ := dbHandle.Begin()

		stmt, err := txn.Prepare(pq.CopyIn("rt_jobs_1", "uuid", "source_id", "custom_val", "event_payload",
			"created_at", "expire_at"))
		misc.AssertError(err)

		defer stmt.Close()
		_, err = stmt.Exec(uuid.NewV4(), "1R0IhjSwDt6m3KsYgQIF3AOEp6W", "GA", "{\"header\":{},\"payload\":{\"t\":\"event\",\"v\":\"1\",\"ea\":\"Test Track\",\"ec\":\"opq\",\"el\":\"opq\",\"ev\":\"5\",\"cid\":\"7e0e8c08-993a-4e44-a5d4-23afbb59fe9a\",\"tid\":\"test\"},\"user_id\":\"7e0e8c08-993a-4e44-a5d4-23afbb59fe9a\",\"endpoint\":\"a\",\"request_config\":{\"request-format\":\"PARAMS\",\"request_method\":\"GET\"}}",
			time.Now(), time.Now())
		misc.AssertError(err)
		_, err = stmt.Exec()
		misc.AssertError(err)
		err = txn.Commit()
		misc.AssertError(err)
	}
}
