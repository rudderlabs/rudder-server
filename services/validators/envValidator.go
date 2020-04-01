package validators

import (
	"database/sql"
	"fmt"
	"strconv"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

var dbHandle *sql.DB

const (
	//This is integer representation of Postgres version.
	//For ex, integer representation of version 9.6.3 is 90603
	//Minimum postgres version needed for rudder server is 10
	minPostgresVersion = 100000
)

func createWorkspaceTable() {
	//Create table to store workspace token
	sqlStatement := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS workspace (
		token TEXT PRIMARY KEY,
		created_at TIMESTAMP NOT NULL,
		parameters JSONB);`)

	_, err := dbHandle.Exec(sqlStatement)
	if err != nil {
		panic(err)
	}
}

func insertTokenIfNotExists() {
	//Read entries, if there are no entries insert hashed current workspace token
	var totalCount int
	sqlStatement := fmt.Sprintf(`SELECT COUNT(*) FROM workspace`)
	row := dbHandle.QueryRow(sqlStatement)
	err := row.Scan(&totalCount)
	if err != nil {
		panic(err)
	}

	if totalCount > 0 {
		return
	}

	//There are no entries in the table, hash current workspace token and insert
	sqlStatement = fmt.Sprintf(`INSERT INTO workspace (token, created_at)
									   VALUES ($1, $2)`)
	stmt, err := dbHandle.Prepare(sqlStatement)
	if err != nil {
		panic(err)
	}
	defer stmt.Close()

	_, err = stmt.Exec(misc.GetMD5Hash(config.GetWorkspaceToken()), time.Now())
	if err != nil {
		panic(err)
	}
}

func getWorkspaceFromDB() string {
	sqlStatement := fmt.Sprintf(`SELECT token FROM workspace order by created_at desc limit 1`)
	var token string
	row := dbHandle.QueryRow(sqlStatement)
	err := row.Scan(&token)
	if err != nil {
		panic(err)
	}

	return token
}

func createDBConnection() {
	psqlInfo := jobsdb.GetConnectionString()
	var err error
	dbHandle, err = sql.Open("postgres", psqlInfo)
	if err != nil {
		panic(err)
	}

	err = dbHandle.Ping()
	if err != nil {
		panic(err)
	}
}

//ValidateEnv validates the current environment available for the server
func ValidateEnv() bool {
	createDBConnection()

	if !misc.IsPostgresCompatible(jobsdb.GetConnectionString()) {
		logger.Errorf("Rudder server needs postgres version >= 10. Exiting.")
		return false
	}

	//create workspace table and insert token
	createWorkspaceTable()
	insertTokenIfNotExists()

	workspaceTokenHashInDB := getWorkspaceFromDB()
	if workspaceTokenHashInDB == misc.GetMD5Hash(config.GetWorkspaceToken()) {
		dbHandle.Close()
		return true
	}

	//db connection should be closed. Else alter db fails.
	dbHandle.Close()

	logger.Warn("Previous workspace token is not same as the current workspace token. Parking current jobsdb aside and creating a new one")

	dbName := config.GetEnv("JOBS_DB_DB_NAME", "ubuntu")
	misc.ReplaceDB(dbName, dbName+"_"+strconv.FormatInt(time.Now().Unix(), 10)+"_"+workspaceTokenHashInDB)

	//New db created. Creating connection to the new db
	createDBConnection()

	//create workspace table and insert hashed token
	createWorkspaceTable()
	insertTokenIfNotExists()

	dbHandle.Close()

	return true
}
