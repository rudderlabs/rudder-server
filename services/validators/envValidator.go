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
	sqlStatement := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		token TEXT PRIMARY KEY,
		created_at TIMESTAMP NOT NULL);`, "workspace")

	_, err := dbHandle.Exec(sqlStatement)
	if err != nil {
		panic(err)
	}

	//Read entries, if there are no entries insert current workspace token
	var totalCount int
	sqlStatement = fmt.Sprintf(`SELECT COUNT(*) FROM %s`, "workspace")
	row := dbHandle.QueryRow(sqlStatement)
	err = row.Scan(&totalCount)
	if err != nil {
		panic(err)
	}

	if totalCount > 0 {
		return
	}

	//There are no entries in the table, insert current workspace token
	insertTokenIntoWorkspace(config.GetEnv("CONFIG_BACKEND_TOKEN", ""))
}

func insertTokenIntoWorkspace(token string) {
	sqlStatement := fmt.Sprintf(`INSERT INTO %s (token, created_at)
									   VALUES ($1, $2)`, "workspace")
	stmt, err := dbHandle.Prepare(sqlStatement)
	if err != nil {
		panic(err)
	}
	defer stmt.Close()

	_, err = stmt.Exec(token, time.Now())
	if err != nil {
		panic(err)
	}
}

//IsPostgresCompatible checks the if the version of postgres is greater than minPostgresVersion
func IsPostgresCompatible() bool {
	var versionNum int
	err := dbHandle.QueryRow("SHOW server_version_num;").Scan(&versionNum)
	if err != nil {
		return false
	}

	return versionNum >= minPostgresVersion
}

func didWorkspaceTokenChange() bool {
	sqlStatement := fmt.Sprintf(`SELECT token FROM %s order by created_at desc limit 1`, "workspace")
	var token string
	row := dbHandle.QueryRow(sqlStatement)
	err := row.Scan(&token)
	if err != nil {
		panic(err)
	}

	if token == config.GetEnv("CONFIG_BACKEND_TOKEN", "") {
		return false
	}

	return true
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

	if !IsPostgresCompatible() {
		logger.Errorf("Rudder server needs postgres version >= 10. Exiting.")
		return false
	}

	createWorkspaceTable()

	if !didWorkspaceTokenChange() {
		dbHandle.Close()
		return true
	}

	//db connection should be closed. Else alter db fails.
	dbHandle.Close()

	logger.Warn("Previous workspace token is not same as the current workspace token. Parking current jobsdb aside and creating a new one")

	dbName := config.GetEnv("JOBS_DB_DB_NAME", "ubuntu")
	misc.ReplaceDB(dbName, dbName+"_"+strconv.FormatInt(time.Now().Unix(), 10)+"_"+config.GetEnv("CONFIG_BACKEND_TOKEN", ""))

	//New db created. Creating connection to the new db
	createDBConnection()
	//create workspace table
	createWorkspaceTable()

	dbHandle.Close()

	return true
}
