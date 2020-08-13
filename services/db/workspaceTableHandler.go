package db

import (
	"database/sql"
	"fmt"
	"log"
	"strconv"

	"github.com/rudderlabs/rudder-server/config"
)

var (
	host, user, password, dbname string
	port                         int
)

type WorkspaceDataT struct {
	token      string
	created_at string
	parameters string
}

func loadOriginalDBConfig(originalDBname string) {
	host = config.GetEnv("JOBS_DB_HOST", "localhost")
	user = config.GetEnv("JOBS_DB_USER", "ubuntu")
	dbname = originalDBname
	port, _ = strconv.Atoi(config.GetEnv("JOBS_DB_PORT", "5432"))
	password = config.GetEnv("JOBS_DB_PASSWORD", "ubuntu") // Reading secrets from

}

func loadDBConfig() {
	host = config.GetEnv("JOBS_DB_HOST", "localhost")
	user = config.GetEnv("JOBS_DB_USER", "ubuntu")
	dbname = config.GetEnv("JOBS_DB_DB_NAME", "ubuntu")
	port, _ = strconv.Atoi(config.GetEnv("JOBS_DB_PORT", "5432"))
	password = config.GetEnv("JOBS_DB_PASSWORD", "ubuntu") // Reading secrets from

}

func createWorkspaceTable(dbHandle *sql.DB) {
	sqlStatement := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS workspace (
		token TEXT PRIMARY KEY,
		created_at TIMESTAMP NOT NULL,
		parameters JSONB);`)

	_, err := dbHandle.Exec(sqlStatement)
	if err != nil {
		panic(err)
	}
}

func GetConnectionString() string {
	return fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)

}

func createDBConnection() *sql.DB {
	psqlInfo := GetConnectionString()
	var err error
	dbHandle, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		panic(err)
	}

	err = dbHandle.Ping()
	if err != nil {
		panic(err)
	}
	return dbHandle
}

func closeDBConnection(handle *sql.DB) {
	err := handle.Close()
	if err != nil {
		panic(err)
	}
}

func getWorkspaceData(originalDBname string) WorkspaceDataT {
	workspaceData := WorkspaceDataT{}
	loadOriginalDBConfig(originalDBname)
	ogDBhandle := createDBConnection()
	sqlStatement := fmt.Sprintf(`SELECT * FROM workspace`)
	rows, err := ogDBhandle.Query(sqlStatement)
	if err != nil {
		panic(err)
	}
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&workspaceData.token, &workspaceData.created_at, &workspaceData.parameters)
		if err != nil {
			log.Fatal(err)
		}
	}
	err = rows.Err()
	if err != nil {
		log.Fatal(err)
	}
	closeDBConnection(ogDBhandle)
	return workspaceData
}
