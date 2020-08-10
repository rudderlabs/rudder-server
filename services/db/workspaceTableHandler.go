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

func loadOriginalDBConfig(originalDBname string) {
	host = config.GetEnv("JOBS_DB_HOST", "localhost")
	user = config.GetEnv("JOBS_DB_USER", "ubuntu")
	dbname = originalDBname
	port, _ = strconv.Atoi(config.GetEnv("JOBS_DB_PORT", "5432"))
	password = config.GetEnv("JOBS_DB_PASSWORD", "ubuntu") // Reading secrets from

}

func loadDBConfig(originalDBname string) {
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

func createOriginalDBConnection() *sql.DB {
	psqlInfo := GetOriginalDBConnectionString()
	var err error
	dbHandle, err := sql.Open("postgres", psqlInfo)
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

func closeOriginalDBConnection(handle *sql.DB) {
	err := handle.Close()
	if err != nil {
		panic(err)
	}
}

func copyWorkspaceData(originalDBname string) {
	loadOriginalDBConfig(originalDBname)
	ogDBhandle := createOriginalDBConnection()
	insertWorkspaceParams := fmt.Sprintf("INSERT INTO jobsdb.workspace (SELECT * FROM \"%s\".workspace)", originalDBname)
	_, err := ogDBhandle.Exec(insertWorkspaceParams)
	if err != nil {
		panic(err)
	}
	closeOriginalDBConnection(ogDBhandle)
}

func getWorkspaceData(originalDBname string) (string, string, string) {
	loadOriginalDBConfig(originalDBname)
	ogDBhandle := createOriginalDBConnection()
	var token string
	var created_at string
	var parameters string
	sqlStatement := fmt.Sprintf(`SELECT * FROM workspace`)
	rows, err := ogDBhandle.Query(sqlStatement)
	if err != nil {
		panic(err)
	}
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&token, &created_at, &parameters)
		if err != nil {
			log.Fatal(err)
		}
	}
	err = rows.Err()
	if err != nil {
		log.Fatal(err)
	}
	return token, created_at, parameters
}

func GetOriginalDBConnectionString() string {
	return fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)
}
