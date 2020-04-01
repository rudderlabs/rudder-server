package misc

import (
	"database/sql"
	"fmt"
	"strconv"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

var (
	host, user, password string
	port                 int
)

func loadConfig() {
	host = config.GetEnv("JOBS_DB_HOST", "localhost")
	user = config.GetEnv("JOBS_DB_USER", "ubuntu")
	port, _ = strconv.Atoi(config.GetEnv("JOBS_DB_PORT", "5432"))
	password = config.GetEnv("JOBS_DB_PASSWORD", "ubuntu") // Reading secrets from
}

/*
ReplaceDB : Rename the OLD DB and create a new one.
Since we are not journaling, this should be idemponent
*/
func ReplaceDB(dbName, targetName string) {
	loadConfig()
	connInfo := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=postgres sslmode=disable",
		host, port, user, password)
	db, err := sql.Open("postgres", connInfo)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	//Killing sessions on the db
	sqlStatement := fmt.Sprintf("SELECT pid, pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '%s' AND pid <> pg_backend_pid();", dbName)
	rows, err := db.Query(sqlStatement)
	if err != nil {
		panic(err)
	}
	rows.Close()

	renameDBStatement := fmt.Sprintf("ALTER DATABASE %s RENAME TO %s",
		dbName, targetName)
	logger.Debug(renameDBStatement)
	_, err = db.Exec(renameDBStatement)

	// If execution of ALTER returns error, pacicking
	if err != nil {
		panic(err)
	}

	createDBStatement := fmt.Sprintf("CREATE DATABASE %s", dbName)
	_, err = db.Exec(createDBStatement)
	if err != nil {
		panic(err)
	}
}
