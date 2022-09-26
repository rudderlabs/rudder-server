package misc

import (
	"database/sql"
	"fmt"

	"github.com/lib/pq"

	"github.com/rudderlabs/rudder-server/config"
)

type AdvisoryLock int

const (
	JobsDBAddDsAdvisoryLock AdvisoryLock = 11
)

var (
	host, user, password, sslmode string
	port                          int
)

func loadConfig() {
	host = config.GetString("DB.host", "localhost")
	user = config.GetString("DB.user", "ubuntu")
	port = config.GetInt("DB.port", 5432)
	password = config.GetString("DB.password", "ubuntu") // Reading secrets from
	sslmode = config.GetString("DB.sslMode", "disable")
}

/*
ReplaceDB : Rename the OLD DB and create a new one.
Since we are not journaling, this should be idemponent
*/
func ReplaceDB(dbName, targetName string) {
	loadConfig()
	connInfo := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=postgres sslmode=%s",
		host, port, user, password, sslmode)
	db, err := sql.Open("postgres", connInfo)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// Killing sessions on the db
	sqlStatement := fmt.Sprintf("SELECT pid, pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '%s' AND pid <> pg_backend_pid();", dbName)
	rows, err := db.Query(sqlStatement)
	if err != nil {
		panic(err)
	}
	rows.Close()

	renameDBStatement := fmt.Sprintf("ALTER DATABASE \"%s\" RENAME TO \"%s\"",
		dbName, targetName)
	pkgLogger.Debug(renameDBStatement)
	_, err = db.Exec(renameDBStatement)

	// If execution of ALTER returns error, pacicking
	if err != nil {
		panic(err)
	}

	createDBStatement := fmt.Sprintf("CREATE DATABASE \"%s\"", dbName)
	_, err = db.Exec(createDBStatement)
	if err != nil {
		panic(err)
	}
}

func QuoteLiteral(literal string) string {
	return pq.QuoteLiteral(literal)
}
