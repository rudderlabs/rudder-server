package helpers

import (
	"database/sql"
	"fmt"
	"strconv"

	"github.com/rudderlabs/rudder-server/config"
)

var (
	host, user, password, dbname, sslmode string
	port                                  int
)

func init() {
	loadConfig()
}

// Loads db config from config file
func loadConfig() {
	host = config.GetEnv("CONFIG_DB_HOST", "localhost")
	user = config.GetEnv("CONFIG_DB_USER", "postgres")
	dbname = config.GetEnv("CONFIG_DB_DB_NAME", "postgresDB")
	port, _ = strconv.Atoi(config.GetEnv("CONFIG_DB_PORT", "5433"))
	password = config.GetEnv("CONFIG_DB_PASSWORD", "postgres")
	sslmode = config.GetEnv("CONFIG_DB_SSL_MODE", "disable")
}

func GetConnectionString() string {
	return fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=%s",
		host, port, user, password, dbname, sslmode)
}

// FetchEventSchemaCount returns count of event_uploads table
// TODO: Currently assuming that the config backend db is accessible
// from here. Change this to hit an endpoint to fetch the event schema count.
func FetchEventSchemaCount(dbHandle *sql.DB) int {
	count := 0
	dbHandle.QueryRow(fmt.Sprintf(`select count(*) from %s;`, "event_uploads")).Scan(&count)

	return count
}
