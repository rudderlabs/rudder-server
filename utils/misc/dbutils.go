package misc

import (
	"database/sql"
	"fmt"
	"os"

	"github.com/lib/pq"

	"github.com/rudderlabs/rudder-go-kit/config"
)

// GetConnectionString Returns Jobs DB connection configuration
func GetConnectionString(c *config.Config) string {
	if c == nil {
		c = config.Default
	}
	host := c.GetString("DB.host", "localhost")
	user := c.GetString("DB.user", "ubuntu")
	dbname := c.GetString("DB.name", "ubuntu")
	port := c.GetInt("DB.port", 5432)
	password := c.GetString("DB.password", "ubuntu") // Reading secrets from
	sslmode := c.GetString("DB.sslMode", "disable")
	// Application Name can be any string of less than NAMEDATALEN characters (64 characters in a standard PostgreSQL build).
	// There is no need to truncate the string on our own though since PostgreSQL auto-truncates this identifier and issues a relevant notice if necessary.
	appName := DefaultString("rudder-server").OnError(os.Hostname())
	return fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=%s application_name=%s",
		host, port, user, password, dbname, sslmode, appName)
}

/*
ReplaceDB : Rename the OLD DB and create a new one.
Since we are not journaling, this should be idemponent
*/
func ReplaceDB(dbName, targetName string, c *config.Config) {
	if c == nil {
		c = config.Default
	}
	host := c.GetString("DB.host", "localhost")
	user := c.GetString("DB.user", "ubuntu")
	port := c.GetInt("DB.port", 5432)
	password := c.GetString("DB.password", "ubuntu") // Reading secrets from
	sslmode := c.GetString("DB.sslMode", "disable")
	connInfo := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=postgres sslmode=%s",
		host, port, user, password, sslmode)
	db, err := sql.Open("postgres", connInfo)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// Killing sessions on the db
	sqlStatement := fmt.Sprintf("SELECT pid, pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '%s' AND pid <> pg_backend_pid();", dbName)
	_, err = db.Exec(sqlStatement)
	if err != nil {
		panic(err)
	}

	renameDBStatement := fmt.Sprintf(`ALTER DATABASE %q RENAME TO %q`,
		dbName, targetName)
	pkgLogger.Debug(renameDBStatement)
	_, err = db.Exec(renameDBStatement)

	// If execution of ALTER returns error, pacicking
	if err != nil {
		panic(err)
	}

	createDBStatement := fmt.Sprintf(`CREATE DATABASE %q`, dbName)
	_, err = db.Exec(createDBStatement)
	if err != nil {
		panic(err)
	}
}

func QuoteLiteral(literal string) string {
	return pq.QuoteLiteral(literal)
}
