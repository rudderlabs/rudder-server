package misc

import (
	"database/sql"
	"fmt"
	"io"
	"strconv"

	"github.com/lib/pq"

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

	renameDBStatement := fmt.Sprintf("ALTER DATABASE \"%s\" RENAME TO \"%s\"",
		dbName, targetName)
	logger.Debug(renameDBStatement)
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

// DumpQueryFilter describes a filter operation on a map returned by a DumpQuery operation
type DumpQueryFilter func(m map[string]interface{}) map[string]interface{}

// DumpQueryToWriter will execute the query in db, and send one line to the writer per result, serialized as json.
func DumpQueryToWriter(db *sql.DB, query string, writer io.Writer, filter DumpQueryFilter) error {
	rows, err := db.Query(query)
	if err != nil {
		return fmt.Errorf("Could not execute query for dump data: %w", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("Could not fetch dump data columns: %w", err)
	}

	// scan all rows, for each write a map to file, with columns as keys and query results as values.
	values := make([][]byte, len(columns))
	valuePointers := make([]interface{}, len(columns))

	for i := range values {
		valuePointers[i] = &values[i]
	}

	for rows.Next() {
		row := make(map[string]interface{})

		if err := rows.Scan(valuePointers...); err != nil {
			return fmt.Errorf("Could not read dump data row: %w", err)
		}

		for i, raw := range values {
			row[columns[i]] = string(raw)
		}

		if filter != nil {
			row = filter(row)
		}

		err := WriteMapToWriter(row, writer)
		_, err = io.WriteString(writer, "\n")
		if err != nil {
			return fmt.Errorf("Could not write dump data row: %w", err)
		}
	}

	return nil
}
