package util

import (
	"database/sql"
	"errors"
	"fmt"
	"strconv"

	_ "github.com/lib/pq"

	"github.com/rudderlabs/rudder-server/cmd/rudder-cli/config"
)

func GetDbConnectionString() string {
	port, _ := strconv.Atoi(config.GetEnv(config.JobsDBPortKey))
	connString := fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable",
		config.GetEnv(config.JobsDBHostKey),
		port,
		config.GetEnv(config.JobsDBUserKey),
		config.GetEnv(config.JobsDBPasswordKey),
		config.GetEnv(config.JobsDBNameKey),
	)
	return connString
}

func GetDbHandle() (*sql.DB, error) {
	var db *sql.DB
	db, err := sql.Open("postgres", GetDbConnectionString())

	return db, err
}

func IsDBConnected() (string, error) {
	db, err := GetDbHandle()
	if err != nil {
		return "Failed to Get DB Handle", err
	}
	row := db.QueryRow(`SELECT 'Rudder DB Health Check'::text as message`)
	if row.Err() != nil {
		fmt.Println(err)
		return "Failed to query DB", err
	}

	return "success", nil
}

func GetAllTableNames(db *sql.DB) ([]string, error) {
	tableNames := []string{}

	stmt, err := db.Prepare(`SELECT tablename
		FROM pg_catalog.pg_tables
		WHERE schemaname != 'pg_catalog' AND
		schemaname != 'information_schema'`)
	if err != nil {
		return tableNames, errors.New("Failed to prepare query")
	}
	defer stmt.Close()

	rows, err := stmt.Query()
	if err != nil {
		return tableNames, errors.New("Failed to run query")
	}
	defer rows.Close()

	for rows.Next() {
		var tbName string
		err = rows.Scan(&tbName)
		if err != nil {
			return tableNames, errors.New("Failed to scan rows")
		}
		tableNames = append(tableNames, tbName)
	}
	if rows.Err() != nil {
		return tableNames, errors.New("Failed to iterate over rows")
	}

	return tableNames, nil
}
