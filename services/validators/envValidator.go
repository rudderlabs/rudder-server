package validators

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	migrator "github.com/rudderlabs/rudder-server/services/sql-migrator"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

const (
	//This is integer representation of Postgres version.
	//For ex, integer representation of version 9.6.3 is 90603
	//Minimum postgres version needed for rudder server is 10
	minPostgresVersion = 100000
)

var pkgLogger logger.LoggerI

func init() {
	pkgLogger = logger.NewLogger().Child("validators").Child("envValidator")
}

func createWorkspaceTable(dbHandle *sql.DB) {
	//Create table to store workspace token
	sqlStatement := `CREATE TABLE IF NOT EXISTS workspace (
		token TEXT PRIMARY KEY,
		created_at TIMESTAMP NOT NULL,
		parameters JSONB);`

	_, err := dbHandle.Exec(sqlStatement)
	if err != nil {
		panic(err)
	}
}

func insertTokenIfNotExists(dbHandle *sql.DB) {
	//Read entries, if there are no entries insert hashed current workspace token
	var totalCount int
	sqlStatement := `SELECT COUNT(*) FROM workspace`
	row := dbHandle.QueryRow(sqlStatement)
	err := row.Scan(&totalCount)
	if err != nil {
		panic(err)
	}

	if totalCount > 0 {
		return
	}

	//There are no entries in the table, hash current workspace token and insert
	sqlStatement = `INSERT INTO workspace (token, created_at)
									   VALUES ($1, $2)`
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

func setWHSchemaVersionIfNotExists(dbHandle *sql.DB) {
	hashedToken := misc.GetMD5Hash(config.GetWorkspaceToken())
	whSchemaVersion := config.GetString("Warehouse.schemaVersion", "v1")
	config.SetWHSchemaVersion(whSchemaVersion)

	var parameters sql.NullString
	sqlStatement := fmt.Sprintf(`SELECT parameters FROM workspace WHERE token = '%s'`, hashedToken)
	row := dbHandle.QueryRow(sqlStatement)
	err := row.Scan(&parameters)
	if err == sql.ErrNoRows {
		return
	}
	if err != nil {
		panic(err)
	}

	if !parameters.Valid {
		// insert current version
		sqlStatement = fmt.Sprintf(`UPDATE workspace SET parameters = '{"wh_schema_version":"%s"}' WHERE token = '%s'`, whSchemaVersion, hashedToken)
		_, err := dbHandle.Exec(sqlStatement)
		if err != nil {
			panic(err)
		}
	} else {
		var parametersMap map[string]interface{}
		err = json.Unmarshal([]byte(parameters.String), &parametersMap)
		if err != nil {
			panic(err)
		}
		if version, ok := parametersMap["wh_schema_version"]; ok {
			whSchemaVersion = version.(string)
			config.SetWHSchemaVersion(whSchemaVersion)
			return
		}
		parametersMap["wh_schema_version"] = whSchemaVersion
		marshalledParameters, err := json.Marshal(parametersMap)
		if err != nil {
			panic(err)
		}
		sqlStatement = fmt.Sprintf(`UPDATE workspace SET parameters = '%s' WHERE token = '%s'`, marshalledParameters, hashedToken)
		_, err = dbHandle.Exec(sqlStatement)
		if err != nil {
			panic(err)
		}
	}
}

func getWorkspaceFromDB(dbHandle *sql.DB) string {
	sqlStatement := `SELECT token FROM workspace order by created_at desc limit 1`
	var token string
	row := dbHandle.QueryRow(sqlStatement)
	err := row.Scan(&token)
	if err != nil {
		panic(err)
	}

	return token
}

func createDBConnection() *sql.DB {
	psqlInfo := jobsdb.GetConnectionString()
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

//IsPostgresCompatible checks the if the version of postgres is greater than minPostgresVersion
func IsPostgresCompatible(db *sql.DB) (bool, error) {
	var versionNum int
	err := db.QueryRow("SHOW server_version_num;").Scan(&versionNum)
	if err != nil {
		return false, err
	}
	return versionNum >= minPostgresVersion, nil
}

//ValidateEnv validates the current environment available for the server
func ValidateEnv() bool {
	dbHandle := createDBConnection()
	defer closeDBConnection(dbHandle)
	isDBCompatible, err := IsPostgresCompatible(dbHandle)
	if err != nil {
		panic(err)
	}
	if !isDBCompatible {
		pkgLogger.Errorf("Rudder server needs postgres version >= 10. Exiting.")
		return false
	}
	return true
}

//InitializeEnv initializes the environment for the server
func InitializeEnv() {
	dbHandle := createDBConnection()

	m := &migrator.Migrator{
		Handle:                     dbHandle,
		MigrationsTable:            "node_migrations",
		ShouldForceSetLowerVersion: config.GetBool("SQLMigrator.forceSetLowerVersion", false),
	}
	err := m.Migrate("node")
	if err != nil {
		panic(fmt.Errorf("Could not run node migrations: %w", err))
	}

	//create workspace table and insert token
	createWorkspaceTable(dbHandle)
	insertTokenIfNotExists(dbHandle)
	setWHSchemaVersionIfNotExists(dbHandle)

	workspaceTokenHashInDB := getWorkspaceFromDB(dbHandle)
	if workspaceTokenHashInDB == misc.GetMD5Hash(config.GetWorkspaceToken()) {
		return
	}

	//db connection should be closed. Else alter db fails.
	closeDBConnection(dbHandle)

	pkgLogger.Warn("Previous workspace token is not same as the current workspace token. Parking current jobsdb aside and creating a new one")

	dbName := config.GetEnv("JOBS_DB_DB_NAME", "ubuntu")
	misc.ReplaceDB(dbName, dbName+"_"+strconv.FormatInt(time.Now().Unix(), 10)+"_"+workspaceTokenHashInDB)

	dbHandle = createDBConnection()

	//create workspace table and insert hashed token
	createWorkspaceTable(dbHandle)
	insertTokenIfNotExists(dbHandle)
	setWHSchemaVersionIfNotExists(dbHandle)

	closeDBConnection(dbHandle)
}
