package validators

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	migrator "github.com/rudderlabs/rudder-server/services/sql-migrator"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

const (
	// This is integer representation of Postgres version.
	// For ex, integer representation of version 9.6.3 is 90603
	// Minimum postgres version needed for rudder server is 10
	minPostgresVersion = 100000
)

var pkgLogger logger.Logger

func init() {
	pkgLogger = logger.NewLogger().Child("validators").Child("envValidator")
}

func createWorkspaceTable(dbHandle *sql.DB) error {
	// Create table to store workspace token
	sqlStatement := `CREATE TABLE IF NOT EXISTS workspace (
		token TEXT PRIMARY KEY,
		created_at TIMESTAMP NOT NULL,
		parameters JSONB);`

	_, err := dbHandle.Exec(sqlStatement)
	if err != nil {
		return fmt.Errorf("error creating workspace table: %w", err)
	}
	return nil
}

func insertTokenIfNotExists(dbHandle *sql.DB) error {
	// Read entries, if there are no entries insert hashed current workspace token
	var totalCount int
	sqlStatement := `SELECT COUNT(*) FROM workspace`
	row := dbHandle.QueryRow(sqlStatement)
	err := row.Scan(&totalCount)
	if err != nil {
		return fmt.Errorf("error reading workspace table: %w", err)
	}

	if totalCount > 0 {
		return nil
	}

	// There are no entries in the table, hash current workspace token and insert
	if _, err := dbHandle.Exec(`INSERT INTO workspace (token, created_at)
									VALUES ($1, $2)`,
		misc.GetMD5Hash(config.GetWorkspaceToken()),
		time.Now()); err != nil {
		return fmt.Errorf("error inserting workspace token: %w", err)
	}
	return nil
}

func setWHSchemaVersionIfNotExists(dbHandle *sql.DB) error {
	hashedToken := misc.GetMD5Hash(config.GetWorkspaceToken())
	whSchemaVersion := config.GetString("Warehouse.schemaVersion", "v1")

	var parameters sql.NullString
	sqlStatement := fmt.Sprintf(`SELECT parameters FROM workspace WHERE token = '%s'`, hashedToken)
	row := dbHandle.QueryRow(sqlStatement)
	err := row.Scan(&parameters)
	if err == sql.ErrNoRows {
		return nil
	}
	if err != nil {
		return fmt.Errorf("error reading workspace table: %w", err)
	}

	if !parameters.Valid {
		// insert current version
		sqlStatement = fmt.Sprintf(`UPDATE workspace SET parameters = '{"wh_schema_version":%q}' WHERE token = '%s'`, whSchemaVersion, hashedToken)
		_, err := dbHandle.Exec(sqlStatement)
		if err != nil {
			return fmt.Errorf("error updating workspace table: %w", err)
		}
	} else {
		var parametersMap map[string]interface{}
		err = json.Unmarshal([]byte(parameters.String), &parametersMap)
		if err != nil {
			return fmt.Errorf("error unmarshalling parameters: %w", err)
		}
		if version, ok := parametersMap["wh_schema_version"]; ok {
			whSchemaVersion = version.(string)
			config.Set("Warehouse.schemaVersion", whSchemaVersion)
			return nil
		}
		parametersMap["wh_schema_version"] = whSchemaVersion
		marshalledParameters, err := json.Marshal(parametersMap)
		if err != nil {
			return fmt.Errorf("error marshalling parameters: %w", err)
		}
		sqlStatement = fmt.Sprintf(`UPDATE workspace SET parameters = '%s' WHERE token = '%s'`, marshalledParameters, hashedToken)
		_, err = dbHandle.Exec(sqlStatement)
		if err != nil {
			return fmt.Errorf("error updating workspace table: %w", err)
		}
	}
	return nil
}

func getWorkspaceFromDB(dbHandle *sql.DB) (string, error) {
	sqlStatement := `SELECT token FROM workspace order by created_at desc limit 1`
	var token string
	row := dbHandle.QueryRow(sqlStatement)
	if err := row.Scan(&token); err != nil {
		return token, fmt.Errorf("error reading workspace table: %w", err)
	}
	return token, nil
}

func createDBConnection() (*sql.DB, error) {
	psqlInfo := misc.GetConnectionString(nil)
	var err error
	dbHandle, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		return nil, fmt.Errorf("error opening db connection: %w", err)
	}

	err = dbHandle.Ping()
	if err != nil {
		return nil, fmt.Errorf("error pinging db: %w", err)
	}
	return dbHandle, nil
}

func closeDBConnection(handle *sql.DB) error {
	err := handle.Close()
	if err != nil {
		return fmt.Errorf("error closing db connection: %w", err)
	}
	return nil
}

func killDanglingDBConnections(db *sql.DB) error {
	rows, err := db.Query(`SELECT PID, QUERY_START, COALESCE(WAIT_EVENT_TYPE,''), COALESCE(WAIT_EVENT, ''), COALESCE(STATE, ''), QUERY, PG_TERMINATE_BACKEND(PID)
							FROM PG_STAT_ACTIVITY
							WHERE PID <> PG_BACKEND_PID()
							AND APPLICATION_NAME = CURRENT_SETTING('APPLICATION_NAME')
							AND APPLICATION_NAME <> ''`)
	if err != nil {
		return fmt.Errorf("querying pg_stat_activity table for terminating dangling connections: %w", err)
	}
	defer func() { _ = rows.Close() }()

	type danglingConnRow struct {
		pid           int
		queryStart    *string
		waitEventType string
		waitEvent     string
		state         string
		query         string
		terminated    bool
	}

	dangling := make([]*danglingConnRow, 0)
	for rows.Next() {
		var row danglingConnRow
		err := rows.Scan(&row.pid, &row.queryStart, &row.waitEventType, &row.waitEvent, &row.state, &row.query, &row.terminated)
		if err != nil {
			return fmt.Errorf("scanning pg_stat_activity table for terminating dangling connections: %w", err)
		}
		dangling = append(dangling, &row)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterating pg_stat_activity table for terminating dangling connections: %w", err)
	}

	if len(dangling) > 0 {
		pkgLogger.Warnf("Terminated %d dangling connection(s)", len(dangling))
		for i, rowPtr := range dangling {
			pkgLogger.Warnf("dangling connection #%d: %+v", i+1, *rowPtr)
		}
	}
	return nil
}

// IsPostgresCompatible checks the if the version of postgres is greater than minPostgresVersion
func IsPostgresCompatible(ctx context.Context, db *sql.DB) (bool, error) {
	var versionNum int
	err := db.QueryRowContext(ctx, "SHOW server_version_num;").Scan(&versionNum)
	if err != nil {
		return false, err
	}
	return versionNum >= minPostgresVersion, nil
}

// ValidateEnv validates the current environment available for the server
func ValidateEnv() error {
	dbHandle, err := createDBConnection()
	if err != nil {
		return err
	}
	defer func() { _ = closeDBConnection(dbHandle) }()

	isDBCompatible, err := IsPostgresCompatible(context.TODO(), dbHandle)
	if err != nil {
		return err
	}
	if !isDBCompatible {
		return errors.New("rudder server needs postgres version >= 10")
	}

	// SQL statements in rudder-server are not executed with a timeout context, instead we are letting them take as much time as they need :)
	// Due to the above, when a server shutdown is initiated in a cloud environment while long-running statements are being executed,
	// the server process will not manage to shutdown gracefully, since it will be blocked by the SQL statements.
	// The container orchestrator will eventually kill the server process, leaving one or more dangling connections in the database.
	// This will ensure that before a new rudder-server instance starts working, all previous dangling connections belonging to this server are being killed.
	return killDanglingDBConnections(dbHandle)
}

// InitializeEnv initializes the environment for the server
func InitializeNodeMigrations() error {
	dbHandle, err := createDBConnection()
	if err != nil {
		return err
	}
	defer func() { _ = closeDBConnection(dbHandle) }()

	m := &migrator.Migrator{
		Handle:                     dbHandle,
		MigrationsTable:            "node_migrations",
		ShouldForceSetLowerVersion: config.GetBool("SQLMigrator.forceSetLowerVersion", true),
	}
	if err := m.Migrate("node"); err != nil {
		return fmt.Errorf("could not run node schema migrations: %w", err)
	}
	return nil
}

func CheckAndValidateWorkspaceToken() error {
	dbHandle, err := createDBConnection()
	if err != nil {
		return err
	}
	defer func() { _ = closeDBConnection(dbHandle) }()

	if err := createWorkspaceTable(dbHandle); err != nil {
		return err
	}
	if err := insertTokenIfNotExists(dbHandle); err != nil {
		return err
	}
	if err := setWHSchemaVersionIfNotExists(dbHandle); err != nil {
		return err
	}

	workspaceTokenHashInDB, err := getWorkspaceFromDB(dbHandle)
	if err != nil {
		return err
	}
	if workspaceTokenHashInDB == misc.GetMD5Hash(config.GetWorkspaceToken()) {
		return nil
	}

	// db connection should be closed. Else alter db fails.
	// A new connection will be created again below, which will be closed on returning of this function (due to defer statement).
	_ = closeDBConnection(dbHandle)

	pkgLogger.Warn("Previous workspace token is not same as the current workspace token. Parking current jobsdb aside and creating a new one")

	dbName := config.GetString("DB.name", "ubuntu")
	misc.ReplaceDB(dbName, dbName+"_"+strconv.FormatInt(time.Now().Unix(), 10)+"_"+workspaceTokenHashInDB, nil)

	dbHandle, err = createDBConnection()
	if err != nil {
		return err
	}

	// create workspace table and insert hashed token
	if err := createWorkspaceTable(dbHandle); err != nil {
		return err
	}
	if err := insertTokenIfNotExists(dbHandle); err != nil {
		return err
	}
	if err := setWHSchemaVersionIfNotExists(dbHandle); err != nil {
		return err
	}
	return nil
}
