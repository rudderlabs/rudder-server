package jobsdb

import (
	"database/sql"
	"fmt"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/jobsdb/internal/lock"
	migrator "github.com/rudderlabs/rudder-server/services/sql-migrator"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

// SchemaMigrationTable returns the table name used for storing current schema version.
func (jd *HandleT) SchemaMigrationTable() string {
	return fmt.Sprintf("%s_schema_migrations", jd.tablePrefix)
}

// setupDatabaseTables will initialize jobsdb tables using migration templates inside 'sql/migrations/jobsdb'.
// Dataset tables are not created via migration scripts, they can only be updated.
// The following data are passed to JobsDB migration templates:
// - Prefix: The table prefix used by this jobsdb instance.
// - Datasets: Array of existing dataset indices.
// If clearAll is set to true, all existing jobsdb tables will be removed first.
func (jd *HandleT) setupDatabaseTables(l lock.LockToken, clearAll bool) {
	if clearAll {
		jd.dropDatabaseTables(l)
	}

	// Important: if jobsdb type is acting as a writer then refreshDSList
	// doesn't return the full list of datasets, only the rightmost two.
	// But we need to run the schema migration against all datasets, no matter
	// whether jobsdb is a writer or not.
	datasets := getDSList(jd, jd.dbHandle, jd.tablePrefix)

	datasetIndices := make([]string, 0)
	for _, dataset := range datasets {
		datasetIndices = append(datasetIndices, dataset.Index)
	}

	templateData := map[string]interface{}{
		"Prefix":   jd.tablePrefix,
		"Datasets": datasetIndices,
	}

	psqlInfo := misc.GetConnectionString()
	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		panic(fmt.Errorf("error DB for migrate open: %w", err))
	}

	defer func() { _ = db.Close() }()

	// setup migrator with appropriate schema migrations table
	m := &migrator.Migrator{
		Handle:                     db,
		MigrationsTable:            jd.SchemaMigrationTable(),
		ShouldForceSetLowerVersion: config.GetBool("SQLMigrator.forceSetLowerVersion", true),
	}

	// execute any necessary migrations
	err = m.MigrateFromTemplates("jobsdb", templateData)
	if err != nil {
		panic(fmt.Errorf("error while migrating '%v' jobsdb tables: %w", jd.tablePrefix, err))
	}
	// finally refresh the dataset list to make sure [datasetList] field is populated
	jd.refreshDSList(l)
}

func (jd *HandleT) dropDatabaseTables(l lock.LockToken) {
	jd.logger.Infof("[JobsDB:%v] Dropping all database tables", jd.tablePrefix)
	jd.dropSchemaMigrationTables()
	jd.assertError(jd.dropAllDS(l))
	jd.dropJournal()
	jd.assertError(jd.dropAllBackupDS())
}

func (jd *HandleT) dropSchemaMigrationTables() {
	sqlStatement := fmt.Sprintf(`DROP TABLE IF EXISTS %s`, jd.SchemaMigrationTable())
	_, err := jd.dbHandle.Exec(sqlStatement)
	jd.assertError(err)
}
