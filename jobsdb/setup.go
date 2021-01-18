package jobsdb

import (
	"fmt"

	"github.com/rudderlabs/rudder-server/config"
	migrator "github.com/rudderlabs/rudder-server/services/sql-migrator"
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
func (jd *HandleT) setupDatabaseTables(clearAll bool) {
	if clearAll {
		jd.dropDatabaseTables()
	}

	// collect all existing dataset indices, and create template data
	datasets := jd.getDSList(true)

	datasetIndices := make([]string, 0)
	for _, dataset := range datasets {
		datasetIndices = append(datasetIndices, dataset.Index)
	}

	templateData := map[string]interface{}{
		"Prefix":   jd.tablePrefix,
		"Datasets": datasetIndices,
	}

	// setup migrator with appropriate schema migrations table
	m := &migrator.Migrator{
		Handle:                     jd.dbHandle,
		MigrationsTable:            jd.SchemaMigrationTable(),
		ShouldForceSetLowerVersion: config.GetBool("SQLMigrator.forceSetLowerVersion", false),
	}

	// execute any necessary migrations
	err := m.MigrateFromTemplates("jobsdb", templateData)
	if err != nil {
		panic(fmt.Errorf("Error while migrating '%v' jobsdb tables: %w", jd.tablePrefix, err))
	}
}

func (jd *HandleT) dropDatabaseTables() {

	jd.logger.Infof("[JobsDB:%v] Dropping all database tables", jd.tablePrefix)
	jd.dropSchemaMigrationTables()
	jd.dropAllDS()
	jd.dropJournal()
	jd.dropAllBackupDS()
	jd.dropMigrationCheckpointTables()
}

func (jd *HandleT) dropSchemaMigrationTables() {
	sqlStatement := fmt.Sprintf(`DROP TABLE IF EXISTS %s`, jd.SchemaMigrationTable())
	_, err := jd.dbHandle.Exec(sqlStatement)
	jd.assertError(err)
}
