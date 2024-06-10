package jobsdb

import (
	"fmt"

	"github.com/rudderlabs/rudder-server/jobsdb/internal/lock"
	migrator "github.com/rudderlabs/rudder-server/services/sql-migrator"
)

// SchemaMigrationTable returns the table name used for storing current schema version.
func (jd *Handle) SchemaMigrationTable() string {
	return fmt.Sprintf("%s_schema_migrations", jd.tablePrefix)
}

// setupDatabaseTables will initialize jobsdb tables using migration templates inside 'sql/migrations/jobsdb'.
// Dataset tables are not created via migration scripts, they can only be updated.
// The following data are passed to JobsDB migration templates:
// - Prefix: The table prefix used by this jobsdb instance.
// - Datasets: Array of existing dataset indices.
func (jd *Handle) setupDatabaseTables(templateData map[string]interface{}) {
	// setup migrator with appropriate schema migrations table
	m := &migrator.Migrator{
		Handle:                     jd.dbHandle,
		MigrationsTable:            jd.SchemaMigrationTable(),
		ShouldForceSetLowerVersion: jd.config.GetBool("SQLMigrator.forceSetLowerVersion", true),
	}
	// execute any necessary migrations
	if err := m.MigrateFromTemplates("jobsdb", templateData); err != nil {
		panic(fmt.Errorf("error while migrating '%v' jobsdb tables: %w", jd.tablePrefix, err))
	}
}

func (jd *Handle) runAlwaysChangesets(templateData map[string]interface{}) {
	// setup migrator with appropriate schema migrations table
	m := &migrator.Migrator{
		Handle:          jd.dbHandle,
		MigrationsTable: fmt.Sprintf("%s_runalways_migrations", jd.tablePrefix),
		RunAlways:       true,
	}
	// execute any necessary migrations
	if err := m.MigrateFromTemplates("jobsdb_always", templateData); err != nil {
		panic(fmt.Errorf("error while running changesets that run always in '%s' jobsdb tables: %w", jd.tablePrefix, err))
	}
}

func (jd *Handle) dropDatabaseTables(l lock.LockToken) {
	jd.logger.Infof("[JobsDB:%v] Dropping all database tables", jd.tablePrefix)
	jd.dropSchemaMigrationTables()
	jd.assertError(jd.dropAllDS(l))
	jd.dropJournal()
}

func (jd *Handle) dropSchemaMigrationTables() {
	sqlStatement := fmt.Sprintf(`DROP TABLE IF EXISTS %s`, jd.SchemaMigrationTable())
	_, err := jd.dbHandle.Exec(sqlStatement)
	jd.assertError(err)
}
