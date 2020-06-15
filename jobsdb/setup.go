package jobsdb

import (
	"fmt"

	migrator "github.com/rudderlabs/rudder-server/services/sql-migrator"
)

// setupDatabaseTables will initialize jobsdb tables using migration templates inside 'sql/migrations/jobsdb'.
// Dataset tables are not created via migration scripts, they can only be updated.
// The following data are passed to JobsDB migration templates:
// - Prefix: The table prefix used by this jobsdb instance.
// - Datasets: Array of existing dataset indices.
func (jd *HandleT) setupDatabaseTables() {
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
	migrationsTable := fmt.Sprintf("%s_schema_migrations", jd.tablePrefix)

	m := &migrator.Migrator{
		Handle:          jd.dbHandle,
		MigrationsTable: migrationsTable,
	}

	// execute any necessary migrations
	err := m.MigrateFromTemplates("jobsdb", templateData)
	if err != nil {
		panic(fmt.Errorf("Error while migrating '%v' jobsdb tables: %w", jd.tablePrefix, err))
	}
}
