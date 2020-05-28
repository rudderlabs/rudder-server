package migrator

import (
	"bytes"
	"database/sql"
	"fmt"
	"io/ioutil"
	"path/filepath"

	"text/template"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file" // file source
	bindata "github.com/golang-migrate/migrate/v4/source/go_bindata"
)

// Migrator is responsible for migrating postgres tables
type Migrator struct {
	// MigrationsTable is name of the table that holds current migration version.
	// Each migration set requires a separate MigrationsTable.
	MigrationsTable string

	// Handle is the sql.DB handle used to execute migration statements
	Handle *sql.DB
}

// Migrate migrates database schema
func (m *Migrator) Migrate(migrationsDir string) error {
	driver, err := m.getDestinationDriver()

	migration, err := migrate.NewWithDatabaseInstance("file://"+migrationsDir, "postgres", driver)
	if err != nil {
		return fmt.Errorf("Could not execute migrations from migration directory '%v': %w", migrationsDir, err)
	}

	return migration.Up()
}

// MigrateFromTemplates migrates database with migration scripts provided by golang templates.
// Migration templates are read from all files in templatesDir and converted using provided context as template data.
// Directories inside templates directory are ignored.
func (m *Migrator) MigrateFromTemplates(templatesDir string, context interface{}) error {
	// look in templatesDir for migration template files
	fileInfos, err := ioutil.ReadDir(templatesDir)
	if err != nil {
		return fmt.Errorf("Could not read migration template directory '%v': %w", templatesDir, err)
	}

	templateNames := make([]string, 0)
	for _, fileInfo := range fileInfos {
		if !fileInfo.IsDir() {
			templateNames = append(templateNames, fileInfo.Name())
		}
	}

	// read files and create bindata source
	assetSource := bindata.Resource(templateNames,
		func(name string) ([]byte, error) {
			// read template file
			templateData, err := ioutil.ReadFile(filepath.Join(templatesDir, name))
			if err != nil {
				return nil, fmt.Errorf("Could not read migration template '%v': %w", name, err)
			}

			// parse template
			tmpl, err := template.New(name).Parse(string(templateData))
			if err != nil {
				return nil, fmt.Errorf("Could not parse migration template '%v': %w", name, err)
			}

			// execute template with given context
			buffer := new(bytes.Buffer)
			err = tmpl.Execute(buffer, context)
			if err != nil {
				return nil, fmt.Errorf("Could not execute migration template '%v': %w", name, err)
			}

			script := buffer.Bytes()

			return script, nil
		})

	sourceDriver, err := bindata.WithInstance(assetSource)
	if err != nil {
		return fmt.Errorf("Could not create migration source from template directory '%v': %w", templatesDir, err)
	}

	// create destination driver from db.handle
	destinationDriver, err := m.getDestinationDriver()
	if err != nil {
		return fmt.Errorf("Could not create migration destination: %w", err)
	}

	// run the migration scripts
	migration, err := migrate.NewWithInstance("go-bindata", sourceDriver, "postgres", destinationDriver)
	if err != nil {
		return fmt.Errorf("Could not setup migration from template directory '%v': %w", templatesDir, err)
	}

	err = migration.Up()
	if err != nil && err != migrate.ErrNoChange { // migrate library reports that no change was required, using ErrNoChange
		return fmt.Errorf("Could not run migration from template directory '%v', %w", templatesDir, err)
	}

	return nil
}

func (m *Migrator) getDestinationDriver() (database.Driver, error) {
	return postgres.WithInstance(m.Handle, &postgres.Config{MigrationsTable: m.MigrationsTable})
}
