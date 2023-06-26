package migrator

import (
	"bytes"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"text/template"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source"
	bindata "github.com/golang-migrate/migrate/v4/source/go_bindata"
	"github.com/golang-migrate/migrate/v4/source/iofs"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/sql/migrations"
)

// Migrator is responsible for migrating postgres tables
type Migrator struct {
	// MigrationsTable is name of the table that holds current migration version.
	// Each migration set requires a separate MigrationsTable.
	MigrationsTable string

	// Handle is the sql.DB handle used to execute migration statements
	Handle *sql.DB

	// Indicates if migration version should be force reset to latest on file in case of revert to lower version
	// Eg. DB has v3 set in MigrationsTable but latest version in MigrationsDir is v2
	ShouldForceSetLowerVersion bool

	// Indicates if all migrations should be run ignoring the current version in MigrationsTable
	RunAlways bool
}

var pkgLogger logger.Logger

func init() {
	pkgLogger = logger.NewLogger().Child("sql-migrator").Child("migrator")
}

// Migrate migrates database schema using migration SQL scripts.
func (m *Migrator) Migrate(migrationsDir string) error {
	destinationDriver, err := m.getDestinationDriver()
	if err != nil {
		return fmt.Errorf("destination driver for %q migrator: %w", migrationsDir, err)
	}

	sourceDriver, err := iofs.New(migrations.FS, migrationsDir)
	if err != nil {
		return fmt.Errorf("source driver for %q migrator: %w", migrationsDir, err)
	}

	migration, err := migrate.NewWithInstance("iofs", sourceDriver, "postgres", destinationDriver)
	if err != nil {
		return fmt.Errorf("Could not execute migrations from migration directory '%v': %w", migrationsDir, err)
	}

	if m.ShouldForceSetLowerVersion {
		err := m.forceSetLowerVersion(migration, sourceDriver, destinationDriver)
		if err != nil {
			return err
		}
	}

	err = migration.Up()
	if err != nil && err != migrate.ErrNoChange { // migrate library reports that no change was required, using ErrNoChange
		if err == os.ErrNotExist {
			pkgLogger.Infof("\n*****************\nMigrate could not find migration file for the version in db.\nPlease set env RSERVER_SQLMIGRATOR_FORCE_SET_LOWER_VERSION to true and restart to force set version in DB to latest version of migration sql files\nAlso please keep in mind that this does not undo the additional migrations done in version specified in DB. It just sets the value in MigrationsTable and marks it as dirty false.\n*****************\n")
		}
		return fmt.Errorf("run migration from directory %q, %w", migrationsDir, err)
	}

	return nil
}

// MigrateFromTemplates migrates database with migration scripts provided by golang templates.
// Migration templates are read from all files in templatesDir and converted using provided context as template data.
// Directories inside templates directory are ignored.
func (m *Migrator) MigrateFromTemplates(templatesDir string, context interface{}) error {
	// look in templatesDir for migration template files
	fileInfos, err := migrations.FS.ReadDir(templatesDir)
	if err != nil {
		return fmt.Errorf("read migration template directory %q: %w", templatesDir, err)
	}

	if len(fileInfos) == 0 {
		return fmt.Errorf("empty migration template directory %q", templatesDir)
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
			path := filepath.Join(templatesDir, name)
			file, err := migrations.FS.Open(path)
			if err != nil {
				return nil, fmt.Errorf("open migration template %q: %w", path, err)
			}

			templateData, err := io.ReadAll(file)
			if err != nil {
				return nil, fmt.Errorf("read migration template %q: %w", name, err)
			}

			// parse template
			tmpl, err := template.New(name).Parse(string(templateData))
			if err != nil {
				return nil, fmt.Errorf("parse migration template %q: %w", name, err)
			}

			// execute template with given context
			buffer := new(bytes.Buffer)
			err = tmpl.Execute(buffer, context)
			if err != nil {
				return nil, fmt.Errorf("execute migration template %q: %w", name, err)
			}

			script := buffer.Bytes()

			return script, nil
		})

	sourceDriver, err := bindata.WithInstance(assetSource)
	if err != nil {
		return fmt.Errorf("create migration source from template directory %q: %w", templatesDir, err)
	}

	// create destination driver from db.handle
	destinationDriver, err := m.getDestinationDriver()
	if err != nil {
		return fmt.Errorf("create migration destination: %w", err)
	}

	// run the migration scripts
	migration, err := migrate.NewWithInstance("go-bindata", sourceDriver, "postgres", destinationDriver)
	if err != nil {
		return fmt.Errorf("setup migration from template directory %q: %w", templatesDir, err)
	}

	if m.ShouldForceSetLowerVersion {
		if err := m.forceSetLowerVersion(migration, sourceDriver, destinationDriver); err != nil {
			return err
		}
	}

	if m.RunAlways {
		if err := migration.Force(-1); err != nil {
			return fmt.Errorf("force migration version to 0: %w", err)
		}
	}

	err = migration.Up()
	if err != nil && err != migrate.ErrNoChange { // migrate library reports that no change was required, using ErrNoChange
		return fmt.Errorf("run migration from template directory %q, %w", templatesDir, err)
	}

	return nil
}

func (m *Migrator) getDestinationDriver() (database.Driver, error) {
	return postgres.WithInstance(m.Handle, &postgres.Config{MigrationsTable: m.MigrationsTable})
}

func latestSourceVersion(sourceDriver source.Driver) (int, error) {
	var v uint
	var err error
	v, err = sourceDriver.First()
	if err != nil {
		return 0, err
	}

	for {
		var nextVersion uint
		nextVersion, err = sourceDriver.Next(v)
		if err == os.ErrNotExist {
			break
		} else if pathErr, ok := err.(*os.PathError); ok && pathErr.Err == os.ErrNotExist {
			break
		} else if err != nil {
			return 0, err
		}
		v = nextVersion
	}
	return int(v), nil
}

func (m *Migrator) forceSetLowerVersion(migration *migrate.Migrate, sourceDriver source.Driver, destinationDriver database.Driver) error {
	// get current version in database migrations table
	versionInDB, _, err := destinationDriver.Version()
	if err != nil {
		return fmt.Errorf("get current migration version in DB: %w", err)
	}

	// check latest version on file
	latestVersionOnFile, err := latestSourceVersion(sourceDriver)
	if err != nil {
		return fmt.Errorf("check latest migration version on file: %w", err)
	}

	// force set version in DB to latestSourceVersion
	// to handle cases where we are reverting back to old version
	// this assumes applied changes on database are also compatible with older versions
	if versionInDB > latestVersionOnFile {
		pkgLogger.Infof("Force setting migration version to %d in %s", latestVersionOnFile, m.MigrationsTable)
		err = migration.Force(latestVersionOnFile)
		if err != nil {
			return fmt.Errorf("force set migration to latest version on file: %w", err)
		}
	}

	return nil
}
