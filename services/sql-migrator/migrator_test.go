package migrator_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/ory/dockertest/v3"
	migrator "github.com/rudderlabs/rudder-server/services/sql-migrator"
	"github.com/rudderlabs/rudder-server/sql/migrations"
	"github.com/rudderlabs/rudder-server/testhelper/destination"
	"github.com/stretchr/testify/require"
)

func TestMigrate(t *testing.T) {
	dirs, err := migrations.FS.ReadDir(".")
	require.NoError(t, err)

	var migrationDir []string

	for _, dir := range dirs {
		if dir.IsDir() {
			migrationDir = append(migrationDir, dir.Name())
		}
	}

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	postgre, err := destination.SetupPostgres(pool, t)
	require.NoError(t, err)

	for _, dir := range migrationDir {
		t.Run(dir, func(t *testing.T) {
			if strings.HasPrefix(dir, "jobsdb") {
				t.Skip("template migrations are tested on jobsdb")
				return
			}

			m := migrator.Migrator{
				MigrationsTable: fmt.Sprintf("migrations_%s", dir),
				Handle:          postgre.DB,
			}

			err := m.Migrate(dir)
			require.NoError(t, err)
		})
	}
}
