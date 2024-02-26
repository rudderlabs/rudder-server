package migrator_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"
	migrator "github.com/rudderlabs/rudder-server/services/sql-migrator"
	"github.com/rudderlabs/rudder-server/sql/migrations"
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

	postgre, err := postgres.Setup(pool, t)
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
			var err error
			if strings.HasPrefix(dir, "reports_always") {
				err = m.MigrateFromTemplates("reports_always", map[string]interface{}{
					"config": config.Default,
				})
			} else {
				err = m.Migrate(dir)
			}
			require.NoError(t, err)
		})
	}

	t.Run("validate if autovacuum_vacuum_cost_limit is being set", func(t *testing.T) {
		query := "select reloptions from pg_class where relname = 'reports';"
		var value interface{}
		require.NoError(t, postgre.DB.QueryRow(query).Scan(&value))
		require.Nil(t, value) // value should be nil if config is not set
		config.Set("Reporting.autoVacuumCostLimit", 300)
		m := migrator.Migrator{
			MigrationsTable: "migrations_reports_always",
			Handle:          postgre.DB,
			RunAlways:       true,
		}
		require.NoError(t, m.MigrateFromTemplates("reports_always", map[string]interface{}{
			"config": config.Default,
		}))
		var costLimit string
		require.NoError(t, postgre.DB.QueryRow(query).Scan(&costLimit))
		require.Equal(t, "{autovacuum_vacuum_cost_limit=300}", costLimit) // value should be set to 300
	})
}
