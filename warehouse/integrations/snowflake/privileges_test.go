package snowflake

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jsonrs"
	th "github.com/rudderlabs/rudder-server/testhelper"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestPrivileges(t *testing.T) {
	type credentials struct {
		Account              string `json:"account"`
		Database             string `json:"database"`
		Warehouse            string `json:"warehouse"`
		UseKeyPairAuth       bool   `json:"useKeyPairAuth"`
		PrivateKey           string `json:"privateKey"`
		PrivateKeyPassphrase string `json:"privateKeyPassphrase"`
	}
	t.Run("TestFetchSchema", func(t *testing.T) {
		/*
			u1 -> r1 -> no privilege
			u2 -> r2 -> USAGE privilege
			u3 -> r1 -> no privilege
			u3 -> r3 -> no privilege
			u4 -> r2 -> USAGE privilege
			u4 -> r4 -> MONITOR privilege
			u5 -> r5 -> MONITOR privilege
			u6 -> r6 -> OWNERSHIP privilege
		*/

		testKey := "SNOWFLAKE_PRIVILEGE_INTEGRATION_TEST_CREDENTIALS"
		rawCredentials, exists := os.LookupEnv(testKey)
		if !exists {
			if os.Getenv("FORCE_RUN_INTEGRATION_TESTS") == "true" {
				t.Fatalf("%s environment variable not set", testKey)
			}
			t.Skipf("Skipping %s as %s is not set", t.Name(), testKey)
		}

		var c credentials
		require.NoError(t, jsonrs.Unmarshal([]byte(rawCredentials), &c))

		ctx := context.Background()
		namespace := "FETCH_SCHEMA_TEST"
		warehouse := model.Warehouse{
			Source: backendconfig.SourceT{
				ID: "test_source_id",
			},
			Destination: backendconfig.DestinationT{
				ID: "test_destination_id",
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Name: whutils.SNOWFLAKE,
				},
				Config: map[string]any{
					"account":              c.Account,
					"database":             c.Database,
					"warehouse":            c.Warehouse,
					"useKeyPairAuth":       c.UseKeyPairAuth,
					"privateKey":           c.PrivateKey,
					"privateKeyPassphrase": c.PrivateKeyPassphrase,
					"namespace":            namespace,
				},
			},
			WorkspaceID: "test_workspace_id",
			Namespace:   namespace,
		}

		conf := config.New()
		conf.Set("Warehouse.snowflake.privileges.fetchSchema.enabled", true)

		t.Run("Configured Role: Single role - No privilege", func(t *testing.T) {
			w := th.Clone(t, warehouse)
			w.Destination.Config[model.UserSetting.String()] = "FETCH_SCHEMA_TEST_USER_1"
			w.Destination.Config[model.RoleSetting.String()] = "FETCH_SCHEMA_TEST_ROLE_1"

			sf := New(conf, logger.NOP, stats.NOP)
			err := sf.Setup(ctx, w, whutils.NewNoOpUploader())
			require.NoError(t, err)
			defer func() { sf.Cleanup(ctx) }()
			roles, err := sf.getGrantedRoles(ctx)
			require.NoError(t, err)
			require.ElementsMatch(t, []string{"FETCH_SCHEMA_TEST_ROLE_1"}, roles)
			privileges, err := sf.getShowGrantsPrivileges(ctx, fmt.Sprintf(`SHOW GRANTS TO ROLE %q;`, "FETCH_SCHEMA_TEST_ROLE_1"))
			require.NoError(t, err)
			require.ElementsMatch(t, []privilegeGrant{
				{"WAREHOUSE_INTEGRATION_TESTS", "USAGE", "DATABASE", "FETCH_SCHEMA_TEST_ROLE_1"},
				{"RUDDER_WAREHOUSE", "USAGE", "WAREHOUSE", "FETCH_SCHEMA_TEST_ROLE_1"},
			}, privileges)
			require.ErrorContains(t, sf.TestFetchSchema(ctx), "Schema 'WAREHOUSE_INTEGRATION_TESTS.FETCH_SCHEMA_TEST' does not exist or not authorized.")
		})
		t.Run("Configured Role: Single role - Insufficient privilege", func(t *testing.T) {
			w := th.Clone(t, warehouse)
			w.Destination.Config[model.UserSetting.String()] = "FETCH_SCHEMA_TEST_USER_5"
			w.Destination.Config[model.RoleSetting.String()] = "FETCH_SCHEMA_TEST_ROLE_5"

			sf := New(conf, logger.NOP, stats.NOP)
			err := sf.Setup(ctx, w, whutils.NewNoOpUploader())
			require.NoError(t, err)
			defer func() { sf.Cleanup(ctx) }()
			roles, err := sf.getGrantedRoles(ctx)
			require.NoError(t, err)
			require.ElementsMatch(t, []string{"FETCH_SCHEMA_TEST_ROLE_5"}, roles)
			privileges, err := sf.getShowGrantsPrivileges(ctx, fmt.Sprintf(`SHOW GRANTS TO ROLE %q;`, "FETCH_SCHEMA_TEST_ROLE_5"))
			require.NoError(t, err)
			require.ElementsMatch(t, []privilegeGrant{
				{"WAREHOUSE_INTEGRATION_TESTS", "USAGE", "DATABASE", "FETCH_SCHEMA_TEST_ROLE_5"},
				{"RUDDER_WAREHOUSE", "USAGE", "WAREHOUSE", "FETCH_SCHEMA_TEST_ROLE_5"},
				{"WAREHOUSE_INTEGRATION_TESTS.FETCH_SCHEMA_TEST", "MONITOR", "SCHEMA", "FETCH_SCHEMA_TEST_ROLE_5"},
			}, privileges)
			schemaPrivileges, err := sf.getShowGrantsPrivileges(ctx, fmt.Sprintf(`SHOW GRANTS ON SCHEMA %q;`, namespace))
			require.NoError(t, err)
			require.ElementsMatch(t, []privilegeGrant{
				{"WAREHOUSE_INTEGRATION_TESTS.FETCH_SCHEMA_TEST", "USAGE", "SCHEMA", "FETCH_SCHEMA_TEST_ROLE_2"},
				{"WAREHOUSE_INTEGRATION_TESTS.FETCH_SCHEMA_TEST", "MONITOR", "SCHEMA", "FETCH_SCHEMA_TEST_ROLE_4"},
				{"WAREHOUSE_INTEGRATION_TESTS.FETCH_SCHEMA_TEST", "MONITOR", "SCHEMA", "FETCH_SCHEMA_TEST_ROLE_5"},
				{"WAREHOUSE_INTEGRATION_TESTS.FETCH_SCHEMA_TEST", "OWNERSHIP", "SCHEMA", "FETCH_SCHEMA_TEST_ROLE_6"},
			}, schemaPrivileges)
			require.ErrorContains(t, sf.TestFetchSchema(ctx), "missing privileges: [USAGE] on schema \"FETCH_SCHEMA_TEST\"")
		})
		t.Run("Configured Role: Single role - Has privilege", func(t *testing.T) {
			w := th.Clone(t, warehouse)
			w.Destination.Config[model.UserSetting.String()] = "FETCH_SCHEMA_TEST_USER_2"
			w.Destination.Config[model.RoleSetting.String()] = "FETCH_SCHEMA_TEST_ROLE_2"

			sf := New(conf, logger.NOP, stats.NOP)
			err := sf.Setup(ctx, w, whutils.NewNoOpUploader())
			require.NoError(t, err)
			defer func() { sf.Cleanup(ctx) }()
			roles, err := sf.getGrantedRoles(ctx)
			require.NoError(t, err)
			require.ElementsMatch(t, []string{"FETCH_SCHEMA_TEST_ROLE_2"}, roles)
			privileges, err := sf.getShowGrantsPrivileges(ctx, fmt.Sprintf(`SHOW GRANTS TO ROLE %q;`, "FETCH_SCHEMA_TEST_ROLE_2"))
			require.NoError(t, err)
			require.ElementsMatch(t, []privilegeGrant{
				{"WAREHOUSE_INTEGRATION_TESTS", "USAGE", "DATABASE", "FETCH_SCHEMA_TEST_ROLE_2"},
				{"RUDDER_WAREHOUSE", "USAGE", "WAREHOUSE", "FETCH_SCHEMA_TEST_ROLE_2"},
				{"WAREHOUSE_INTEGRATION_TESTS.FETCH_SCHEMA_TEST", "USAGE", "SCHEMA", "FETCH_SCHEMA_TEST_ROLE_2"},
				{"WAREHOUSE_INTEGRATION_TESTS.FETCH_SCHEMA_TEST.TEST_TABLE", "SELECT", "TABLE", "FETCH_SCHEMA_TEST_ROLE_2"},
			}, privileges)
			require.NoError(t, sf.TestFetchSchema(ctx))
		})
		t.Run("Configured Role: Single role - Has Ownership privilege", func(t *testing.T) {
			w := th.Clone(t, warehouse)
			w.Destination.Config[model.UserSetting.String()] = "FETCH_SCHEMA_TEST_USER_6"
			w.Destination.Config[model.RoleSetting.String()] = "FETCH_SCHEMA_TEST_ROLE_6"

			sf := New(conf, logger.NOP, stats.NOP)
			err := sf.Setup(ctx, w, whutils.NewNoOpUploader())
			require.NoError(t, err)
			defer func() { sf.Cleanup(ctx) }()
			roles, err := sf.getGrantedRoles(ctx)
			require.NoError(t, err)
			require.ElementsMatch(t, []string{"FETCH_SCHEMA_TEST_ROLE_6"}, roles)
			privileges, err := sf.getShowGrantsPrivileges(ctx, fmt.Sprintf(`SHOW GRANTS TO ROLE %q;`, "FETCH_SCHEMA_TEST_ROLE_6"))
			require.NoError(t, err)
			require.ElementsMatch(t, []privilegeGrant{
				{"WAREHOUSE_INTEGRATION_TESTS", "USAGE", "DATABASE", "FETCH_SCHEMA_TEST_ROLE_6"},
				{"RUDDER_WAREHOUSE", "USAGE", "WAREHOUSE", "FETCH_SCHEMA_TEST_ROLE_6"},
				{"WAREHOUSE_INTEGRATION_TESTS.FETCH_SCHEMA_TEST", "OWNERSHIP", "SCHEMA", "FETCH_SCHEMA_TEST_ROLE_6"},
				{"WAREHOUSE_INTEGRATION_TESTS.FETCH_SCHEMA_TEST.TEST_TABLE", "SELECT", "TABLE", "FETCH_SCHEMA_TEST_ROLE_6"},
			}, privileges)
			require.NoError(t, sf.TestFetchSchema(ctx))
		})
		t.Run("Non-Configured Role: Single role - No privilege", func(t *testing.T) {
			w := th.Clone(t, warehouse)
			w.Destination.Config[model.UserSetting.String()] = "FETCH_SCHEMA_TEST_USER_1"
			w.Destination.Config[model.RoleSetting.String()] = ""

			sf := New(conf, logger.NOP, stats.NOP)
			err := sf.Setup(ctx, w, whutils.NewNoOpUploader())
			require.NoError(t, err)
			defer func() { sf.Cleanup(ctx) }()
			roles, err := sf.getGrantedRoles(ctx)
			require.NoError(t, err)
			require.ElementsMatch(t, []string{"FETCH_SCHEMA_TEST_ROLE_1"}, roles)
			privileges, err := sf.getShowGrantsPrivileges(ctx, fmt.Sprintf(`SHOW GRANTS TO ROLE %q;`, "FETCH_SCHEMA_TEST_ROLE_1"))
			require.NoError(t, err)
			require.ElementsMatch(t, []privilegeGrant{
				{"WAREHOUSE_INTEGRATION_TESTS", "USAGE", "DATABASE", "FETCH_SCHEMA_TEST_ROLE_1"},
				{"RUDDER_WAREHOUSE", "USAGE", "WAREHOUSE", "FETCH_SCHEMA_TEST_ROLE_1"},
			}, privileges)
			require.ErrorContains(t, sf.TestFetchSchema(ctx), "Schema 'WAREHOUSE_INTEGRATION_TESTS.FETCH_SCHEMA_TEST' does not exist or not authorized.")
		})
		t.Run("Non-Configured Role: Single role - Has privilege", func(t *testing.T) {
			w := th.Clone(t, warehouse)
			w.Destination.Config[model.UserSetting.String()] = "FETCH_SCHEMA_TEST_USER_2"
			w.Destination.Config[model.RoleSetting.String()] = ""

			sf := New(conf, logger.NOP, stats.NOP)
			err := sf.Setup(ctx, w, whutils.NewNoOpUploader())
			require.NoError(t, err)
			defer func() { sf.Cleanup(ctx) }()
			roles, err := sf.getGrantedRoles(ctx)
			require.NoError(t, err)
			require.ElementsMatch(t, []string{"FETCH_SCHEMA_TEST_ROLE_2"}, roles)
			privileges, err := sf.getShowGrantsPrivileges(ctx, fmt.Sprintf(`SHOW GRANTS TO ROLE %q;`, "FETCH_SCHEMA_TEST_ROLE_2"))
			require.NoError(t, err)
			require.ElementsMatch(t, []privilegeGrant{
				{"WAREHOUSE_INTEGRATION_TESTS", "USAGE", "DATABASE", "FETCH_SCHEMA_TEST_ROLE_2"},
				{"RUDDER_WAREHOUSE", "USAGE", "WAREHOUSE", "FETCH_SCHEMA_TEST_ROLE_2"},
				{"WAREHOUSE_INTEGRATION_TESTS.FETCH_SCHEMA_TEST", "USAGE", "SCHEMA", "FETCH_SCHEMA_TEST_ROLE_2"},
				{"WAREHOUSE_INTEGRATION_TESTS.FETCH_SCHEMA_TEST.TEST_TABLE", "SELECT", "TABLE", "FETCH_SCHEMA_TEST_ROLE_2"},
			}, privileges)
			require.NoError(t, sf.TestFetchSchema(ctx))
		})
		t.Run("Non-Configured Role: Multiple role - No privilege in any", func(t *testing.T) {
			w := th.Clone(t, warehouse)
			w.Destination.Config[model.UserSetting.String()] = "FETCH_SCHEMA_TEST_USER_3"
			w.Destination.Config[model.RoleSetting.String()] = ""

			sf := New(conf, logger.NOP, stats.NOP)
			err := sf.Setup(ctx, w, whutils.NewNoOpUploader())
			require.NoError(t, err)
			defer func() { sf.Cleanup(ctx) }()
			roles, err := sf.getGrantedRoles(ctx)
			require.NoError(t, err)
			require.ElementsMatch(t, []string{"FETCH_SCHEMA_TEST_ROLE_1", "FETCH_SCHEMA_TEST_ROLE_3"}, roles)
			privileges, err := sf.getShowGrantsPrivileges(ctx, fmt.Sprintf(`SHOW GRANTS TO ROLE %q;`, "FETCH_SCHEMA_TEST_ROLE_3"))
			require.NoError(t, err)
			require.ElementsMatch(t, []privilegeGrant{
				{"WAREHOUSE_INTEGRATION_TESTS", "USAGE", "DATABASE", "FETCH_SCHEMA_TEST_ROLE_3"},
				{"RUDDER_WAREHOUSE", "USAGE", "WAREHOUSE", "FETCH_SCHEMA_TEST_ROLE_3"},
			}, privileges)
			require.ErrorContains(t, sf.TestFetchSchema(ctx), "Schema 'WAREHOUSE_INTEGRATION_TESTS.FETCH_SCHEMA_TEST' does not exist or not authorized.")
		})
		t.Run("Non-Configured Role: Multiple role - Some has privilege", func(t *testing.T) {
			w := th.Clone(t, warehouse)
			w.Destination.Config[model.UserSetting.String()] = "FETCH_SCHEMA_TEST_USER_4"
			w.Destination.Config[model.RoleSetting.String()] = ""

			sf := New(conf, logger.NOP, stats.NOP)
			err := sf.Setup(ctx, w, whutils.NewNoOpUploader())
			require.NoError(t, err)
			defer func() { sf.Cleanup(ctx) }()
			roles, err := sf.getGrantedRoles(ctx)
			require.NoError(t, err)
			require.ElementsMatch(t, []string{"FETCH_SCHEMA_TEST_ROLE_2", "FETCH_SCHEMA_TEST_ROLE_4"}, roles)
			privileges, err := sf.getShowGrantsPrivileges(ctx, fmt.Sprintf(`SHOW GRANTS TO ROLE %q;`, "FETCH_SCHEMA_TEST_ROLE_4"))
			require.NoError(t, err)
			require.ElementsMatch(t, []privilegeGrant{
				{"WAREHOUSE_INTEGRATION_TESTS", "USAGE", "DATABASE", "FETCH_SCHEMA_TEST_ROLE_4"},
				{"RUDDER_WAREHOUSE", "USAGE", "WAREHOUSE", "FETCH_SCHEMA_TEST_ROLE_4"},
				{"WAREHOUSE_INTEGRATION_TESTS.FETCH_SCHEMA_TEST", "MONITOR", "SCHEMA", "FETCH_SCHEMA_TEST_ROLE_4"},
			}, privileges)
			require.NoError(t, sf.TestFetchSchema(ctx))
		})
	})
}
