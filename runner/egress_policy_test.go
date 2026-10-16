package runner

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect"

	"github.com/rudderlabs/rudder-server/app"
)

func TestSetupSQLConnectEgressPolicyFromEnv(t *testing.T) {
	t.Run("applies trimmed non-empty CIDRs when required", func(t *testing.T) {
		t.Setenv("RSERVER_WAREHOUSE_EGRESS_POLICY_REQUIRED", "true")
		t.Setenv("RSERVER_WAREHOUSE_EGRESS_BLOCKED_CIDRS", " 10.0.0.0/8, ,\t2001:db8::/32 ,, ")

		var applied []string
		stubSetSQLConnectEgressPolicy(t, func(blockedCIDRs []string) error {
			applied = blockedCIDRs
			return nil
		})

		require.NoError(t, setupSQLConnectEgressPolicyFromEnv())
		require.Equal(t, []string{"10.0.0.0/8", "2001:db8::/32"}, applied)
	})

	for _, raw := range []string{"", " , \t, "} {
		t.Run("errors when required CIDRs are empty", func(t *testing.T) {
			t.Setenv("RSERVER_WAREHOUSE_EGRESS_POLICY_REQUIRED", "true")
			t.Setenv("RSERVER_WAREHOUSE_EGRESS_BLOCKED_CIDRS", raw)

			called := false
			stubSetSQLConnectEgressPolicy(t, func([]string) error {
				called = true
				return nil
			})

			err := setupSQLConnectEgressPolicyFromEnv()
			require.ErrorContains(t, err, "must contain at least one CIDR")
			require.False(t, called)
		})
	}

	t.Run("rejects the whole list when one CIDR is invalid", func(t *testing.T) {
		t.Setenv("RSERVER_WAREHOUSE_EGRESS_POLICY_REQUIRED", "true")
		t.Setenv("RSERVER_WAREHOUSE_EGRESS_BLOCKED_CIDRS", "10.0.0.0/8,not-a-cidr")
		t.Cleanup(func() { require.NoError(t, sqlconnect.SetEgressPolicy(nil)) })

		var applied []string
		stubSetSQLConnectEgressPolicy(t, func(blockedCIDRs []string) error {
			applied = blockedCIDRs
			return sqlconnect.SetEgressPolicy(blockedCIDRs)
		})

		err := setupSQLConnectEgressPolicyFromEnv()
		require.ErrorContains(t, err, "setting SQL connection egress policy")
		require.Equal(t, []string{"10.0.0.0/8", "not-a-cidr"}, applied)
	})

	t.Run("propagates policy application errors", func(t *testing.T) {
		t.Setenv("RSERVER_WAREHOUSE_EGRESS_POLICY_REQUIRED", "true")
		t.Setenv("RSERVER_WAREHOUSE_EGRESS_BLOCKED_CIDRS", "10.0.0.0/8")

		stubSetSQLConnectEgressPolicy(t, func([]string) error {
			return errors.New("policy rejected")
		})

		err := setupSQLConnectEgressPolicyFromEnv()
		require.ErrorContains(t, err, "setting SQL connection egress policy: policy rejected")
	})

	t.Run("errors on a malformed required value", func(t *testing.T) {
		t.Setenv("RSERVER_WAREHOUSE_EGRESS_POLICY_REQUIRED", "sometimes")
		t.Setenv("RSERVER_WAREHOUSE_EGRESS_BLOCKED_CIDRS", "10.0.0.0/8")

		called := false
		stubSetSQLConnectEgressPolicy(t, func([]string) error {
			called = true
			return nil
		})

		err := setupSQLConnectEgressPolicyFromEnv()
		require.ErrorContains(t, err, "parsing RSERVER_WAREHOUSE_EGRESS_POLICY_REQUIRED")
		require.False(t, called)
	})

	for _, required := range []string{"", "false"} {
		t.Run("skips when policy is not required", func(t *testing.T) {
			t.Setenv("RSERVER_WAREHOUSE_EGRESS_POLICY_REQUIRED", required)
			t.Setenv("RSERVER_WAREHOUSE_EGRESS_BLOCKED_CIDRS", "10.0.0.0/8")

			called := false
			stubSetSQLConnectEgressPolicy(t, func([]string) error {
				called = true
				return nil
			})

			require.NoError(t, setupSQLConnectEgressPolicyFromEnv())
			require.False(t, called)
		})
	}

	t.Run("ignores values from the config provider", func(t *testing.T) {
		config.Set("RSERVER_WAREHOUSE_EGRESS_POLICY_REQUIRED", true)
		config.Set("RSERVER_WAREHOUSE_EGRESS_BLOCKED_CIDRS", "10.0.0.0/8")
		t.Cleanup(config.Reset)
		t.Setenv("RSERVER_WAREHOUSE_EGRESS_POLICY_REQUIRED", "")
		t.Setenv("RSERVER_WAREHOUSE_EGRESS_BLOCKED_CIDRS", "")

		called := false
		stubSetSQLConnectEgressPolicy(t, func([]string) error {
			called = true
			return nil
		})

		require.NoError(t, setupSQLConnectEgressPolicyFromEnv())
		require.False(t, called)
	})
}

func TestRunnerSetupSQLConnectEgressPolicyFromEnv(t *testing.T) {
	t.Setenv("RSERVER_WAREHOUSE_EGRESS_POLICY_REQUIRED", "true")

	for _, blockedCIDRs := range []string{"", "not-a-cidr"} {
		t.Run("non-warehouse mode skips invalid policy", func(t *testing.T) {
			t.Setenv("RSERVER_WAREHOUSE_EGRESS_BLOCKED_CIDRS", blockedCIDRs)

			called := false
			stubSetSQLConnectEgressPolicy(t, func([]string) error {
				called = true
				return errors.New("invalid CIDR")
			})

			r := &Runner{appType: app.GATEWAY, warehouseMode: config.EmbeddedMode}
			require.NoError(t, r.setupSQLConnectEgressPolicyFromEnv())
			require.False(t, called)
		})

		t.Run("warehouse mode rejects invalid policy", func(t *testing.T) {
			t.Setenv("RSERVER_WAREHOUSE_EGRESS_BLOCKED_CIDRS", blockedCIDRs)

			stubSetSQLConnectEgressPolicy(t, func([]string) error {
				return errors.New("invalid CIDR")
			})

			r := &Runner{appType: app.EMBEDDED, warehouseMode: config.EmbeddedMode}
			require.Error(t, r.setupSQLConnectEgressPolicyFromEnv())
		})
	}
}

func stubSetSQLConnectEgressPolicy(t *testing.T, stub func([]string) error) {
	t.Helper()
	previous := setSQLConnectEgressPolicy
	setSQLConnectEgressPolicy = stub
	t.Cleanup(func() { setSQLConnectEgressPolicy = previous })
}
