package backendconfig

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/ProtonMail/go-crypto/openpgp"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
)

// fixturePassphraseEnv carries the passphrase the committed fixtures are encrypted with.
const fixturePassphraseEnv = "WORKSPACE_CONFIG_FIXTURE_PASSPHRASE"

// TestV2MapperAgainstFixtures maps a captured v2 namespace response and holds the result against
// the v1 response captured from the same namespace at the same moment - the only check that covers
// the mapper against production shaped definitions rather than hand written ones.
//
// The pair is committed encrypted, the repository being public and the scrubbing not hiding a
// namespace's shape, and the test decrypts it itself:
//
//	WORKSPACE_CONFIG_FIXTURE_PASSPHRASE=... go test ./backend-config/ -run TestV2MapperAgainstFixtures
//
// A divergence here is not always the mapper's: the scrubbing has to move both sides of every join
// the mapper makes. backend-config/testdata/namespace_capture_fixtures.md covers refreshing the
// pair, and telling those two apart.
func TestV2MapperAgainstFixtures(t *testing.T) {
	var v1Config map[string]*ConfigT
	require.NoError(t, jsonrs.Unmarshal(fixture(t, "v1"), &v1Config))

	var v2Config v2NamespaceConfig
	require.NoError(t, jsonrs.Unmarshal(fixture(t, "v2"), &v2Config))

	require.NotEmpty(t, v1Config)
	require.Equal(t, len(v1Config), len(v2Config.Workspaces), "the two captures must cover the same namespace")

	mapper := newConfigMapper(logger.NOP)
	for workspaceID, expected := range v1Config {
		t.Run(workspaceID, func(t *testing.T) {
			workspace, ok := v2Config.Workspaces[workspaceID]
			require.True(t, ok, "workspace missing from the v2 capture")
			require.NotNil(t, workspace)

			actual, err := mapper.Map(workspaceID, workspace.Raw, v2Config.v2Catalogues)
			require.NoError(t, err)

			// the resolver runs both of these on both paths, so the comparison has to as well
			expected.ApplyReplaySources()
			expected.processAccountAssociations()
			actual.ApplyReplaySources()
			actual.processAccountAssociations()

			t.Logf("sources %d, destinations %d, connections %d",
				len(actual.Sources), countDestinations(actual), len(actual.Connections))

			// normalized exactly as the live shadow comparison normalizes, which also makes this
			// the one place shadowNormalize meets production shaped data
			if diff := cmp.Diff(shadowNormalize(*expected), shadowNormalize(actual), configCmpOptions()...); diff != "" {
				t.Errorf("mapped config differs from the v1 capture (-v1 +v2):\n%s", diff)
			}
		})
	}
}

// fixture decrypts one half of the committed capture.
//
// Without the passphrase there is nothing to run against, so the test skips - unless CI says these
// must run, in which case a missing passphrase is a broken secret rather than a local setup, and
// silently skipping would retire the mapper's only production shaped coverage without a word.
func fixture(t *testing.T, version string) []byte {
	t.Helper()
	passphrase, ok := os.LookupEnv(fixturePassphraseEnv)
	if !ok || passphrase == "" {
		if os.Getenv("FORCE_RUN_INTEGRATION_TESTS") == "true" {
			t.Fatalf("%s environment variable not set", fixturePassphraseEnv)
		}
		t.Skipf("Skipping %s: %s is not set, see backend-config/testdata/namespace_capture_fixtures.md",
			t.Name(), fixturePassphraseEnv)
	}
	encrypted := filepath.Join("testdata", fmt.Sprintf("sample_namespace_%s.json.gpg", version))
	file, err := os.Open(encrypted)
	require.NoError(t, err)
	defer func() { _ = file.Close() }()

	var attempted bool
	message, err := openpgp.ReadMessage(file, nil, func([]openpgp.Key, bool) ([]byte, error) {
		if attempted { // the library asks again on a wrong passphrase, which would never end
			return nil, fmt.Errorf("passphrase in %s does not decrypt %s", fixturePassphraseEnv, encrypted)
		}
		attempted = true
		return []byte(passphrase), nil
	}, nil)
	require.NoErrorf(t, err, "decrypting %s", encrypted)

	body, err := io.ReadAll(message.UnverifiedBody)
	require.NoErrorf(t, err, "reading %s", encrypted)
	return body
}

func countDestinations(config ConfigT) int {
	var count int
	for _, source := range config.Sources {
		count += len(source.Destinations)
	}
	return count
}
