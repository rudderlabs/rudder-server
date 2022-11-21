package badgerdb_test

import (
	"testing"

	"github.com/rudderlabs/rudder-server/enterprise/suppress-user/internal/badgerdb"
	"github.com/rudderlabs/rudder-server/enterprise/suppress-user/internal/repotest"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/stretchr/testify/require"
)

// TestBadgerRepoSpec tests the badgerdb repository implementation.
func TestBadgerRepoSpec(t *testing.T) {
	path := t.TempDir()
	repo, err := badgerdb.NewRepository(path, logger.NOP)
	require.NoError(t, err)
	repotest.RunRepositoryTestSuite(t, repo)
}
