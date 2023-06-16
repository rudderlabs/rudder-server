package badgerdb_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/enterprise/suppress-user/internal/badgerdb"
	"github.com/rudderlabs/rudder-server/enterprise/suppress-user/internal/repotest"
)

// TestBadgerRepoSpec tests the badgerdb repository implementation.
func TestBadgerRepoSpec(t *testing.T) {
	path := t.TempDir()
	repo, err := badgerdb.NewRepository(path, logger.NOP, stats.Default)
	require.NoError(t, err)
	repotest.RunRepositoryTestSuite(t, repo)
}
