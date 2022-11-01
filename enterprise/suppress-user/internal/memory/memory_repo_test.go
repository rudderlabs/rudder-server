package memory_test

import (
	"testing"

	"github.com/rudderlabs/rudder-server/enterprise/suppress-user/internal/memory"
	"github.com/rudderlabs/rudder-server/enterprise/suppress-user/internal/repotest"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

// TestMemoryRepoSpec tests the memory repository implementation.
func TestMemoryRepoSpec(t *testing.T) {
	repotest.RunRepositoryTestSuite(t, memory.NewRepository(logger.NOP))
}
