package cgroup_test

import (
	"testing"

	"github.com/rudderlabs/rudder-server/utils/bytesize"
	"github.com/rudderlabs/rudder-server/utils/mem/internal/cgroup"
	"github.com/stretchr/testify/require"
)

func TestCgroupMemory(t *testing.T) {
	t.Run("cgroups v1 with limit", func(t *testing.T) {
		basePath := "testdata/cgroups_v1_mem_limit"
		totalMem := int(100 * bytesize.GB)
		limit := cgroup.GetMemoryLimit(basePath, totalMem)

		require.EqualValues(t, 25*bytesize.GB, limit, "when a limit is set, this limit should be returned")
		require.EqualValues(t, 9456156672, cgroup.GetMemoryUsage(basePath))
	})

	t.Run("cgroups v1 with self limit", func(t *testing.T) {
		basePath := "testdata/cgroups_v1_mem_limit_proc_self"
		totalMem := int(100 * bytesize.GB)
		limit := cgroup.GetMemoryLimit(basePath, totalMem)

		require.EqualValues(t, 25*bytesize.GB, limit, "when a limit is set, this limit should be returned")
		require.EqualValues(t, 9456156672, cgroup.GetMemoryUsage(basePath))
	})

	t.Run("cgroups v1 with hierarchical limit", func(t *testing.T) {
		basePath := "testdata/cgroups_v1_mem_hierarchy"
		totalMem := int(100 * bytesize.GB)
		limit := cgroup.GetMemoryLimit(basePath, totalMem)

		require.EqualValues(t, 25*bytesize.GB, limit, "when a hierarchical limit is set, this limit should be returned")
		require.EqualValues(t, 9456156672, cgroup.GetMemoryUsage(basePath))
	})

	t.Run("cgroups v1 no limit", func(t *testing.T) {
		basePath := "testdata/cgroups_v1_mem_no_limit"
		totalMem := int(100 * bytesize.GB)
		limit := cgroup.GetMemoryLimit(basePath, totalMem)

		require.EqualValues(t, totalMem, limit, "when no limit is set, total memory should be returned")
		require.EqualValues(t, 9456156672, cgroup.GetMemoryUsage(basePath))
	})

	t.Run("cgroups v2 with limit", func(t *testing.T) {
		basePath := "testdata/cgroups_v2_mem_limit"
		totalMem := int(100 * bytesize.GB)
		limit := cgroup.GetMemoryLimit(basePath, totalMem)

		require.EqualValues(t, 32*bytesize.GB, limit, "when a limit is set, this limit should be returned")
		require.EqualValues(t, 34263040, cgroup.GetMemoryUsage(basePath))
	})

	t.Run("cgroups v2 no limit", func(t *testing.T) {
		basePath := "testdata/cgroups_v2_mem_no_limit"
		totalMem := int(100 * bytesize.GB)
		limit := cgroup.GetMemoryLimit(basePath, totalMem)

		require.EqualValues(t, totalMem, limit, "when no limit is set, total memory should be returned")
		require.EqualValues(t, 34263040, cgroup.GetMemoryUsage(basePath))
	})

	t.Run("no cgroups info", func(t *testing.T) {
		basePath := "testdata/invalid_path"
		totalMem := int(100 * bytesize.GB)
		limit := cgroup.GetMemoryLimit(basePath, totalMem)

		require.EqualValues(t, limit, limit, "when no cgroups info is available, this limit should be returned")
		require.EqualValues(t, 0, cgroup.GetMemoryUsage(basePath))
	})
}
