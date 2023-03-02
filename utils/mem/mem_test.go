package mem

import (
	"testing"

	gomem "github.com/shirou/gopsutil/v3/mem"
	"github.com/stretchr/testify/require"
)

func TestMemCollector(t *testing.T) {
	const expectedCgroupsTotal = 100000 // setting a pretty low value to make sure the system running the test will have more memory than this
	const expectedCgroupsUsed = 9000
	mem, err := gomem.VirtualMemory()
	require.NoError(t, err)
	require.Greater(t, mem.Total, uint64(expectedCgroupsTotal), "cgroups total should be less than the actual total memory of the system running the tests")

	t.Run("without cgroups", func(t *testing.T) {
		c := &collector{basePath: "testdata/invalidpath"}
		s, err := c.Get()
		require.NoError(t, err)
		require.Greater(t, s.Total, uint64(0))
		require.Greater(t, s.Used, uint64(0))
		require.EqualValues(t, s.Total-s.Used, s.Available, "available memory should be total memory minus used memory")
		require.Greater(t, s.Total, s.Used, "total memory should be greater than used memory")
		require.Greater(t, s.Total, s.Available, "total memory should be greater than available memory")
		require.LessOrEqual(t, s.UsedPercent, float64(100))
		require.LessOrEqual(t, s.AvailablePercent, float64(100))
		require.EqualValues(t, float64(s.Used)*100/float64(s.Total), s.UsedPercent)
	})
	t.Run("with cgroups", func(t *testing.T) {
		c := &collector{basePath: "testdata/cgroups_v1_mem_limit"}
		s, err := c.Get()

		require.NoError(t, err)
		require.EqualValues(t, expectedCgroupsTotal, s.Total)
		require.EqualValues(t, expectedCgroupsUsed, s.Used)
		require.EqualValues(t, s.Total-s.Used, s.Available)
		require.EqualValues(t, float64(s.Used)*100/float64(s.Total), s.UsedPercent)
	})
}
