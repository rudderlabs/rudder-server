package dsindex_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/jobsdb/internal/dsindex"
)

func Test_Index_Parse(t *testing.T) {
	t.Run("success scenarios", func(t *testing.T) {
		idx, err := dsindex.Parse("0")
		require.NoError(t, err)
		require.Equal(t, "0", idx.String())
		require.Equal(t, 1, idx.Length())

		idx, err = dsindex.Parse("0_1")
		require.NoError(t, err)
		require.Equal(t, "0_1", idx.String())
		require.Equal(t, 2, idx.Length())

		idx, err = dsindex.Parse("1_2_3")
		require.NoError(t, err)
		require.Equal(t, "1_2_3", idx.String())
		require.Equal(t, 3, idx.Length())
	})

	t.Run("error scenarios", func(t *testing.T) {
		_, err := dsindex.Parse("")
		require.Error(t, err)

		_, err = dsindex.Parse("a")
		require.Error(t, err)

		_, err = dsindex.Parse("1a")
		require.Error(t, err)

		_, err = dsindex.Parse("1_a")
		require.Error(t, err)

		_, err = dsindex.Parse("1_0") // zero is allowed only as first segment
		require.Error(t, err)

		_, err = dsindex.Parse("1_-1")
		require.Error(t, err)

		require.Panics(t, func() { dsindex.MustParse("1_-1") })
	})
}

func Test_Index_Less(t *testing.T) {
	require.True(t, dsindex.MustParse("-10").Less(dsindex.MustParse("-9")))
	require.True(t, dsindex.MustParse("0").Less(dsindex.MustParse("1")))
	require.True(t, dsindex.MustParse("0_1").Less(dsindex.MustParse("1")))
	require.True(t, dsindex.MustParse("0_1_2").Less(dsindex.MustParse("1")))
	require.True(t, dsindex.MustParse("1_1").Less(dsindex.MustParse("1_2")))
	require.True(t, dsindex.MustParse("1_1_1").Less(dsindex.MustParse("1_1_3")))
	require.True(t, dsindex.MustParse("11").Less(dsindex.MustParse("11_1")))

	require.False(t, dsindex.MustParse("1_1_1").Less(dsindex.MustParse("1_1")))
}

func Test_Index_Increment(t *testing.T) {
	t.Run("success scenarios", func(t *testing.T) {
		require.Equal(t, "-9", dsindex.MustParse("-10").MustIncrement(0).String())
		require.Equal(t, "0_1", dsindex.MustParse("0").MustIncrement(1).String())
		require.Equal(t, "0_2", dsindex.MustParse("0_1").MustIncrement(1).String())
		require.Equal(t, "1", dsindex.MustParse("0_1").MustIncrement(0).String())
		require.Equal(t, "0_2", dsindex.MustParse("0_1_2").MustIncrement(1).String())
	})

	t.Run("error scenarios", func(t *testing.T) {
		_, err := dsindex.MustParse("0").Increment(-1)
		require.Error(t, err)

		_, err = dsindex.MustParse("0").Increment(2)
		require.Error(t, err)

		_, err = dsindex.MustParse("0").Increment(3)
		require.Error(t, err)

		require.Panics(t, func() { dsindex.MustParse("0").MustIncrement(3) })
	})
}

func Test_Index_Bump(t *testing.T) {
	t.Run("success scenarios", func(t *testing.T) {
		require.Equal(t, "0_1", dsindex.MustParse("0").MustBump(dsindex.MustParse("1")).String())
		require.Equal(t, "0_1", dsindex.MustParse("0").MustBump(dsindex.MustParse("2")).String())
		require.Equal(t, "0_1", dsindex.MustParse("0").MustBump(dsindex.MustParse("3")).String())
		require.Equal(t, "1_2_2", dsindex.MustParse("1_2_1").MustBump(dsindex.MustParse("1_2_3")).String())
		require.Equal(t, "-11_1", dsindex.MustParse("-11").MustBump(dsindex.MustParse("-10")).String())
	})

	t.Run("error scenarios", func(t *testing.T) {
		_, err := dsindex.MustParse("1_2_1").Bump(dsindex.MustParse("1_2_1"))
		require.Error(t, err, "bump should fail if index is not less than other")

		_, err = dsindex.MustParse("1_2").Bump(dsindex.MustParse("1_1"))
		require.Error(t, err, "bump should fail if index is not less than other")

		require.Panics(t, func() { dsindex.MustParse("1_2").MustBump(dsindex.MustParse("1_1")) })
	})
}
