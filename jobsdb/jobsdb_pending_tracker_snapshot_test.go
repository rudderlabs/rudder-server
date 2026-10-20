package jobsdb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPgSnapshot(t *testing.T) {
	t.Run("parse and sees", func(t *testing.T) {
		snap, err := parsePgSnapshot("701:705:701,703") // xmin is the oldest running transaction, so it is in xip
		require.NoError(t, err)
		for xid, seen := range map[uint64]bool{
			650: true, // below xmin: finished before the snapshot
			700: true,
			701: false, // xmin itself: still running
			702: true,  // in the window, not running
			703: false,
			704: true,
			705: false, // at xmax: started after the snapshot
			710: false,
		} {
			require.Equal(t, seen, snap.sees(xid), "xid %d", xid)
		}
	})
	t.Run("no running transactions", func(t *testing.T) {
		snap, err := parsePgSnapshot("800:800:")
		require.NoError(t, err)
		require.True(t, snap.sees(799))
		require.False(t, snap.sees(800))
	})
	t.Run("invalid", func(t *testing.T) {
		for _, s := range []string{"", "1:2", "a:2:", "1:b:", "5:4:", "1:3:x"} {
			_, err := parsePgSnapshot(s)
			require.Error(t, err, s)
		}
	})
}
