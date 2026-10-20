package jobsdb

import (
	"fmt"
	"strconv"
	"strings"
)

// pgSnapshot is a Postgres visibility snapshot, as returned by pg_current_snapshot().
//
// Every transaction below xmin had finished when the snapshot was taken, every transaction at
// or above xmax had not started, and xip lists the transactions in between that were still
// running.
type pgSnapshot struct {
	xmin uint64
	xmax uint64
	xip  map[uint64]struct{}
}

// parsePgSnapshot parses the text form of a pg_snapshot: "xmin:xmax:xip1,xip2,...".
func parsePgSnapshot(s string) (pgSnapshot, error) {
	parts := strings.Split(s, ":")
	if len(parts) != 3 {
		return pgSnapshot{}, fmt.Errorf("invalid snapshot %q: expected xmin:xmax:xip", s)
	}
	xmin, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return pgSnapshot{}, fmt.Errorf("invalid snapshot %q: xmin: %w", s, err)
	}
	xmax, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return pgSnapshot{}, fmt.Errorf("invalid snapshot %q: xmax: %w", s, err)
	}
	if xmax < xmin {
		return pgSnapshot{}, fmt.Errorf("invalid snapshot %q: xmax < xmin", s)
	}
	snap := pgSnapshot{xmin: xmin, xmax: xmax}
	if parts[2] != "" {
		ids := strings.Split(parts[2], ",")
		snap.xip = make(map[uint64]struct{}, len(ids))
		for _, id := range ids {
			xid, err := strconv.ParseUint(id, 10, 64)
			if err != nil {
				return pgSnapshot{}, fmt.Errorf("invalid snapshot %q: xip: %w", s, err)
			}
			snap.xip[xid] = struct{}{}
		}
	}
	return snap, nil
}

// sees reports whether the snapshot sees the effects of the transaction with the given id,
// assuming that transaction committed.
func (s pgSnapshot) sees(xid uint64) bool {
	if xid < s.xmin {
		return true
	}
	if xid >= s.xmax {
		return false
	}
	_, running := s.xip[xid]
	return !running
}
