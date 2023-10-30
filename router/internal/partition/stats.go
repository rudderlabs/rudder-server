package partition

import (
	"math"
	"slices"
	"sync"
	"time"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/stats/metric"
)

// NewStats returns a new, initialised partition stats
func NewStats() *Stats {
	return &Stats{
		pstats: make(map[string]*pstat),
	}
}

// Stats keeps track of throughput and error rates for each partition
type Stats struct {
	pstatsMu sync.RWMutex
	pstats   map[string]*pstat
}

// Update updates the stats for the given partition in terms of throughput, total requests and errors
func (s *Stats) Update(partition string, duration time.Duration, total, errors int) {
	s.pstatsMu.Lock()
	defer s.pstatsMu.Unlock()
	if _, ok := s.pstats[partition]; !ok {
		s.pstats[partition] = &pstat{
			throughput: metric.NewMovingAverage(),
			successes:  metric.NewMovingAverage(),
			errors:     metric.NewMovingAverage(),
		}
	}
	ps := s.pstats[partition]
	ps.throughput.Add(float64(total) / float64(duration.Milliseconds()))
	ps.successes.Add(float64(total - errors))
	ps.errors.Add(float64(errors))
}

// Score returns a score for the given partition. The score is a number between 0 and 100. Scores are calculated
// comparatively to other partitions, so that the partition with the highest throughput and the lowest error ratio
// will receive the highest score.
func (s *Stats) Score(partition string) int {
	all := s.All()
	if len(all) == 0 {
		return 100
	}
	return all[partition].Score
}

type PartitionStats struct {
	Partition  string
	Throughput float64
	Successes  float64
	Errors     float64

	NormalizedThroughput float64
	Score                int
}

// All returns a map containing stats for all available partitions
func (s *Stats) All() map[string]PartitionStats {
	s.pstatsMu.RLock()
	pstats := make([]PartitionStats, len(s.pstats))
	var i int
	for p, ps := range s.pstats {
		pt := PartitionStats{
			Partition:  p,
			Throughput: ps.throughput.Value(),
			Successes:  ps.successes.Value(),
			Errors:     ps.errors.Value(),
		}
		errorRatio := math.MaxFloat64
		if pt.Errors == 0 {
			errorRatio = 0
		} else if pt.Successes > 0 {
			errorRatio = pt.Errors / pt.Successes
		}

		// error ratio decreases throughput
		errorRatioMultiplier := math.Max(0, 2-errorRatio) / 2
		pt.NormalizedThroughput = pt.Throughput * errorRatioMultiplier
		pstats[i] = pt
		i++
	}
	s.pstatsMu.RUnlock()

	// highest throughput wins
	slices.SortFunc(pstats, func(first, second PartitionStats) int {
		a, b := first.NormalizedThroughput, second.NormalizedThroughput
		if a < b {
			return -1
		}
		if a > b {
			return +1
		}
		return 0
	})

	for i := range pstats {
		rank := float64(i+1) / float64(len(pstats))
		pstats[i].Score = int(100 * rank)
	}
	return lo.Associate(pstats, func(pt PartitionStats) (string, PartitionStats) {
		return pt.Partition, pt
	})
}

type pstat struct {
	throughput metric.MovingAverage // successful events per millisecond
	successes  metric.MovingAverage // number of successful events
	errors     metric.MovingAverage // number of errors
}
