package testhelper

import (
	"math"
	"sort"
	"time"

	"github.com/samber/lo"
)

// MaxDurationInWindow calculates the maximum time duration in non-overlapping windows.
func MaxDurationInWindow(durations []time.Duration, windowSize int) []time.Duration {
	if windowSize <= 0 {
		return nil
	}
	var maxDurations []time.Duration
	for i := 0; i < len(durations); i += windowSize {
		maxValue := durations[i]
		for j := 1; j < windowSize && i+j < len(durations); j++ {
			maxValue = max(maxValue, durations[i+j])
		}
		maxDurations = append(maxDurations, maxValue)
	}
	return maxDurations
}

// PercentileDurationInWindow calculates the specified percentile (e.g., 90th percentile) of time durations in non-overlapping windows.
func PercentileDurationInWindow(durations []time.Duration, windowSize int, percentile float64) []time.Duration {
	if windowSize <= 0 || percentile < 0 || percentile > 1 {
		return nil
	}

	var percentileDurations []time.Duration
	for i := 0; i < len(durations); i += windowSize {
		end := min(i+windowSize, len(durations))
		window := append([]time.Duration{}, durations[i:end]...)

		sort.Slice(window, func(a, b int) bool {
			return window[a] < window[b]
		})

		if len(window) > 0 {
			idx := percentile * float64(len(window)-1)
			lowerIdx := int(math.Floor(idx))
			upperIdx := int(math.Ceil(idx))

			if lowerIdx == upperIdx {
				percentileDurations = append(percentileDurations, window[lowerIdx])
			} else {
				frac := idx - float64(lowerIdx)
				interpolated := window[lowerIdx] + time.Duration(frac*float64(window[upperIdx]-window[lowerIdx]))
				percentileDurations = append(percentileDurations, interpolated)
			}
		}
	}
	return percentileDurations
}

// AverageDurationInWindow computes the average duration in non-overlapping windows of the given size.
func AverageDurationInWindow(durations []time.Duration, windowSize int) []time.Duration {
	if windowSize <= 0 {
		return nil
	}

	var avgDurations []time.Duration
	for i := 0; i < len(durations); i += windowSize {
		end := min(i+windowSize, len(durations))
		sum := lo.Sum(durations[i:end])
		count := end - i

		if count > 0 {
			avgDurations = append(avgDurations, sum/time.Duration(count))
		}
	}
	return avgDurations
}
