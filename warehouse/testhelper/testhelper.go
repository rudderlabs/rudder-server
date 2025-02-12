package testhelper

import (
	"math"
	"sort"
	"time"
)

// MaxDurationInWindow calculates the maximum time duration in non-overlapping windows.
func MaxDurationInWindow(durations []time.Duration, windowSize int64) []time.Duration {
	if windowSize <= 0 {
		return nil
	}
	var maxDurations []time.Duration
	for i := 0; i < len(durations); i += int(windowSize) {
		maxValue := durations[i]
		for j := 1; j < int(windowSize) && i+j < len(durations); j++ {
			if durations[i+j] > maxValue {
				maxValue = durations[i+j]
			}
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
		end := i + windowSize
		if end > len(durations) {
			end = len(durations)
		}
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
		var sum time.Duration
		var count int

		end := i + windowSize
		if end > len(durations) {
			end = len(durations)
		}

		for _, d := range durations[i:end] {
			sum += d
			count++
		}

		if count > 0 {
			avgDurations = append(avgDurations, sum/time.Duration(count))
		}
	}
	return avgDurations
}
