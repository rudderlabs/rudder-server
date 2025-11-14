package jobsdb

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
)

type cpvMockJobsdb struct {
	calls atomic.Int64
	JobsDB
}

func (j *cpvMockJobsdb) GetDistinctParameterValues(ctx context.Context, parameter ParameterName, customVal string) ([]string, error) {
	j.calls.Add(1)
	if customVal == "" {
		return []string{"value1", "value2"}, nil
	}
	return []string{"value3", "value4"}, nil
}

var testParameter parameterName = "test_parameter"

func TestCachingDistinctParameterValuesJobsdb(t *testing.T) {
	t.Run("single goroutine", func(t *testing.T) {
		// Create a mock JobsDB
		jobsdb := &cpvMockJobsdb{}

		cachingJobsdb := NewCachingDistinctParameterValuesJobsdb(
			config.SingleValueLoader(100*time.Millisecond),
			jobsdb,
		)
		ctx := context.Background()

		// First call should fetch from the mock JobsDB
		values, err := cachingJobsdb.GetDistinctParameterValues(ctx, testParameter, "")
		require.NoError(t, err)
		require.EqualValues(t, 1, jobsdb.calls.Load())
		require.Equal(t, []string{"value1", "value2"}, values)

		// Second call should hit the cache
		values, err = cachingJobsdb.GetDistinctParameterValues(ctx, testParameter, "")
		require.NoError(t, err)
		require.EqualValues(t, 1, jobsdb.calls.Load())
		require.Equal(t, []string{"value1", "value2"}, values)

		values, err = cachingJobsdb.GetDistinctParameterValues(ctx, testParameter, "someCustomVal")
		require.NoError(t, err)
		require.EqualValues(t, 2, jobsdb.calls.Load())
		require.Equal(t, []string{"value3", "value4"}, values)

		time.Sleep(100 * time.Millisecond)

		values, err = cachingJobsdb.GetDistinctParameterValues(ctx, testParameter, "")
		require.NoError(t, err)
		require.EqualValues(t, 3, jobsdb.calls.Load())
		require.Equal(t, []string{"value1", "value2"}, values)
	})

	t.Run("multiple goroutines and parameters", func(t *testing.T) {
		jobsdb := &cpvMockJobsdb{}
		ttl := 100 * time.Millisecond
		cachingJobsdb := NewCachingDistinctParameterValuesJobsdb(
			config.SingleValueLoader(ttl),
			jobsdb,
		)
		ctx := context.Background()

		var wg sync.WaitGroup

		// each goroutine will call GetDistinctParameterValues 4 times
		run := func(i int) {
			values, err := cachingJobsdb.GetDistinctParameterValues(ctx, parameterName("test_parameter_"+strconv.Itoa(i)), "")
			require.NoError(t, err)
			require.Equal(t, []string{"value1", "value2"}, values)
			values, err = cachingJobsdb.GetDistinctParameterValues(ctx, parameterName("test_parameter_"+strconv.Itoa(i)), "someCustomVal")
			require.NoError(t, err)
			require.Equal(t, []string{"value3", "value4"}, values)
			time.Sleep(2 * ttl)

			values, err = cachingJobsdb.GetDistinctParameterValues(ctx, parameterName("test_parameter_"+strconv.Itoa(i)), "")
			require.NoError(t, err)
			require.Equal(t, []string{"value1", "value2"}, values)
			values, err = cachingJobsdb.GetDistinctParameterValues(ctx, parameterName("test_parameter_"+strconv.Itoa(i)), "someCustomVal")
			require.NoError(t, err)
			require.Equal(t, []string{"value3", "value4"}, values)
		}
		iterations := 10
		for i := range iterations {
			wg.Go(func() {
				run(i)
			})
			wg.Go(func() {
				run(i)
			})
		}
		wg.Wait()
		require.EqualValues(t, 4*iterations, jobsdb.calls.Load())
	})
}
