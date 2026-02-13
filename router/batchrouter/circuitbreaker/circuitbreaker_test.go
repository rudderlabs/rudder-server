package circuitbreaker_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/rudderlabs/rudder-server/router/batchrouter/circuitbreaker"
)

func TestCircuitBreaker_InitialState(t *testing.T) {
	cb := circuitbreaker.NewCircuitBreaker("test")
	assert.False(t, cb.IsOpen(), "Circuit breaker should be initially closed")
}

func TestCircuitBreaker_TripAfterFailures(t *testing.T) {
	consecutiveFailures := 3
	cb := circuitbreaker.NewCircuitBreaker("test", circuitbreaker.WithConsecutiveFailures(consecutiveFailures))

	assert.False(t, cb.IsOpen(), "Should be closed initially")

	// Record failures less than the threshold
	for i := 0; i < consecutiveFailures-1; i++ {
		cb.Failure()
		assert.False(t, cb.IsOpen(), "Should remain closed before reaching failure threshold")
	}

	// Record the final failure to trip the breaker
	cb.Failure()
	assert.True(t, cb.IsOpen(), "Should be open after reaching failure threshold")
}

func TestCircuitBreaker_ResetAfterSuccessInHalfOpen(t *testing.T) {
	consecutiveFailures := 2
	timeout := 100 * time.Millisecond // Short timeout for testing
	cb := circuitbreaker.NewCircuitBreaker("test",
		circuitbreaker.WithConsecutiveFailures(consecutiveFailures),
		circuitbreaker.WithTimeout(timeout),
		circuitbreaker.WithMaxRequests(1), // Allow one request in half-open
	)

	// Trip the breaker
	for range consecutiveFailures {
		cb.Failure()
	}
	assert.True(t, cb.IsOpen(), "Should be open after failures")

	// Wait for timeout to enter half-open state
	time.Sleep(timeout + 50*time.Millisecond)

	// Record a success in half-open state
	// Note: gobreaker might need an Allow() check before Success() can reset from half-open
	// The internal cb.Execute() handles the Allow() check implicitly
	cb.Success()
	assert.False(t, cb.IsOpen(), "Should be closed after success in half-open state")
}

func TestCircuitBreaker_ReTripAfterFailureInHalfOpen(t *testing.T) {
	consecutiveFailures := 2
	timeout := 100 * time.Millisecond // Short timeout for testing
	cb := circuitbreaker.NewCircuitBreaker("test",
		circuitbreaker.WithConsecutiveFailures(consecutiveFailures),
		circuitbreaker.WithTimeout(timeout),
		circuitbreaker.WithMaxRequests(1), // Allow one request in half-open
	)

	// Trip the breaker
	for range consecutiveFailures {
		cb.Failure()
	}
	assert.True(t, cb.IsOpen(), "Should be open after failures")

	// Wait for timeout to enter half-open state
	time.Sleep(timeout + 50*time.Millisecond)

	// Record a failure in half-open state
	// Note: gobreaker might need an Allow() check before Failure() can re-trip from half-open
	// The internal cb.Execute() handles the Allow() check implicitly
	cb.Failure()
	assert.True(t, cb.IsOpen(), "Should be open again after failure in half-open state")
}

func TestCircuitBreaker_SuccessResetsFailures(t *testing.T) {
	consecutiveFailures := 3
	cb := circuitbreaker.NewCircuitBreaker("test", circuitbreaker.WithConsecutiveFailures(consecutiveFailures))

	assert.False(t, cb.IsOpen(), "Should be closed initially")

	// Record some failures
	cb.Failure()
	cb.Failure()
	assert.False(t, cb.IsOpen(), "Should remain closed")

	// Record a success
	cb.Success()
	assert.False(t, cb.IsOpen(), "Should remain closed after success")

	// Record more failures - should not trip yet as counter was reset
	cb.Failure()
	cb.Failure()
	assert.False(t, cb.IsOpen(), "Should remain closed as failure count was reset")

	// Final failure to trip
	cb.Failure()
	assert.True(t, cb.IsOpen(), "Should be open after reaching threshold again")
}

func TestCircuitBreaker_DifferentConfigurations(t *testing.T) {
	// Test with different timeout
	timeout := 50 * time.Millisecond
	cbTimeout := circuitbreaker.NewCircuitBreaker("timeoutTest", circuitbreaker.WithTimeout(timeout), circuitbreaker.WithConsecutiveFailures(1))
	cbTimeout.Failure()
	assert.True(t, cbTimeout.IsOpen(), "Should be open")
	time.Sleep(timeout + 20*time.Millisecond)
	// After timeout, it should allow a request (go into half-open), success closes it.
	cbTimeout.Success()
	assert.False(t, cbTimeout.IsOpen(), "Should be closed after timeout and success")

	// Test with MaxRequests (influences half-open state behavior)
	maxRequests := 2
	cbMaxReq := circuitbreaker.NewCircuitBreaker("maxReqTest", circuitbreaker.WithMaxRequests(maxRequests), circuitbreaker.WithConsecutiveFailures(1), circuitbreaker.WithTimeout(timeout))
	cbMaxReq.Failure()
	assert.True(t, cbMaxReq.IsOpen(), "Should be open")
	time.Sleep(timeout + 20*time.Millisecond) // Enter Half-Open

	// In half-open, allow 'maxRequests' successes before closing fully
	cbMaxReq.Success()
	assert.False(t, cbMaxReq.IsOpen(), "Should be closed after 1st success in half-open (gobreaker behavior)")

	// Re-trip to test again
	cbMaxReq.Failure()
	assert.True(t, cbMaxReq.IsOpen(), "Should be open again")
	time.Sleep(timeout + 20*time.Millisecond) // Enter Half-Open again

	cbMaxReq.Success() // First success in half-open
	assert.False(t, cbMaxReq.IsOpen(), "Closed after first success")
	// According to gobreaker docs, MaxRequests limits requests allowed *through* Execute when HalfOpen.
	// A single success while HalfOpen transitions it back to Closed. MaxRequests > 1 allows more trials *if* the first ones fail.
	// Let's test failure in half-open
	cbMaxReq.Failure()                        // Trip again
	time.Sleep(timeout + 20*time.Millisecond) // Half-open
	cbMaxReq.Failure()                        // Fail in half-open
	assert.True(t, cbMaxReq.IsOpen(), "Should re-open after failure in half-open")
}
