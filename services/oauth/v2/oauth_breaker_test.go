package v2

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/samber/lo"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"
)

func TestOauthBreaker(t *testing.T) {
	token1 := json.RawMessage(`{"access_token":"token1"}`)
	token2 := json.RawMessage(`{"access_token":"token2"}`)
	err1 := NewStatusCodeError(500, errors.New("error 1"))
	// var err2 = NewStatusCodeError(502, errors.New("error 2"))

	const refresh = "refresh"

	type attempt struct {
		delay  time.Duration
		method string // "fetch" or "refresh", default: fetch
		value  json.RawMessage
		err    StatusCodeError
	}

	type scenario struct {
		delegateResponses            []lo.Tuple2[json.RawMessage, StatusCodeError]
		attempts                     []attempt
		expectedErrorBreaksTripped   int // expected number of times error breaker was tripped
		expectedSuccessBreaksTripped int // expected number of times success breaker was tripped
	}

	runScenario := func(t *testing.T, s scenario) {
		// Create memstats store for tracking stats
		statsStore, err := memstats.New()
		require.NoError(t, err)

		mock := &mockOAuthHandler{
			responses: s.delegateResponses,
		}
		breaker := newOAuthBreaker(mock, OAuthBreakerOptions{
			ConsecutiveErrorsThreshold: 2,
			ErrorsTimeout:              500 * time.Millisecond,
			SuccessesThreshold:         2,
			SuccessesInterval:          1 * time.Minute,
			SuccessesTimeout:           500 * time.Millisecond,
			Stats:                      statsStore,
		})
		for i, a := range s.attempts {
			if a.delay > 0 {
				time.Sleep(a.delay)
			}
			var v json.RawMessage
			var err StatusCodeError

			switch a.method {
			case refresh:
				v, err = breaker.RefreshToken(&OAuthTokenParams{
					AccountID:   "account-id",
					WorkspaceID: "workspace-id",
					DestType:    "dest-type",
				}, nil)
			default:
				// default to fetch
				v, err = breaker.FetchToken(&OAuthTokenParams{
					AccountID:   "account-id",
					WorkspaceID: "workspace-id",
					DestType:    "dest-type",
				})
			}
			if err != nil {
				require.ErrorIs(t, err, a.err, "expected error for response %d", i+1)
			} else {
				require.Equal(t, string(a.value), string(v), "expected value for response %d", i+1)
			}
		}
		require.Equalf(t, len(s.delegateResponses), mock.calls, "expected number of calls")

		// Verify stats counters
		statsTags := stats.Tags{
			"id":          "account-id",
			"workspaceId": "workspace-id",
			"destType":    "dest-type",
		}
		errorBreakerCount := int(statsStore.Get("oauth_action_error_breaker_tripped", statsTags).LastValue())
		successBreakerCount := int(statsStore.Get("oauth_action_success_breaker_tripped", statsTags).LastValue())

		require.Equal(t, s.expectedErrorBreaksTripped, errorBreakerCount, "expected error breaker trips")
		require.Equal(t, s.expectedSuccessBreaksTripped, successBreakerCount, "expected success breaker trips")
	}

	t.Run("success with different token trips the success breaker", func(t *testing.T) {
		runScenario(t, scenario{
			delegateResponses: []lo.Tuple2[json.RawMessage, StatusCodeError]{
				{A: token1, B: nil},
				{A: token2, B: nil},
			},
			attempts: []attempt{
				{value: token1},
				{value: token2},
				{value: token2, method: refresh}, // common breaker for fetch and refresh
			},
			expectedErrorBreaksTripped:   0,
			expectedSuccessBreaksTripped: 1,
		})
	})

	t.Run("success with same token doesn't trip the success breaker", func(t *testing.T) {
		runScenario(t, scenario{
			delegateResponses: []lo.Tuple2[json.RawMessage, StatusCodeError]{
				{A: token1, B: nil},
				{A: token1, B: nil},
				{A: token1, B: nil},
			},
			attempts: []attempt{
				{value: token1},
				{value: token1},
				{value: token1},
			},
			expectedErrorBreaksTripped:   0,
			expectedSuccessBreaksTripped: 0,
		})
	})

	t.Run("success with mixed different and same token trip the success breaker based on different token count", func(t *testing.T) {
		runScenario(t, scenario{
			delegateResponses: []lo.Tuple2[json.RawMessage, StatusCodeError]{
				{A: token1, B: nil},
				{A: token1, B: nil},
				{A: token2, B: nil},
			},
			attempts: []attempt{
				{value: token1},
				{value: token1},
				{value: token2, method: refresh}, // breaker is open after this
				{value: token2},
			},
			expectedErrorBreaksTripped:   0,
			expectedSuccessBreaksTripped: 1,
		})
	})

	t.Run("intermittent failures don't affect success breaker", func(t *testing.T) {
		runScenario(t, scenario{
			delegateResponses: []lo.Tuple2[json.RawMessage, StatusCodeError]{
				{A: token1, B: nil},
				{A: nil, B: err1},
				{A: token1, B: nil},
				{A: nil, B: err1},
				{A: token2, B: nil},
			},
			attempts: []attempt{
				{value: token1},
				{err: err1}, // failure, but doesn't reset success breaker
				{value: token1},
				{err: err1},                      // failure, but doesn't reset success breaker
				{value: token2, method: refresh}, // breaker is open after this
				{value: token2},
			},
			expectedErrorBreaksTripped:   0,
			expectedSuccessBreaksTripped: 1,
		})
	})

	t.Run("tripped success breaker recovers after timeout", func(t *testing.T) {
		runScenario(t, scenario{
			delegateResponses: []lo.Tuple2[json.RawMessage, StatusCodeError]{
				{A: token1, B: nil}, {A: token2, B: nil}, {A: token1, B: nil},
			},
			attempts: []attempt{
				{value: token1},
				{value: token2},                     // breaker is open after this
				{value: token2},                     // should return last value
				{value: token2, method: refresh},    // should return last value
				{delay: time.Second, value: token1}, // half-open, should call delegate
			},
			expectedErrorBreaksTripped:   0,
			expectedSuccessBreaksTripped: 2,
		})
	})

	t.Run("consecutive errors trip the error breaker", func(t *testing.T) {
		runScenario(t, scenario{
			delegateResponses: []lo.Tuple2[json.RawMessage, StatusCodeError]{
				{A: nil, B: err1},
				{A: nil, B: err1},
				{A: token1, B: nil},
			},
			attempts: []attempt{
				{err: err1},
				{err: err1},                  // breaker is open after this
				{err: err1},                  // should return last error
				{err: err1, method: refresh}, // should return last error
				{delay: time.Second, value: token1},
			},
			expectedErrorBreaksTripped:   2,
			expectedSuccessBreaksTripped: 0,
		})
	})

	t.Run("intermittent successes reset error breaker", func(t *testing.T) {
		runScenario(t, scenario{
			delegateResponses: []lo.Tuple2[json.RawMessage, StatusCodeError]{
				{A: nil, B: err1},
				{A: token1, B: nil},
				{A: nil, B: err1},
				{A: token1, B: nil},
				{A: nil, B: err1},
			},
			attempts: []attempt{
				{err: err1},
				{value: token1}, // success, resets error breaker
				{err: err1},
				{value: token1}, // success, resets error breaker
				{err: err1},
			},
			expectedErrorBreaksTripped:   0,
			expectedSuccessBreaksTripped: 0,
		})
	})

	t.Run("OAuthBreakerOptions defaults", func(t *testing.T) {
		var opts OAuthBreakerOptions
		opts.applyDefaults()
		require.Equal(t, 5, opts.ConsecutiveErrorsThreshold)
		require.Equal(t, 5*time.Minute, opts.ErrorsTimeout)
		require.Equal(t, 5, opts.SuccessesThreshold)
		require.Equal(t, 1*time.Minute, opts.SuccessesInterval)
		require.Equal(t, 1*time.Minute, opts.SuccessesTimeout)
	})
}

type mockOAuthHandler struct {
	OAuthHandler
	calls     int
	responses []lo.Tuple2[json.RawMessage, StatusCodeError]
}

func (m *mockOAuthHandler) FetchToken(*OAuthTokenParams) (json.RawMessage, StatusCodeError) {
	return m.call()
}

func (m *mockOAuthHandler) RefreshToken(*OAuthTokenParams, json.RawMessage) (json.RawMessage, StatusCodeError) {
	return m.call()
}

func (m *mockOAuthHandler) call() (json.RawMessage, StatusCodeError) {
	m.calls++
	if len(m.responses) < m.calls-1 {
		panic("not enough responses")
	}
	return m.responses[m.calls-1].A, m.responses[m.calls-1].B
}
