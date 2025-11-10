package v2

import (
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/sony/gobreaker"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/stats"
)

// OAuthBreakerOptions contains configuration options for the OAuth breaker.
// An OAuth breaker is used to prevent making too many requests to an OAuth provider
// by breaking the circuit when there are too many consecutive errors or too many
// successful (new) token generations within a certain interval.
//
// Different accounts have different breakers.
type OAuthBreakerOptions struct {
	// number of consecutive errors to trip the error breaker
	ConsecutiveErrorsThreshold int

	// Time duration that needs to elapse to switch an open error breaker to half-open state
	ErrorsTimeout time.Duration

	// number of successful (new) token generations within the interval to trip the success breaker
	SuccessesThreshold int

	// Time duration that needs to elapse to reset the success counter
	SuccessesInterval time.Duration

	// Time duration that needs to elapse to switch an open success breaker to half-open state
	SuccessesTimeout time.Duration

	Stats stats.Stats // stats instance to use for recording breaker metrics
}

// applyDefaults sets default values for any zero-value fields in the options.
func (o *OAuthBreakerOptions) applyDefaults() {
	if o.ConsecutiveErrorsThreshold <= 0 {
		o.ConsecutiveErrorsThreshold = 5
	}
	if o.ErrorsTimeout <= 0 {
		o.ErrorsTimeout = 5 * time.Minute
	}
	if o.SuccessesThreshold <= 0 {
		o.SuccessesThreshold = 5
	}
	if o.SuccessesInterval <= 0 {
		o.SuccessesInterval = 1 * time.Minute
	}
	if o.SuccessesTimeout <= 0 {
		o.SuccessesTimeout = 1 * time.Minute
	}
	if o.Stats == nil {
		o.Stats = stats.Default
	}
}

// newOAuthBreaker creates a new OAuthHandler that wraps the given delegate handler
func newOAuthBreaker(delegate OAuthHandler, opts OAuthBreakerOptions) OAuthHandler {
	opts.applyDefaults()
	return &oauthBreaker{
		delegate:        delegate,
		accountBreakers: newAccountBreakers(opts),
	}
}

type oauthBreaker struct {
	delegate        OAuthHandler
	accountBreakers *accountBreakers
}

func (b *oauthBreaker) FetchToken(params *OAuthTokenParams) (json.RawMessage, StatusCodeError) {
	return b.accountBreakers.get(params).exec(func() (json.RawMessage, StatusCodeError) {
		return b.delegate.FetchToken(params)
	})
}

func (b *oauthBreaker) RefreshToken(params *OAuthTokenParams, previousSecret json.RawMessage) (json.RawMessage, StatusCodeError) {
	return b.accountBreakers.get(params).exec(func() (json.RawMessage, StatusCodeError) {
		return b.delegate.RefreshToken(params, previousSecret)
	})
}

// accountBreakers manages circuit breakers for different accounts.
type accountBreakers struct {
	breakersMu sync.RWMutex
	breakers   map[string]*accountBreaker
	opts       OAuthBreakerOptions
}

// newAccountBreakers creates a new accountBreakers instance.
func newAccountBreakers(opts OAuthBreakerOptions) *accountBreakers {
	return &accountBreakers{
		breakers: make(map[string]*accountBreaker),
		opts:     opts,
	}
}

// get returns the accountBreaker for the given account ID, creating it if it doesn't exist.
func (b *accountBreakers) get(params *OAuthTokenParams) *accountBreaker {
	b.breakersMu.RLock()

	if brk, ok := b.breakers[params.AccountID]; ok {
		b.breakersMu.RUnlock()
		return brk
	}

	b.breakersMu.RUnlock()
	b.breakersMu.Lock()
	defer b.breakersMu.Unlock()

	brk, ok := b.breakers[params.AccountID]
	if !ok {
		brk = &accountBreaker{
			id: params.AccountID,
			errorBreaker: gobreaker.NewCircuitBreaker(gobreaker.Settings{
				Name: params.AccountID + "-error",
				ReadyToTrip: func(counts gobreaker.Counts) bool {
					// trip the breaker if there are more than consecutiveErrorsThreshold consecutive failures
					return int(counts.ConsecutiveFailures) >= b.opts.ConsecutiveErrorsThreshold
				},
				Timeout: b.opts.ErrorsTimeout,
			}),
			errorBreakerCounter: b.opts.Stats.NewTaggedStat("oauth_action_error_breaker_tripped", stats.CountType, stats.Tags{
				"id":          params.AccountID,
				"workspaceId": params.WorkspaceID,
				"destType":    params.DestType,
			}),
			successBreaker: gobreaker.NewCircuitBreaker(gobreaker.Settings{
				Name: params.AccountID + "-success",
				ReadyToTrip: func(counts gobreaker.Counts) bool {
					// trip the breaker if there are more than successesThreshold successes (new tokens generated) within the interval
					return int(counts.TotalFailures) >= b.opts.SuccessesThreshold
				},
				Interval: b.opts.SuccessesInterval,
				Timeout:  b.opts.SuccessesTimeout,
			}),
			successBreakerCounter: b.opts.Stats.NewTaggedStat("oauth_action_success_breaker_tripped", stats.CountType, stats.Tags{
				"id":          params.AccountID,
				"workspaceId": params.WorkspaceID,
				"destType":    params.DestType,
			}),
		}
		b.breakers[params.AccountID] = brk
	}
	return brk
}

type accountBreaker struct {
	id                    string                    // the account ID
	errorBreaker          *gobreaker.CircuitBreaker // the error circuit breaker to stop making requests on repeated errors
	errorBreakerCounter   stats.Counter             // counter capturing error breker being tripped
	successBreaker        *gobreaker.CircuitBreaker // the success circuit breaker to stop making requests on repeated successes (new tokens generated)
	successBreakerCounter stats.Counter             // counter capturing success breker being tripped

	mu        sync.RWMutex
	lastValue json.RawMessage // the last successful token value
	lastError StatusCodeError // the last error encountered
}

// exec executes the given function within the context of the account's circuit breakers.
func (b *accountBreaker) exec(fn func() (json.RawMessage, StatusCodeError)) (json.RawMessage, StatusCodeError) {
	return b.withErrBreaker(func() (json.RawMessage, StatusCodeError) {
		return b.withSuccessBreaker(fn)
	})
}

// withErrBreaker executes the given function within the context of the error circuit breaker.
func (b *accountBreaker) withErrBreaker(fn func() (json.RawMessage, StatusCodeError)) (json.RawMessage, StatusCodeError) {
	var result json.RawMessage
	var err StatusCodeError
	// error breaker
	if _, ebErr := b.errorBreaker.Execute(func() (any, error) {
		// nested success breaker
		result, err = fn()
		if err != nil {
			b.mu.Lock()
			b.lastError = err
			b.mu.Unlock()
		}
		return nil, err
	}); ebErr != nil && // need to return the last error if the breaker is open
		(errors.Is(ebErr, gobreaker.ErrOpenState) || errors.Is(ebErr, gobreaker.ErrTooManyRequests)) {
		b.mu.RLock()
		defer b.mu.RUnlock()
		b.errorBreakerCounter.Increment()
		return nil, b.lastError
	}
	return result, err
}

// withSuccessBreaker executes the given function within the context of the success circuit breaker.
func (b *accountBreaker) withSuccessBreaker(fn func() (json.RawMessage, StatusCodeError)) (json.RawMessage, StatusCodeError) {
	var result json.RawMessage
	var err StatusCodeError
	newTokenErr := errors.New("got different token")
	if _, sbErr := b.successBreaker.Execute(func() (any, error) {
		result, err = fn()
		b.mu.Lock()
		previousValue := b.lastValue
		if result != nil {
			b.lastValue = result
		}
		b.mu.Unlock()
		if err == nil && (previousValue == nil || string(previousValue) != string(result)) {
			return nil, newTokenErr
		}
		return result, nil
	}); sbErr != nil && // need to return the last value if the success breaker is open
		(errors.Is(sbErr, gobreaker.ErrOpenState) || errors.Is(sbErr, gobreaker.ErrTooManyRequests)) {
		b.mu.RLock()
		defer b.mu.RUnlock()
		b.successBreakerCounter.Increment()
		return b.lastValue, nil
	}
	return result, err
}

// ConfigToOauthBreakerOptions converts configuration variables to OAuthBreakerOptions. If breaker is disabled, it returns nil.
func ConfigToOauthBreakerOptions(prefix string, c *config.Config) *OAuthBreakerOptions {
	if enabled := c.GetBoolVar(true, prefix+".OauthBreaker.enabled", "OauthBreaker.enabled"); !enabled {
		return nil
	}
	configKeys := func(s string) []string {
		if prefix == "" {
			return []string{"OauthBreaker." + s}
		}
		return []string{prefix + ".OauthBreaker." + s, "OauthBreaker." + s}
	}
	return &OAuthBreakerOptions{
		ConsecutiveErrorsThreshold: c.GetIntVar(5, 1, configKeys("consecutiveErrorsThreshold")...),
		ErrorsTimeout:              c.GetDurationVar(5, time.Minute, configKeys("errorsTimeout")...),
		SuccessesThreshold:         c.GetIntVar(5, 1, configKeys("successesThreshold")...),
		SuccessesInterval:          c.GetDurationVar(1, time.Minute, configKeys("successesInterval")...),
		SuccessesTimeout:           c.GetDurationVar(1, time.Minute, configKeys("successesTimeout")...),
	}
}
