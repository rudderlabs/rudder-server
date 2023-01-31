package throttling

import "github.com/go-redis/redis/v8"

// Option is a functional option for the limiter, see With* functions for reference
type Option func(*Limiter)

// WithInMemoryGCRA allows to use the GCRA algorithm (in-memory) with the specified burst or with the
// burst as rate if the provided burst is <= 0
func WithInMemoryGCRA(burst int64) Option {
	return func(l *Limiter) {
		l.useGCRA = true
		if burst > 0 {
			l.gcraBurst = burst
		}
	}
}

// WithRedisGCRA allows to use the GCRA algorithm (Redis version) with the specified burst or with the
// burst as rate if the provided burst is <= 0
func WithRedisGCRA(rc *redis.Client, burst int64) Option {
	return func(l *Limiter) {
		l.useGCRA = true
		l.redisSpeaker = rc
		if burst > 0 {
			l.gcraBurst = burst
		}
	}
}

// WithRedisSortedSet allows to use the Redis SortedSet algorithm for rate limiting
func WithRedisSortedSet(rc *redis.Client) Option {
	return func(l *Limiter) {
		l.useGCRA = false
		l.redisSpeaker = rc
	}
}

// WithStatsCollector allows to setup a stats collector for the limiter
func WithStatsCollector(sc statsCollector) Option {
	return func(l *Limiter) {
		l.statsCollector = sc
	}
}
