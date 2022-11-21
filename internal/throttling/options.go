package throttling

import "github.com/go-redis/redis/v8"

// Option is a functional option for the limiter, see With* functions for reference
type Option interface {
	apply(*Limiter)
}

type withOption struct{ setup func(*Limiter) }

func (w withOption) apply(c *Limiter) { w.setup(c) }

// WithGoRate allows to setup a limiter with golang.org/x/time/rate (supports returning tokens)
func WithGoRate() Option {
	return withOption{setup: func(l *Limiter) {
		l.useGoRate = true
	}}
}

// WithGCRA allows to use the GCRA algorithm
func WithGCRA() Option {
	return withOption{setup: func(l *Limiter) {
		l.useGCRA = true
	}}
}

// WithGCRABurst allows to use the GCRA algorithm with the specified burst
func WithGCRABurst(burst int64) Option {
	return withOption{setup: func(l *Limiter) {
		l.gcraBurst = burst
	}}
}

// WithRedisClient allows to setup a limiter for Distributed Throttling with Redis
func WithRedisClient(rc *redis.Client) Option {
	return withOption{setup: func(l *Limiter) {
		l.redisSpeaker = rc
	}}
}

// WithStatsCollector allows to setup a stats collector for the limiter
func WithStatsCollector(sc statsCollector) Option {
	return withOption{setup: func(l *Limiter) {
		l.statsCollector = sc
	}}
}
