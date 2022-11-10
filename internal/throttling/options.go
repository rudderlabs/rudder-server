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

// WithGCRABurstAsRate allows to use the GCRA algorithm with burst as rate
func WithGCRABurstAsRate() Option {
	return withOption{setup: func(l *Limiter) {
		l.useGCRABurstAsRate = true
	}}
}

type keyToRedisSpeaker = string

// WithRedisClient allows to setup a limiter for Distributed Throttling with Redis
func WithRedisClient(rc *redis.Client, mappings ...keyToRedisSpeaker) Option {
	return withOption{setup: func(l *Limiter) {
		if len(mappings) == 0 {
			l.redisSpeaker = rc
			return
		}
		if l.redisKeyToClientMap == nil {
			l.redisKeyToClientMap = make(map[string]redisSpeaker)
		}
		for i := range mappings {
			l.redisKeyToClientMap[mappings[i]] = rc
		}
	}}
}

// WithStatsCollector allows to setup a stats collector for the limiter
func WithStatsCollector(sc statsCollector) Option {
	return withOption{setup: func(l *Limiter) {
		l.statsCollector = sc
	}}
}
