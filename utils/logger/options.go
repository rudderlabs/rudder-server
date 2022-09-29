package logger

import "go.uber.org/zap/zapcore"

// An Option configures a Factory.
type Option interface {
	apply(*Factory)
}

// optionFunc wraps a func so it satisfies the Option interface.
type optionFunc func(*Factory)

func (f optionFunc) apply(factory *Factory) {
	f(factory)
}

// WithClock specifies the clock used by the logger to determine the current
// time for logged entries. Defaults to the system clock with time.Now.
func WithClock(clock zapcore.Clock) Option {
	return optionFunc(func(factory *Factory) {
		factory.config.clock = clock
	})
}
