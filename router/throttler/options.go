package throttler

import "github.com/rudderlabs/rudder-server/utils/logger"

// Option is a functional option for the Client, see With* functions for reference
type Option interface {
	apply(client *Client)
}

type withOption struct{ setup func(*Client) }

func (w withOption) apply(c *Client) { w.setup(c) }

// WithLogger allows to setup a logger for the Client
func WithLogger(logger logger.Logger) Option {
	return withOption{setup: func(c *Client) {
		c.logger = logger
	}}
}
