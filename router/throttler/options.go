package throttler

import "github.com/rudderlabs/rudder-server/utils/logger"

// Option is a functional option for the Client, see With* functions for reference
type Option func(client *Client)

// WithLogger allows to setup a logger for the Client
func WithLogger(logger logger.Logger) Option {
	return func(c *Client) {
		c.logger = logger
	}
}
