package tunnel

import "context"

type Tunnel interface {
	Open(context.Context) error
	Close(context.Context) error
	LocalConnectionString() string // ip:port
}
