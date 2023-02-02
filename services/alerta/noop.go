package alerta

import (
	"context"
)

// NOOP alerta implementation that does nothing
type NOOP struct{}

func (*NOOP) SendAlert(ctx context.Context, resource string, opts SendAlertOpts) error {
	return nil
}
