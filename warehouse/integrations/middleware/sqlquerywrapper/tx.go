package sqlquerywrapper

import (
	"context"
	"fmt"
	"time"
)

// txWithTimeout is a wrapper around sql.Tx that adds a timeout to Rollback.
type txWithTimeout struct {
	*Tx
	timeout time.Duration
}

func (tx *txWithTimeout) Rollback() error {
	ctx, cancel := context.WithTimeout(context.Background(), tx.timeout)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- tx.Tx.Rollback()
	}()

	select {
	case <-ctx.Done():
		return fmt.Errorf("rollback timed out after %s", tx.timeout)
	case err := <-errCh:
		if err != nil {
			return fmt.Errorf("rollback failed: %w", err)
		}
		return nil
	}
}
