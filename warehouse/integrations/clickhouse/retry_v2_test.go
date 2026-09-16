package clickhouse

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"net"
	"syscall"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v5"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
)

// TestWithBlockRetriesV2 covers which failures buy another attempt and how many
// there are. Getting this wrong is expensive in both directions: too eager and a
// block the server rejected is sent again and again, too shy and one dead socket
// out of the pool fails a load that had already inserted millions of rows.
func TestWithBlockRetriesV2(t *testing.T) {
	// The real policy waits a second between attempts. Options are applied in
	// order and the last of a kind wins, so passing a shorter backoff replaces
	// it without touching what is under test.
	fast := backoff.WithBackOff(backoff.NewConstantBackOff(time.Millisecond))

	newCH := func(maxRetriesPerBlock int) *ClickhouseV2 {
		conf := config.New()
		conf.Set("Warehouse.clickhouse.v2.maxRetriesPerBlock", maxRetriesPerBlock)
		return NewV2(conf, logger.NOP, stats.NOP)
	}

	// failThenSucceed returns a send that fails with err its first n times.
	failThenSucceed := func(n int, err error, attempts *int) func() error {
		return func() error {
			*attempts++
			if *attempts <= n {
				return err
			}
			return nil
		}
	}

	t.Run("a send that works is attempted once", func(t *testing.T) {
		var attempts int
		require.NoError(t, newCH(3).withBlockRetries(context.Background(), failThenSucceed(0, nil, &attempts), fast))
		require.Equal(t, 1, attempts)
	})

	t.Run("a connection failure is repeated until it lands", func(t *testing.T) {
		var attempts int
		err := newCH(3).withBlockRetries(context.Background(),
			failThenSucceed(2, driver.ErrBadConn, &attempts), fast)
		require.NoError(t, err)
		require.Equal(t, 3, attempts, "two failures then the send that worked")
	})

	t.Run("a rejected block is not repeated", func(t *testing.T) {
		// What the server says no to it will say no to again. Sending it once
		// more only widens the window for a partially loaded table.
		rejected := errors.New("code: 62, DB::Exception: Syntax error")

		var attempts int
		err := newCH(3).withBlockRetries(context.Background(),
			failThenSucceed(99, rejected, &attempts), fast)
		require.ErrorIs(t, err, rejected)
		require.Equal(t, 1, attempts)
	})

	t.Run("the budget is finite and the last failure is returned", func(t *testing.T) {
		var attempts int
		err := newCH(3).withBlockRetries(context.Background(),
			failThenSucceed(99, driver.ErrBadConn, &attempts), fast)
		require.ErrorIs(t, err, driver.ErrBadConn)
		require.Equal(t, 4, attempts, "the first send plus maxRetriesPerBlock")
	})

	t.Run("zero retries still sends once", func(t *testing.T) {
		var attempts int
		err := newCH(0).withBlockRetries(context.Background(),
			failThenSucceed(99, driver.ErrBadConn, &attempts), fast)
		require.ErrorIs(t, err, driver.ErrBadConn)
		require.Equal(t, 1, attempts)
	})

	// The driver hands these back wrapped, so the classifier has to unwrap
	// rather than compare. A miss here turns every transient failure into a
	// failed load, which is the bug this whole change exists to remove.
	for _, tc := range []struct {
		name string
		err  error
	}{
		{name: "bad connection", err: driver.ErrBadConn},
		{name: "broken pipe", err: syscall.EPIPE},
		{name: "connection reset", err: syscall.ECONNRESET},
		{name: "a net.OpError", err: &net.OpError{Op: "write", Err: syscall.ECONNRESET}},
	} {
		t.Run("wrapped "+tc.name+" is still retried", func(t *testing.T) {
			var attempts int
			err := newCH(2).withBlockRetries(context.Background(),
				failThenSucceed(1, fmt.Errorf("executing statement: %w", tc.err), &attempts), fast)
			require.NoError(t, err)
			require.Equal(t, 2, attempts)
		})
	}

	t.Run("a cancelled load stops retrying", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		var attempts int
		err := newCH(3).withBlockRetries(ctx,
			failThenSucceed(99, driver.ErrBadConn, &attempts), fast)
		require.ErrorIs(t, err, context.Canceled)
		require.Equal(t, 1, attempts, "the send in flight is not abandoned, but nothing follows it")
	})
}
