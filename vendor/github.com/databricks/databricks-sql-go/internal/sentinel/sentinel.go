package sentinel

import (
	"context"
	"fmt"
	"time"

	"github.com/databricks/databricks-sql-go/logger"
	"github.com/pkg/errors"
)

const (
	DEFAULT_TIMEOUT  = 0 //no timeout
	DEFAULT_INTERVAL = 100 * time.Millisecond
)

type WatchStatus int

const (
	WatchSuccess WatchStatus = iota
	WatchErr
	WatchExecuting
	WatchTimeout
	WatchCanceled
)

func (s WatchStatus) String() string {
	switch s {
	case WatchSuccess:
		return "SUCCESS"
	case WatchErr:
		return "ERROR"
	case WatchExecuting:
		return "EXECUTING"
	case WatchCanceled:
		return "CANCELED"
	case WatchTimeout:
		return "TIMEOUT"
	}
	return "<UNSET>"
}

type Done func() bool

type Sentinel struct {
	StatusFn         func() (doneFn Done, statusResp any, err error)
	OnCancelFn       func() (onCancelFnResp any, err error)
	OnDoneFn         func(statusResp any) (onDoneFnResp any, err error)
	onCancelFnCalled bool
}

// Wait takes care of checking the status of something on a given interval, up to a timeout.
// The StatusFn check will continue until given Done function returns true or statusFn returns an error.
// Context cancellation is supported and in that case it will return WaitCanceled status.
func (s Sentinel) Watch(ctx context.Context, interval, timeout time.Duration) (WatchStatus, any, error) {
	if s.StatusFn == nil {
		s.StatusFn = func() (Done, any, error) { return func() bool { return true }, nil, nil }
	}
	if timeout == 0 {
		timeout = DEFAULT_TIMEOUT
	}
	if interval == 0 {
		interval = DEFAULT_INTERVAL
	}

	var timeoutTimerCh <-chan time.Time
	if timeout != 0 {
		timeoutTimer := time.NewTimer(timeout)
		timeoutTimerCh = timeoutTimer.C
		defer timeoutTimer.Stop()
	}

	intervalTimer := time.NewTimer(interval)
	defer intervalTimer.Stop()

	resCh := make(chan any, 1)
	errCh := make(chan error, 1)
	processor := func(statusResp any) {
		ret, err := s.OnDoneFn(statusResp)
		if err != nil {
			errCh <- err
		} else {
			resCh <- ret
		}
	}

	// If the watch times out or is cancelled this function
	// will stop the interval timer and call the cancel function
	// if necessary.
	timeoutOrCancel := func() {
		_ = intervalTimer.Stop()
		if s.OnCancelFn != nil && !s.onCancelFnCalled {
			s.onCancelFnCalled = true
			_, err := s.OnCancelFn()
			if err != nil {
				logger.Err(err).Msg("databricks: cancel failed")
			} else {
				logger.Debug().Msgf("databricks: cancel success")
			}
		}
	}

	for {
		select {
		case <-intervalTimer.C:
			done, statusResp, err := s.StatusFn()
			if err != nil {
				return WatchErr, statusResp, err
			}
			// resetting it here so statusFn is called again after interval time
			_ = intervalTimer.Reset(interval)
			if done() {
				intervalTimer.Stop()
				if s.OnDoneFn != nil {
					go processor(statusResp)
				} else {
					return WatchSuccess, statusResp, nil
				}
			}
		case err := <-errCh:
			return WatchErr, nil, err
		case res := <-resCh:
			return WatchSuccess, res, nil
		case <-ctx.Done():
			timeoutOrCancel()
			return WatchCanceled, nil, ctx.Err()
		case <-timeoutTimerCh:
			msg := fmt.Sprintf("wait timed out after %s", timeout.String())
			logger.Info().Msg(msg)
			timeoutOrCancel()
			err := errors.New(msg)
			return WatchTimeout, nil, err
		}
	}
}
