package filemanager

import (
	"context"
	"time"

	"github.com/rudderlabs/rudder-go-kit/logger"
)

// baseManager is the base struct for all file managers.
type baseManager struct {
	logger         logger.Logger
	timeout        time.Duration
	defaultTimeout func() time.Duration
}

func (manager *baseManager) SetTimeout(timeout time.Duration) {
	manager.timeout = timeout
}

func (manager *baseManager) getTimeout() time.Duration {
	if manager.timeout > 0 {
		return manager.timeout
	}
	if manager.defaultTimeout != nil {
		return manager.defaultTimeout()
	}
	return defaultTimeout
}

// baseListSession is the base struct for all list sessions.
type baseListSession struct {
	ctx        context.Context
	startAfter string
	prefix     string
	maxItems   int64
}
