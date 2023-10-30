package controlplane

import (
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
)

type ConnectionManager struct {
	AuthInfo        AuthInfo
	RegisterService func(*grpc.Server)
	RetryInterval   time.Duration
	UseTLS          bool
	Logger          LoggerI
	mu              sync.Mutex
	active          bool
	url             string
	connHandler     *ConnHandler
	Options         []grpc.ServerOption
}

type LoggerI interface {
	Warn(a ...interface{})
	Warnf(format string, a ...interface{})
	Info(a ...interface{})
	Infof(format string, a ...interface{})
	Error(a ...interface{})
	Errorf(format string, a ...interface{})
}

const defaultRetryInterval time.Duration = time.Second

func (cm *ConnectionManager) Apply(url string, active bool) {
	if url == "" {
		return
	}
	cm.mu.Lock()
	defer cm.mu.Unlock()

	cm.url = url

	if active && !cm.active {
		cm.active = true
		cm.Logger.Infof(`Connection to CP Router not active. Establishing new connection`)
		go cm.maintainConnection()
	} else if !active && cm.active {
		cm.active = false
		cm.Logger.Infof(`Closing connection to CP Router`)
		_ = cm.closeConnection()
	}
}

func (cm *ConnectionManager) connect() error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	conn, err := cm.establishConnection()
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}
	cm.RegisterService(conn.GRPCServer)
	cm.connHandler = conn
	return nil
}

func (cm *ConnectionManager) maintainConnection() {
	for cm.active {
		if err := cm.connect(); err != nil {
			cm.Logger.Error(err.Error())
		} else {
			if err := cm.connHandler.ServeOnConnection(); err != nil {
				cm.Logger.Error(err.Error())
			}
		}

		time.Sleep(cm.retryInterval())
	}
}

func (cm *ConnectionManager) closeConnection() error {
	defer func() {
		cm.connHandler = nil
	}()

	if err := cm.connHandler.Close(); err != nil {
		return fmt.Errorf("failed to close grpc connection: %w", err)
	}

	return nil
}

func (cm *ConnectionManager) retryInterval() time.Duration {
	if cm.RetryInterval == 0 {
		return defaultRetryInterval
	}

	return cm.RetryInterval
}
