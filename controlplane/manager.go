package controlplane

import (
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/rudderlabs/rudder-go-kit/config"
	proto "github.com/rudderlabs/rudder-server/proto/common"
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

	Config                   *config.Config
	duplicateBackoffInterval time.Duration
}

type LoggerI interface {
	Warn(a ...interface{})
	Warnf(format string, a ...interface{})
	Info(a ...interface{})
	Infof(format string, a ...interface{})
	Error(a ...interface{})
	Errorf(format string, a ...interface{})
}

const (
	defaultRetryInterval            time.Duration = time.Second
	defaultDuplicateBackoffInterval time.Duration = 5 * time.Minute
)

// SetConfig sets the config and loads configuration values for the ConnectionManager
func (cm *ConnectionManager) SetConfig(conf *config.Config) {
	cm.Config = conf
	cm.loadConfigValues()
}

// loadConfigValues loads configuration values from config
func (cm *ConnectionManager) loadConfigValues() {
	if cm.Config != nil {
		cm.RetryInterval = cm.Config.GetDuration("CPRouter.retryInterval", 1, time.Second)
		cm.duplicateBackoffInterval = cm.Config.GetDuration("CPRouter.duplicateConnectionBackoff", 300, time.Second)
	} else {
		cm.RetryInterval = defaultRetryInterval
		cm.duplicateBackoffInterval = defaultDuplicateBackoffInterval
	}
}

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

		time.Sleep(cm.getRetryInterval())
		cm.resetRetryInterval()
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

func (cm *ConnectionManager) getRetryInterval() time.Duration {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.RetryInterval == 0 {
		return defaultRetryInterval
	}
	return cm.RetryInterval
}

func (cm *ConnectionManager) handleDuplicateConnectionNotification(req *proto.DuplicateConnectionRequest) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	cm.Logger.Warnf("Received duplicate connection notification. ConnectionID: %s, Service: %s, Existing: %s, Rejected: %s",
		req.ConnectionID, req.Service, req.ExistingInstanceID, req.RejectedInstanceID)

	// If this is our instance being rejected, we should back off longer
	if req.RejectedInstanceID == cm.AuthInfo.InstanceID {
		cm.Logger.Infof("This instance was rejected due to duplicate connection. Will use extended backoff.")
		// Set a flag or modify retry behavior
		cm.setDuplicateConnectionBackoff()
	}
}

func (cm *ConnectionManager) setDuplicateConnectionBackoff() {
	// Extend the retry interval for duplicate connections using configurable value
	cm.RetryInterval = cm.getDuplicateBackoffInterval()
}

func (cm *ConnectionManager) resetRetryInterval() {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	// Reset to configured default or reload from config
	cm.loadConfigValues()
}

func (cm *ConnectionManager) getDuplicateBackoffInterval() time.Duration {
	if cm.duplicateBackoffInterval == 0 {
		return defaultDuplicateBackoffInterval
	}
	return cm.duplicateBackoffInterval
}
