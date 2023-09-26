package logger

import (
	"errors"
	"sync"

	"go.uber.org/zap/zapcore"

	"github.com/rudderlabs/rudder-go-kit/config"
)

// factoryConfig is the configuration for the logger
type factoryConfig struct {
	rootLevel        int                      // the level for the root logger
	enableNameInLog  bool                     // whether to include the logger name in the log message
	enableStackTrace *config.Reloadable[bool] // for fatal logs

	levelConfig      *syncMap[string, int] // preconfigured log levels for loggers
	levelConfigCache *syncMap[string, int] // cache of all calculated log levels for loggers

	// zap specific config
	clock zapcore.Clock
}

// SetLogLevel sets the log level for the given logger name
func (fc *factoryConfig) SetLogLevel(name, levelStr string) error {
	level, ok := levelMap[levelStr]
	if !ok {
		return errors.New("invalid level value : " + levelStr)
	}
	if name == "" {
		fc.rootLevel = level
	} else {
		fc.levelConfig.set(name, level)
	}
	fc.levelConfigCache = newSyncMap[string, int]()
	return nil
}

// getOrSetLogLevel returns the log level for the given logger name or sets it using the provided function if no level is set
func (fc *factoryConfig) getOrSetLogLevel(name string, parentLevelFunc func() int) int {
	if name == "" {
		return fc.rootLevel
	}

	if level, found := fc.levelConfigCache.get(name); found {
		return level
	}
	level := func() int { // either get the level from the config or use the parent's level
		if level, ok := fc.levelConfig.get(name); ok {
			return level
		}
		return parentLevelFunc()
	}()
	fc.levelConfigCache.set(name, level) // cache the level
	return level
}

// newSyncMap creates a new syncMap
func newSyncMap[K comparable, V any]() *syncMap[K, V] {
	return &syncMap[K, V]{m: map[K]V{}}
}

// syncMap is a thread safe map for getting and setting keys concurrently
type syncMap[K comparable, V any] struct {
	mu sync.RWMutex
	m  map[K]V
}

func (sm *syncMap[K, V]) get(key K) (V, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	v, ok := sm.m[key]
	return v, ok
}

func (sm *syncMap[K, V]) set(key K, value V) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.m[key] = value
}
