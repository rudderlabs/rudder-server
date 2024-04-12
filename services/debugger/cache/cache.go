package cache

import (
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-server/services/debugger/cache/internal/badger"
	"github.com/rudderlabs/rudder-server/services/debugger/cache/internal/memory"
)

type CacheType int8

const (
	MemoryCacheType CacheType = iota
	BadgerCacheType
)

type Cache[T any] interface {
	Update(key string, value T) error
	Read(key string) ([]T, error)
	Stop() error
}

func New[T any](ct CacheType, origin string, l logger.Logger) (Cache[T], error) {
	switch ct {
	case BadgerCacheType:
		l.Info("Using badger cache")
		return badger.New[T](origin, l, stats.Default)
	default:
		l.Info("Using in-memory cache")
		return memory.New[T]()
	}
}
