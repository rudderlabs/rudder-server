package cache

import (
	"github.com/rudderlabs/rudder-server/services/debugger/cache/badgercache"
	"github.com/rudderlabs/rudder-server/services/debugger/cache/memcache"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

type CacheType int8

const (
	MemoryCacheType CacheType = iota
	BadgerCacheType
)

type Cache[T any] interface {
	Update(key string, value T) error
	Read(key string) []T
	Stop() error
}

func New[T any](ct CacheType, origin string, l logger.Logger) Cache[T] {
	switch ct {
	case BadgerCacheType:
		l.Info("Using badger cache")
		return &embeddedcache.EmbeddedCache[T]{Origin: origin, Logger: l}
	default:
		l.Info("Using in-memory cache")
		return &listcache.ListCache[T]{Origin: origin}
	}
}
