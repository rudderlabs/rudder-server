package cache

import (
	"github.com/rudderlabs/rudder-server/services/debugger/cache/embeddedcache"
	"github.com/rudderlabs/rudder-server/services/debugger/cache/listcache"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

type CacheType int8

const (
	MemoryCacheType CacheType = iota
	BadgerCacheType
)

type Cache[T any] interface {
	Update(key string, value T)
	Read(key string) []T
}

func New[T any](ct CacheType, origin string, l logger.Logger) Cache[T] {
	switch ct {
	case MemoryCacheType:
		l.Info("Using in-memory cache")
		return &listcache.ListCache[T]{Origin: origin}
	case BadgerCacheType:
		l.Info("Using badger cache")
		return &embeddedcache.EmbeddedCache[T]{Origin: origin, Logger: l}
	default:
		return &listcache.ListCache[T]{Origin: origin}
	}
}
