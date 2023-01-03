package cache

//go:generate mockgen -destination=../../../mocks/services/debugger/cache/mock_cache.go -package=mocks_cache github.com/rudderlabs/rudder-server/services/debugger/cache CacheAny

import (
	"fmt"
	"github.com/rudderlabs/rudder-server/services/debugger/cache/embeddedcache"
	"github.com/rudderlabs/rudder-server/services/debugger/cache/listcache"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

type cacheType int8

const (
	MemoryCacheType cacheType = iota
	BadgerCacheType
)

type Cache[T any] interface {
	Update(key string, value T)
	ReadAndPopData(key string) []T
}

func New[T any](ct cacheType, l logger.Logger) (Cache[T], error) {
	switch ct {
	case MemoryCacheType:
		l.Info("Using in-memory cache")
		return &listcache.ListCache[T]{}, nil
	case BadgerCacheType:
		l.Info("Using badger cache")
		return &embeddedcache.EmbeddedCache[T]{}, nil
	default:
		return nil, fmt.Errorf("cache type %v out of the known domain", ct)
	}
}
