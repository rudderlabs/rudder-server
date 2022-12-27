package cache

//go:generate mockgen -destination=../mocks/cache/mock_cache.go -package=mocks_cache github.com/rudderlabs/rudder-server/cache CacheAny

import "github.com/rudderlabs/rudder-server/utils/logger"

var (
	pkgLogger logger.Logger
)

type CacheAny interface {
	Cache[any]
}

type Cache[E any] interface {
	Update(key string, value E)
	ReadAndPopData(key string) []E
}

type Impl[E any] struct{}

type FactoryImpl[E any] struct{}

type Factory[E any] interface {
	New(cacheType string) Cache[E]
}

func init() {
	pkgLogger = logger.NewLogger().Child("cache")
}

func (FactoryImpl[E]) New(cacheType string) Cache[E] {
	switch cacheType {
	case "memory":
		pkgLogger.Info("Using in-memory cache")
		return &ListCache[E]{}
	case "goqpue":
		pkgLogger.Info("Using goque cache")
		return &EmbeddedCache[E]{}
	default:
		pkgLogger.Info("Using default in-memory cache")
		return &ListCache[E]{}
	}
}
