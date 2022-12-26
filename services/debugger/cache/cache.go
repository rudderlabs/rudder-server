package cache

import "github.com/rudderlabs/rudder-server/utils/logger"

var (
	pkgLogger logger.Logger
)

type Cache[E any] interface {
	Update(key string, value E)
	ReadAndPopData(key string) []E
}

type Impl[E any] struct{}

type FactoryImpl[E any] struct{}

type Factory[E any] interface {
	New(cacheType string) Impl[E]
}

func init() {
	DefaultFactoryImpl := &FactoryImpl[any]{}
	pkgLogger = logger.NewLogger().Child("cache")
}

func (*FactoryImpl[E]) New(cacheType string) Cache[E] {
	switch cacheType {
	case "memory":
		return &ListCache[E]{}
	//case "go-queue":
	//	return &GoQueueCache[E]{}
	default:
		return &ListCache[E]{}
	}
}
