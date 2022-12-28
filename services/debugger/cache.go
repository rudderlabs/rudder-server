package debugger

import (
	"sync"
	"time"

	"github.com/rudderlabs/rudder-server/config"
)

/*
	loadCacheConfig sets the properties of the cache after reading it from the config file.

This gives a feature of hot readability as well.
*/
func (cache *Cache[E]) loadCacheConfig() {
	if cache.Size == 0 {
		config.RegisterIntConfigVariable(3, &cache.Size, true, 1, "LiveEvent.cache.size")
	}
	if cache.KeyTTL == 0 {
		config.RegisterDurationConfigVariable(3, &cache.KeyTTL, true, time.Hour, "LiveEvent.cache.ttl") // default keyTTL is 30 days
	}
	if cache.CleanupFreq == 0 {
		config.RegisterDurationConfigVariable(15, &cache.CleanupFreq, true, time.Second, "LiveEvent.cache.clearFreq") // default clearFreq is 15 seconds
	}
}

type cacheItem[E any] struct {
	data       []E
	lastAccess time.Time
}

/*
	Cache is an in-memory cache. Each key-value pair stored in this cache have a TTL and one goroutine removes the

key-value pair form the cache which is older than TTL time.
*/
type Cache[E any] struct {
	lock        sync.RWMutex
	KeyTTL      time.Duration // Time after which the data will be expired and removed from the cache
	CleanupFreq time.Duration // This is the time at which a cleaner goroutines  checks whether the data is expired in cache
	Size        int           // This is the size upto which this cache can store a value corresponding to any key
	cacheMap    map[string]*cacheItem[E]
	once        sync.Once
}

/*
This method initiates the cache object. To initiate, this sets certain properties of the cache like keyTTL,
cleanupFreq, size, empty cacheMap
*/
func (cache *Cache[E]) init() {
	cache.once.Do(func() {
		cache.loadCacheConfig()
		cache.cacheMap = make(map[string]*cacheItem[E], cache.Size)

		go func() {
			for {
				time.Sleep(cache.CleanupFreq)
				now := time.Now()
				expThreshold := now.Add(-cache.KeyTTL)
				cache.lock.Lock()
				for k, v := range cache.cacheMap {
					if v.lastAccess.Before(expThreshold) {
						delete(cache.cacheMap, k)
					}
				}
				cache.lock.Unlock()
			}
		}()
	})
}

/*
Update Inserts the data in the cache, This method expects a string as a key and []byte as the data
*/
func (cache *Cache[E]) Update(key string, value E) {
	cache.init()
	cache.lock.Lock()
	defer cache.lock.Unlock()

	if _, ok := cache.cacheMap[key]; !ok {
		cache.cacheMap[key] = &cacheItem[E]{data: make([]E, 0, cache.Size)}
	}
	tempCacheElement := cache.cacheMap[key].data
	tempCacheElement = append(tempCacheElement, value)
	if len(tempCacheElement) > cache.Size {
		tempCacheElement = tempCacheElement[len(tempCacheElement)-cache.Size:]
	}
	cache.cacheMap[key].data = tempCacheElement
	cache.cacheMap[key].lastAccess = time.Now()
}

/*
	ReadAndPopData reads the data by taking a string key,

if there is any data available corresponding to the given key then it removes the data from the cache and returns it
in the form of []byte
*/
func (cache *Cache[E]) ReadAndPopData(key string) []E {
	cache.init()
	var historicEventsDelivery []E
	cache.lock.Lock()
	if deliveryStatus, ok := cache.cacheMap[key]; ok {
		historicEventsDelivery = deliveryStatus.data
		delete(cache.cacheMap, key)
	}
	cache.lock.Unlock()
	return historicEventsDelivery
}
