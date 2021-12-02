package debugger

import (
	"github.com/rudderlabs/rudder-server/config"
	"sync"
	"time"
)

func (cache *CacheT) loadCacheConfig() {
	config.RegisterIntConfigVariable(3, &cache.size, true, 1, "LiveEvent.Cache.Size")
	config.RegisterDurationConfigVariable(time.Duration(720), &cache.keyTTL, true, time.Hour,
		"LiveEvent.Cache.TTL")            // default KeyTTL 15 days
	config.RegisterDurationConfigVariable(time.Duration(15), &cache.cleanupFreq, true, time.Second,
		"LiveEvent.Cache.ClearFreq") // default KeyTTL 15 days
}

type cacheItem struct {
	data       [][]byte
	lastAccess time.Time
}

type CacheT struct {
	lock        sync.RWMutex
	keyTTL      time.Duration
	cleanupFreq time.Duration
	size        int
	cacheMap    map[string]*cacheItem
	once        sync.Once
}

func (cache *CacheT) init() {
	cache.once.Do(func() {
		cache.loadCacheConfig()
		cache.cacheMap = make(map[string]*cacheItem, cache.size)

		go func() {
			for {
				time.Sleep(cache.cleanupFreq)
				now := time.Now()
				expThreshold := now.Add(-cache.keyTTL)
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

func (cache *CacheT) Update(key string, value []byte) {
	cache.init()
	cache.lock.Lock()
	defer cache.lock.Unlock()

	if _, ok := cache.cacheMap[key]; !ok {
		cache.cacheMap[key] = &cacheItem{data: make([][]byte, 0, cache.size)}
	}
	tempCacheElement := cache.cacheMap[key].data
	tempCacheElement = append(tempCacheElement, value)
	if len(tempCacheElement) > cache.size {
		tempCacheElement = tempCacheElement[len(tempCacheElement)-cache.size:]
	}
	cache.cacheMap[key].data = tempCacheElement
	cache.cacheMap[key].lastAccess = time.Now()
}

func (cache *CacheT) ReadAndPopData(key string) [][]byte {
	cache.init()
	var historicEventsDelivery [][]byte
	cache.lock.Lock()
	if deliveryStatus, ok := cache.cacheMap[key]; ok {
		historicEventsDelivery = deliveryStatus.data
		delete(cache.cacheMap, key)
	}
	cache.lock.Unlock()
	return historicEventsDelivery
}
