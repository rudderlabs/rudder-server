package listcache

import (
	"sync"
	"time"

	"github.com/rudderlabs/rudder-server/config"
)

type cacheItem[E any] struct {
	data       []E
	lastAccess time.Time
}

/*
loadCacheConfig sets the properties of the cache after reading it from the config file.
This gives a feature of hot readability as well.
*/
func (c *ListCache[E]) loadCacheConfig() {
	if c.size == 0 {
		config.RegisterIntConfigVariable(3, &c.size, true, 1, "LiveEvent.cache.size")
	}
	if c.keyTTL == 0 {
		config.RegisterDurationConfigVariable(3, &c.keyTTL, true, time.Hour, "LiveEvent.cache.ttl") // default keyTTL is 30 days
	}
	if c.cleanupFreq == 0 {
		config.RegisterDurationConfigVariable(15, &c.cleanupFreq, true, time.Second, "LiveEvent.cache.clearFreq") // default clearFreq is 15 seconds
	}
}

/*
	Cache is an in-memory cache. Each key-value pair stored in this cache have a TTL and one goroutine removes the

key-value pair form the cache which is older than TTL time.
*/
type ListCache[E any] struct {
	lock        sync.RWMutex
	keyTTL      time.Duration // Time after which the data will be expired and removed from the cache
	cleanupFreq time.Duration // This is the time at which a cleaner goroutines  checks whether the data is expired in cache
	size        int           // This is the size upto which this cache can store a value corresponding to any key
	cacheMap    map[string]*cacheItem[E]
	once        sync.Once
	Origin      string
}

/*
This method initiates the cache object. To initiate, this sets certain properties of the cache like keyTTL,
cleanupFreq, size, empty cacheMap
*/
func (c *ListCache[E]) init() {
	c.once.Do(func() {
		c.loadCacheConfig()
		c.cacheMap = make(map[string]*cacheItem[E], c.size)

		go func() {
			for {
				time.Sleep(c.cleanupFreq)
				now := time.Now()
				expThreshold := now.Add(-c.keyTTL)
				c.lock.Lock()
				for k, v := range c.cacheMap {
					if v.lastAccess.Before(expThreshold) {
						delete(c.cacheMap, k)
					}
				}
				c.lock.Unlock()
			}
		}()
	})
}

/*
Update Inserts the data in the cache, This method expects a string as a key and []byte as the data
*/
func (c *ListCache[E]) Update(key string, value E) {
	c.init()
	c.lock.Lock()
	defer c.lock.Unlock()

	if _, ok := c.cacheMap[key]; !ok {
		c.cacheMap[key] = &cacheItem[E]{data: make([]E, 0, c.size)}
	}
	tempCacheElement := c.cacheMap[key].data
	tempCacheElement = append(tempCacheElement, value)
	if len(tempCacheElement) > c.size {
		tempCacheElement = tempCacheElement[len(tempCacheElement)-c.size:]
	}
	c.cacheMap[key].data = tempCacheElement
	c.cacheMap[key].lastAccess = time.Now()
}

/*
	ReadAndPopData reads the data by taking a string key,

if there is any data available corresponding to the given key then it removes the data from the cache and returns it
in the form of []byte
*/
func (c *ListCache[E]) Read(key string) []E {
	c.init()
	var historicEventsDelivery []E
	c.lock.Lock()
	if deliveryStatus, ok := c.cacheMap[key]; ok {
		historicEventsDelivery = deliveryStatus.data
		delete(c.cacheMap, key)
	}
	c.lock.Unlock()
	return historicEventsDelivery
}

func (c *ListCache[E]) Stop() error {
	// NO-OP
	return nil
}
