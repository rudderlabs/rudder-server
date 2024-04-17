package memory

import (
	"sync"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"

	"github.com/rudderlabs/rudder-server/rruntime"
)

type cacheItem[E any] struct {
	data       []E
	lastAccess time.Time
}

/*
loadCacheConfig sets the properties of the cache after reading it from the config file.
This gives a feature of hot readability as well.
*/
func (c *Cache[E]) loadCacheConfig() {
	if c.size == nil {
		c.size = config.GetReloadableIntVar(3, 1, "LiveEvent.cache.size")
	}
	if c.keyTTL == nil {
		c.keyTTL = config.GetReloadableDurationVar(3, time.Hour, "LiveEvent.cache.ttl") // default keyTTL is 30 days
	}
	if c.cleanupFreq == nil {
		c.cleanupFreq = config.GetReloadableDurationVar(15, time.Second, "LiveEvent.cache.clearFreq") // default clearFreq is 15 seconds
	}
}

/*
	Cache is an in-memory cache. Each key-value pair stored in this cache have a TTL and one goroutine removes the

key-value pair form the cache which is older than TTL time.
*/
type Cache[E any] struct {
	lock        sync.RWMutex
	keyTTL      config.ValueLoader[time.Duration] // Time after which the data will be expired and removed from the cache
	cleanupFreq config.ValueLoader[time.Duration] // This is the time at which a cleaner goroutines  checks whether the data is expired in cache
	size        config.ValueLoader[int]           // This is the size upto which this cache can store a value corresponding to any key
	cacheMap    map[string]*cacheItem[E]

	done   chan struct{}
	closed chan struct{}
}

/*
New method initiates the cache object. To initiate, this sets certain properties of the cache like keyTTL,
cleanupFreq, size, empty cacheMap
*/
func New[E any]() (*Cache[E], error) {
	c := Cache[E]{
		done:   make(chan struct{}),
		closed: make(chan struct{}),
	}

	c.loadCacheConfig()
	c.cacheMap = make(map[string]*cacheItem[E], c.size.Load())

	rruntime.Go(func() {
		for {
			select {
			case <-c.done:
				close(c.closed)
				return
			case <-time.After(c.cleanupFreq.Load()):
				now := time.Now()
				expThreshold := now.Add(-c.keyTTL.Load())
				c.lock.Lock()
				for k, v := range c.cacheMap {
					if v.lastAccess.Before(expThreshold) {
						delete(c.cacheMap, k)
					}
				}
				c.lock.Unlock()
			}
		}
	})

	return &c, nil
}

/*
Update Inserts the data in the cache, This method expects a string as a key and []byte as the data
*/
func (c *Cache[E]) Update(key string, value E) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	if _, ok := c.cacheMap[key]; !ok {
		c.cacheMap[key] = &cacheItem[E]{data: make([]E, 0, c.size.Load())}
	}
	tempCacheElement := c.cacheMap[key].data
	tempCacheElement = append(tempCacheElement, value)
	if len(tempCacheElement) > c.size.Load() {
		tempCacheElement = tempCacheElement[len(tempCacheElement)-c.size.Load():]
	}
	c.cacheMap[key].data = tempCacheElement
	c.cacheMap[key].lastAccess = time.Now()
	return nil
}

/*
	ReadAndPopData reads the data by taking a string key,

if there is any data available corresponding to the given key then it removes the data from the cache and returns it
in the form of []byte
*/
func (c *Cache[E]) Read(key string) ([]E, error) {
	var historicEventsDelivery []E
	c.lock.Lock()
	if deliveryStatus, ok := c.cacheMap[key]; ok {
		historicEventsDelivery = deliveryStatus.data
		delete(c.cacheMap, key)
	}
	c.lock.Unlock()
	return historicEventsDelivery, nil
}

func (c *Cache[E]) Stop() error {
	close(c.done)
	<-c.closed
	return nil
}
