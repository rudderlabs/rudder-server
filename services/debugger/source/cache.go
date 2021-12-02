package sourcedebugger

import (
	"fmt"
	"github.com/rudderlabs/rudder-server/config"
	"sync"
	"time"
)

var (
	CacheTTL     time.Duration
	CacheMaxSize int
)

func loadCacheConfig() {
	config.RegisterIntConfigVariable(2, &CacheMaxSize, true, 1, "SourceDebugger.maxEventsCacheSize")
	config.RegisterDurationConfigVariable(720 * time.Hour, &CacheTTL, true, 1, "SourceDebugger.maxEventsCacheTTL") // default KeyTTL 15 days
}

type cacheItem struct {
	objs       []string
	lastAccess time.Time
}

type Cache struct {
	lock   sync.RWMutex
	Size   int
	KeyTTL time.Duration
	m      map[string]*cacheItem
	once   sync.Once
}

func (c *Cache) init() {
	c.once.Do(func() {
		c.Size = CacheMaxSize
		c.m = make(map[string]*cacheItem, c.Size)
		c.KeyTTL = CacheTTL

		go func() {
			for now := range time.Tick(time.Second) {
				expThreshold := now.Add(-c.KeyTTL)
				c.lock.Lock()
				for k, v := range c.m {
					if v.lastAccess.Before(expThreshold) {
						delete(c.m, k)
					}
				}
				c.lock.Unlock()
			}
		}()
	})
}

func (c *Cache) update(key string, value string) {
	c.init()
	c.lock.Lock()
	defer c.lock.Unlock()

	if _, ok := c.m[key]; !ok {
		c.m[key] = &cacheItem{objs: make([]string, 0, c.Size)}
	}
	tempCacheElement := c.m[key].objs
	tempCacheElement = append(tempCacheElement, value)
	if len(tempCacheElement) > c.Size {
		tempCacheElement = tempCacheElement[len(tempCacheElement)-c.Size:]
	}
	c.m[key].objs = tempCacheElement
	c.m[key].lastAccess = time.Now()
}

func (c *Cache) readAndPopData(key string) []string {
	c.init()
	var historicEventsDelivery []string
	c.lock.Lock()
	if deliveryStatus, ok := c.m[key]; ok {
		historicEventsDelivery = deliveryStatus.objs
		delete(c.m, key)
	}
	c.lock.Unlock()
	return historicEventsDelivery
}

func (c *Cache) readData(key string) []string {
	c.init()
	var historicEventsDelivery []string
	c.lock.Lock()
	if deliveryStatus, ok := c.m[key]; ok {
		historicEventsDelivery = deliveryStatus.objs
		c.m[key].lastAccess = time.Now()
	}
	c.lock.Unlock()
	return historicEventsDelivery
}

func (c *Cache) printCache() {
	c.init()
	for {
		time.Sleep(time.Second * 5)
		fmt.Println("*******************")
		for k, v := range c.m {
			fmt.Println("key: ", k)
			for _, e := range v.objs {
				fmt.Printf(e + " ")
			}
			fmt.Println("")
		}
	}
}
