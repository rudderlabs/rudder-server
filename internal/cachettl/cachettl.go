package cachettl

import (
	"sync"
	"time"
)

// Cache is a double linked list sorted by expiration time (ascending order)
// the root (head) node is the node with the lowest expiration time
// the tail node (end) is the node with the highest expiration time
// Cleanups are done on Get() calls so if Get() is never invoked then Nodes stay in-memory.
type Cache struct {
	root *node
	mu   sync.Mutex
	m    map[string]*node
	now  func() time.Time
}

type node struct {
	key        string
	value      interface{}
	prev       *node
	next       *node
	ttl        time.Duration
	expiration time.Time
}

func (n *node) remove() {
	n.prev.next = n.next
	n.next.prev = n.prev
}

// New returns a new Cache.
func New() *Cache {
	return &Cache{
		now:  time.Now,
		root: &node{},
		m:    make(map[string]*node),
	}
}

// Get returns the value associated with the key or nil otherwise.
// Additionally, Get() will refresh the TTL and cleanup expired nodes.
func (c *Cache) Get(key string) interface{} {
	c.mu.Lock()
	defer c.mu.Unlock()

	defer func() { // remove expired nodes
		cn := c.root.next // start from head since we're sorting by expiration with the highest expiration at the tail
		for cn != nil && cn != c.root {
			if c.now().After(cn.expiration) {
				cn.remove()         // removes a node from the linked list (leaves the map untouched)
				delete(c.m, cn.key) // remove node from map too
			} else { // there is nothing else to clean up, no need to iterate further
				break
			}
			cn = cn.next
		}
	}()

	if n, ok := c.m[key]; ok && n.expiration.After(c.now()) {
		n.remove()
		n.expiration = c.now().Add(n.ttl) // refresh TTL
		c.add(n)
		return n.value
	}
	return nil
}

// Put adds or updates an element inside the Cache.
// The Cache will be sorted with the node with the highest expiration at the tail.
func (c *Cache) Put(key string, value interface{}, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := c.now()

	n, ok := c.m[key]
	if !ok {
		n = &node{key: key, value: value, ttl: ttl, expiration: now.Add(ttl)}
		c.m[key] = n
	} else {
		n.value = value
		n.expiration = now.Add(ttl)
	}

	if c.root.next == nil { // first node insertion
		c.root.next = n
		c.root.prev = n
		n.prev = c.root
		n.next = c.root
		return
	}

	if ok { // removes a node from the linked list (leaves the map untouched)
		n.remove()
	}

	c.add(n)
}

func (c *Cache) add(n *node) {
	cn := c.root.prev // tail
	for cn != nil {   // iterate from tail to root because we have expiring nodes towards the tail
		if n.expiration.After(cn.expiration) || n.expiration.Equal(cn.expiration) {
			// insert node after cn
			save := cn.next
			cn.next = n
			n.prev = cn
			n.next = save
			save.prev = n
			break
		}
		cn = cn.prev
	}
}

// slice is used for debugging purposes only
func (c *Cache) slice() (s []interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()

	cn := c.root.next
	for cn != nil && cn != c.root {
		s = append(s, cn.value)
		cn = cn.next
	}
	return
}
