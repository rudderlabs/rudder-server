package v2

type Cache interface {
	// Get retrieves a value for the given key from the cache.
	// If the value doesn't exist, it returns (nil, false).
	Get(key string) (interface{}, bool)

	// Set stores a key-value pair in the cache.
	Set(key string, value interface{})

	// Delete removes a key-value pair from the cache.
	Delete(key string)
}

func NewCache() Cache {
	return &cache{
		tokenCache: make(map[string]*AuthResponse),
	}
}

type cache struct {
	tokenCache map[string]*AuthResponse
}

func (c *cache) Get(key string) (interface{}, bool) {
	value, ok := c.tokenCache[key]
	if !ok {
		return nil, false
	}
	return value, true
}

func (c *cache) Set(key string, value interface{}) {
	c.tokenCache[key] = value.(*AuthResponse)
}

func (c *cache) Delete(key string) {
	delete(c.tokenCache, key)
}
