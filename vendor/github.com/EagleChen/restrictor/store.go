package restrictor

import (
	"errors"
	"fmt"
	"time"

	"github.com/EagleChen/mapmutex"
	"github.com/garyburd/redigo/redis"
	"github.com/gogo/protobuf/proto"
	cache "github.com/patrickmn/go-cache"
)

var unlockScript = redis.NewScript(1, `
	if redis.call("get",KEYS[1]) == ARGV[1] then
		return redis.call("del",KEYS[1])
	else
		return 0
	end
`)

// Store is the interface for central storage
type Store interface {
	GetLimiter(key string) (*Limiter, time.Time, bool)
	// expireAfter is in seconds
	SetLimiter(key string, value *Limiter, expireAfter int) error

	TryLock(key, mark string) bool
	Unlock(key, mark string) error
}

// LocalCacheStore is an in-memory key:value store for used in single machine
type localCacheStore struct {
	cache cache.Cache
	m     *mapmutex.Mutex
}

// GetLimiter gets Limiter for key
func (l *localCacheStore) GetLimiter(key string) (*Limiter, time.Time, bool) {
	result, t, found := l.cache.GetWithExpiration(key)
	if found {
		return result.(*Limiter), t, true
	}

	return nil, t, false
}

// SetLimiter sets key, limiter pointer and expire duration
// use pointer as value for better performance
func (l *localCacheStore) SetLimiter(key string, value *Limiter, expireAfter int) error {
	l.cache.Set(key, value, time.Duration(expireAfter)*time.Second)
	return nil
}

// TryLock tries to get a lock for the key
// mark is useless for localcachestore
func (l *localCacheStore) TryLock(key, mark string) bool {
	return l.m.TryLock(key)
}

// Unlock release the lock
// mark is useless for localcachestore
func (l *localCacheStore) Unlock(key, mark string) error {
	l.m.Unlock(key)
	return nil
}

// redisStore is the store use 'redis' as backend
// this should be the central store for many backend api servers
type redisStore struct {
	conn redis.Conn
}

func (r *redisStore) GetLimiter(key string) (*Limiter, time.Time, bool) {
	now := time.Now()
	r.conn.Send("MULTI")
	r.conn.Send("GET", key)
	r.conn.Send("TTL", key)
	reply, err := redis.Values(r.conn.Do("EXEC"))
	if err != nil {
		return nil, now, false
	}

	r1, ok := reply[0].([]byte)
	if !ok {
		return nil, now, false
	}
	ttl, ok := reply[1].(int)
	if !ok {
		return nil, now, false
	}

	var l Limiter
	if err = proto.Unmarshal(r1, &l); err != nil {
		return nil, now, false
	}
	return &l, now.Add(time.Duration(ttl) * time.Second), true
}

func (r *redisStore) SetLimiter(key string, l *Limiter, expireAfter int) error {
	out, err := proto.Marshal(l)
	if err != nil {
		return err
	}

	reply, err := redis.String(r.conn.Do("SETEX", key, expireAfter, out))
	if err != nil {
		return err
	}
	if reply != "OK" {
		return fmt.Errorf("redis err reply: %s", reply)
	}
	return nil
}

// TryLock tries to get a lock for the key
func (r *redisStore) TryLock(key, mark string) bool {
	reply, err := redis.String(r.conn.Do("SET", key, mark,
		"NX", "EX", 3)) // expire after 3 seconds
	if err != nil {
		return false
	}
	if reply != "OK" {
		return false // fmt.Errorf("redis err reply: %s", reply)
	}
	return true

}

// Unlock release the lock
func (r *redisStore) Unlock(key, mark string) error {
	if res, err := redis.Int(unlockScript.Do(r.conn, key, mark)); err != nil {
		return err
	} else if res != 1 {
		return errors.New("Unlock failed, key or value incorrect")
	}

	// Success
	return nil
}

// NewMemoryStore creates an in-memory store
func NewMemoryStore() (Store, error) {
	c := cache.New(cache.DefaultExpiration, 10*time.Minute)
	l := localCacheStore{
		cache: *c,
		m:     mapmutex.NewMapMutex(),
	}
	return &l, nil
}

// NewRedisStore creates an store using redis
func NewRedisStore(rawurl string, options ...redis.DialOption) (Store, error) {
	c, err := redis.DialURL(rawurl, options...)
	if err != nil {
		return nil, err
	}

	return &redisStore{conn: c}, nil
}
