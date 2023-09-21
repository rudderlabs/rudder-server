package subjectcache

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-server/internal/tokens"
)

// map[string]time.Time
//
// key: wildcard subject of format:
//
// dsIndex.WorkspaceID.CustomVal.SourceID.DestinationID.State
type cacheMap struct {
	sync.Map
}

type Cache interface {
	// call before querying jobsdb to check if the query is necessary
	Check(key string) bool

	// call with a subject literal after writing to jobsdb
	Remove(key string)

	// call before querying jobsdb to set a placeholder value in the cache
	SetPreRead(key string)

	// call after querying jobsdb(no jobs found) to commit the PreRead key to the cache
	CommitPreRead(key string)

	// call after querying jobsdb(jobs found) to remove the PreRead key from the cache
	RemovePreRead(key string)

	// String returns a string representation of the cache
	String() string

	// ClearDS clears the cache for a given dsIndex
	ClearDS(dsIndex string)
}

// New returns a new subjectCache
//
// Subject pattern used: dsIndex.WorkspaceID.CustomVal.SourceID.DestinationID.State
//
// each write must do *cache.Remove(subject literal) - no wildcards allowed
//
// reads may use wildcards as per query params
//
// existence of a superset of read subject implies existence of read subject
//
// do not query jobsdb in that case
//
// works because reads look for existence of a subject whose subset key may belong to
//
// and writes remove the key and any other subject whose subset key may belong to
func New() Cache {
	c := &cacheMap{}
	return c
}

// check if key belongs to the subset of any subject in the cache
func (c *cacheMap) Check(key string) bool {
	var subjectExists bool
	c.Range(func(k, _ any) bool {
		keySub, ok := k.(string)
		if !ok {
			panic(fmt.Sprintf("invalid key type: %v", k))
		}
		if tokens.SubjectIsSubsetMatch(key, keySub) {
			subjectExists = true
			return false
		}
		return true
	})
	return subjectExists
}

// removes the key and any other subject whose subset key may belong to
func (c *cacheMap) Remove(key string) {
	if key == "" {
		panic("invalid key - key empty: " + key)
	}
	// cannot contain wildCard
	if strings.Contains(key, "*") {
		panic("invalid key - wildcard: " + key)
	}
	// cannot have an empty token
	if strings.Contains(key, "..") {
		panic("invalid key - empty token: " + key)
	}
	// cannot have empty dsIndex
	if strings.HasPrefix(key, ".") {
		panic("invalid key - empty dsIndex: " + key)
	}
	// cannot have empty state
	if strings.HasSuffix(key, ".") {
		panic("invalid key - empty state: " + key)
	}
	c.Range(func(k, _ any) bool {
		keySub, ok := k.(string)
		if !ok {
			panic(fmt.Sprintf("invalid key type: %v", k))
		}
		if tokens.SubjectIsSubsetMatch(key, keySub) {
			c.Delete(k)
		}
		return true
	})
}

// sets a placeholder value in the cache
//
// depending on the result of the query, the value may be overwritten or deleted
//
// overwritten(CompareAndSwap) in case no jobs are found
//
// deleted in case jobs are found
func (c *cacheMap) SetPreRead(key string) {
	c.Store(key, time.Time{})
}

// CommitPreRead commits the PreRead key to the cache
//
// # Done when the query is complete, and no jobs are found
//
// if the key is not found, it is a no-op
func (c *cacheMap) CommitPreRead(key string) {
	_ = c.CompareAndSwap(key, time.Time{}, time.Now())
}

// RemovePreRead removes the PreRead key from the cache
//
// # Done when the query is complete, and jobs are found
//
// if the key is not found, it is a no-op
func (c *cacheMap) RemovePreRead(key string) {
	c.Delete(key)
}

// func (c *cacheMap) cleanupLoop() {
// 	go func() {
// 		for {
// 			time.Sleep(15 * time.Minute)
// 			c.Range(func(k any, val any) bool {
// 				t, ok := val.(time.Time)
// 				if !ok {
// 					panic("invalid value type")
// 				}
// 				if t.Add(GetDurationVar(
// 					120, time.Minute, []string{"JobsDB.cacheExpiration"}...,
// 				)).Before(time.Now()) {
// 					c.Delete(k)
// 				}
// 				return true
// 			})
// 		}
// 		ticker := time.NewTicker(5 * time.Minute)
// 		for range ticker.C {
// 			c.cleanup()
// 		}
// 	}()
// }

func (c *cacheMap) String() string {
	var s strings.Builder
	c.Range(func(k, _ any) bool {
		key, ok := k.(string)
		if !ok {
			panic(fmt.Sprintf("invalid key type: %v", k))
		}
		s.WriteString(key)
		s.WriteString("\n")
		return true
	})
	return s.String()
}

func (c *cacheMap) ClearDS(dsIndex string) {
	c.Range(func(k, _ any) bool {
		key, ok := k.(string)
		if !ok {
			panic(fmt.Sprintf("invalid key type: %v", k))
		}
		if strings.HasPrefix(key, dsIndex+".") {
			c.Delete(k)
		}
		return true
	})
}
