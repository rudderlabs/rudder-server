package throttling

import (
	"strconv"
	"sync"
	"time"

	"github.com/wangjia184/sortedset"
)

// TODO add expiration mechanism, if we don't touch a key anymore it will stay in memory forever
type sortedSet struct {
	sets    map[string]*sortedset.SortedSet
	expires map[string]time.Time
	mu      sync.Mutex
}

func (s *sortedSet) limit(key string, cost, rate, period int64) (
	members []string, err error,
) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.sets == nil {
		s.sets = make(map[string]*sortedset.SortedSet)
		s.expires = make(map[string]time.Time)
	}

	now := time.Now()
	set, ok := s.sets[key]
	if !ok || now.After(s.expires[key]) {
		set = sortedset.New()
		s.sets[key] = set
	} else {
		for _, trim := range set.GetByScoreRange(0, sortedset.SCORE(now.Unix()), nil) {
			_ = set.Remove(trim.Key())
		}
	}

	if count := set.GetCount(); (cost + int64(count)) > rate { // limit reached
		return nil, nil
	}

	members = make([]string, cost)
	for i := int64(0); i <= cost; i++ {
		members[i] = strconv.FormatInt((now.UnixMicro()*10)+i, 10)
		set.AddOrUpdate(members[i], sortedset.SCORE(now.Unix()), members[i])
	}

	s.expires[key] = now.Add(time.Duration(period) * time.Second)

	return members, nil
}

func (s *sortedSet) remove(key string, members []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sets == nil {
		return
	}

	set, ok := s.sets[key]
	if !ok || set.GetCount() == 0 {
		return
	}
	if time.Now().After(s.expires[key]) {
		delete(s.sets, key)
		delete(s.expires, key)
		return
	}
	for _, trim := range members {
		set.Remove(trim)
	}
}
