package snowpipestreaming

import (
	"sync"
	"time"

	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

// hpState is the per-destination state of the high-performance path that must survive the manager being rebuilt:
// which channels are open, their continuation tokens and the offsets appended on them. Losing it while an import is in
// flight would make poll treat rows still pending as lost, failing them and reopening their channel for nothing.
type hpState struct {
	mu       sync.Mutex
	account  string                // the account the channels belong to
	channels map[string]*hpChannel // by table
}

// hpStates holds the state of every destination in this process.
type hpStates struct {
	mu            sync.Mutex
	byDestination map[string]*hpState // by destination ID
}

func newHPStates() *hpStates {
	return &hpStates{byDestination: make(map[string]*hpState)}
}

// processHPStates is the state of this process. A restart loses it, which poll handles by reopening channels.
var processHPStates = newHPStates()

func (s *hpStates) get(destinationID string) *hpState {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.byDestination[destinationID]
	if !ok {
		st = &hpState{channels: make(map[string]*hpChannel)}
		s.byDestination[destinationID] = st
	}
	return st
}

// cachedSchema is a table schema read from Snowflake.
type cachedSchema struct {
	schema    whutils.ModelTableSchema
	fetchedAt time.Time
}
