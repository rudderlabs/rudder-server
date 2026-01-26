package partitionmigration_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"

	"github.com/tidwall/gjson"
)

type logger interface {
	Helper()
	Logf(format string, args ...any)
}

// newTestWebhook creates a test webhook server that records the total number of events received
// and counts out-of-order events based on [userId] and [eventIndex] fields.
func newTestWebhook(l logger) *testWebhook {
	wh := &testWebhook{
		outOfOrderEvents: make(map[string][]int),
		lastEventIndex:   make(map[string]int),
	}
	wh.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "failed to read body", http.StatusBadRequest)
			return
		}
		userID := gjson.GetBytes(body, "userId").String()
		if userID == "" {
			http.Error(w, "missing userId", http.StatusBadRequest)
			return
		}
		eventIndex := int(gjson.GetBytes(body, "eventIndex").Int())

		wh.mu.Lock()
		defer wh.mu.Unlock()
		lastIndex, exists := wh.lastEventIndex[userID]
		if !exists { // first event for this userID
			wh.totalEvents.Add(1)
			wh.lastEventIndex[userID] = eventIndex
		} else if eventIndex < lastIndex { // out of order event
			l.Logf("webhooktest: received out-of-order webhook request for user %q and eventIndex %d (lastIndex: %d)", userID, eventIndex, lastIndex)
			wh.totalEvents.Add(1)
			wh.outOfOrderCount.Add(1)
			wh.outOfOrderEvents[userID] = append(wh.outOfOrderEvents[userID], wh.lastEventIndex[userID], eventIndex)
		} else if eventIndex == lastIndex { // duplicate event (from router retry?)
			l.Logf("webhooktest: received duplicate webhook request for user %q and eventIndex %d", userID, eventIndex)
		} else {
			if eventIndex > lastIndex+1 {
				l.Logf("webhooktest: detected missing events for user %q: lastIndex=%d, currentIndex=%d", userID, lastIndex, eventIndex)
			}
			// in order event
			wh.lastEventIndex[userID] = eventIndex
			wh.totalEvents.Add(1)
		}
	}))
	return wh
}

type testWebhook struct {
	*httptest.Server
	totalEvents      atomic.Int64
	outOfOrderCount  atomic.Int64
	mu               sync.Mutex
	outOfOrderEvents map[string][]int
	lastEventIndex   map[string]int
}
