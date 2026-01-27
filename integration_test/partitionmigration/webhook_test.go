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
		outOfOrderEvents: make(map[string][]eventIndexAndInstanceID),
		lastEventIndex:   make(map[string]eventIndexAndInstanceID),
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
		instanceID := r.Header.Get("X-Rudder-Instance-ID")

		wh.mu.Lock()
		defer wh.mu.Unlock()
		last, exists := wh.lastEventIndex[userID]
		if !exists { // first event for this userID
			wh.totalEvents.Add(1)
			wh.lastEventIndex[userID] = eventIndexAndInstanceID{eventIndex: eventIndex, instanceID: instanceID}
		} else if eventIndex == last.eventIndex { // duplicate event (from router retry?)
			l.Logf("webhooktest: received duplicate webhook request for user %q and eventIndex %d [%s]", userID, eventIndex, instanceID)
		} else if eventIndex != last.eventIndex+1 { // out of order event
			l.Logf("webhooktest: received out-of-order webhook request for user %q and eventIndex %d [%s] (lastIndex: %d [%s])", userID, eventIndex, instanceID, last.eventIndex, last.instanceID)
			wh.outOfOrderCount.Add(1)
			wh.outOfOrderEvents[userID] = append(wh.outOfOrderEvents[userID], wh.lastEventIndex[userID], eventIndexAndInstanceID{eventIndex: eventIndex, instanceID: instanceID})
			wh.lastEventIndex[userID] = eventIndexAndInstanceID{eventIndex: eventIndex, instanceID: instanceID}
			wh.totalEvents.Add(1)
		} else {
			if instanceID != last.instanceID {
				l.Logf("webhooktest: instanceID change for user %q on index %d: [%s -> %s]", userID, eventIndex, last.instanceID, instanceID)
			}
			// in order event
			wh.lastEventIndex[userID] = eventIndexAndInstanceID{eventIndex: eventIndex, instanceID: instanceID}
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
	outOfOrderEvents map[string][]eventIndexAndInstanceID
	lastEventIndex   map[string]eventIndexAndInstanceID
}

type eventIndexAndInstanceID struct {
	eventIndex int
	instanceID string
}
