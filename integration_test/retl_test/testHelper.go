package retltest

import (
	"sync"
	"testing"

	"github.com/rudderlabs/rudder-go-kit/testhelper/rand"
	whUtil "github.com/rudderlabs/rudder-server/testhelper/webhook"
)

// webhook is a test helper for webhook destinations.
type webhook struct {
	Recorder *whUtil.Recorder

	once sync.Once
	id   string
	name string
}

func (w *webhook) ID() string {
	w.once.Do(func() {
		w.id = rand.String(27)
	})
	return w.id
}

func (w *webhook) Name() string {
	return w.name
}

func (w *webhook) TypeName() string {
	return "WEBHOOK"
}

func (w *webhook) Config() map[string]interface{} {
	return map[string]interface{}{
		"webhookUrl":    w.Recorder.Server.URL,
		"webhookMethod": "POST",
	}
}

func (w *webhook) Start(t *testing.T) {
	w.Recorder = whUtil.NewRecorder()
	t.Cleanup(w.Recorder.Close)

	t.Logf("Webhook URL: %s", w.Recorder.Server.URL)
}

func (w *webhook) Shutdown(*testing.T) {
	w.Recorder.Close()
}

func (w *webhook) Count() int {
	return w.Recorder.RequestsCount()
}
