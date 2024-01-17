package retltest

import (
	"sync"
	"testing"

	"github.com/rudderlabs/rudder-go-kit/testhelper/rand"
	whUtil "github.com/rudderlabs/rudder-server/testhelper/webhook"
)

// Webhook is a test helper for webhook destinations.
type Webhook struct {
	Recorder *whUtil.Recorder

	once sync.Once
	id   string
	name string
}

func (w *Webhook) ID() string {
	w.once.Do(func() {
		w.id = rand.String(27)
	})
	return w.id
}
func (w *Webhook) Name() string {
	return w.name
}
func (w *Webhook) TypeName() string {
	return "WEBHOOK"
}
func (w *Webhook) Config() map[string]interface{} {
	return map[string]interface{}{
		"webhookUrl":    w.Recorder.Server.URL,
		"webhookMethod": "POST",
	}
}
func (w *Webhook) Start(t *testing.T) {
	w.Recorder = whUtil.NewRecorder()
	t.Cleanup(w.Recorder.Close)

	t.Logf("Webhook URL: %s", w.Recorder.Server.URL)
}
func (w *Webhook) Shutdown(t *testing.T) {
	w.Recorder.Close()
}
func (w *Webhook) Count() int {
	return w.Recorder.RequestsCount()
}
