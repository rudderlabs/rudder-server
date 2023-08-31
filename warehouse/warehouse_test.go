package warehouse

import "testing"

func TestApp(t *testing.T) {
	t.Run("Graceful shutdown", func(t *testing.T) {})
	t.Run("degraded", func(t *testing.T) {})
	t.Run("mode=embedded", func(t *testing.T) {})
	t.Run("mode=embedded_master", func(t *testing.T) {})
	t.Run("mode=master", func(t *testing.T) {})
	t.Run("mode=master_and_slave", func(t *testing.T) {})
	t.Run("mode=off", func(t *testing.T) {})
	t.Run("mode=slave", func(t *testing.T) {})
	t.Run("incompatible postgres", func(t *testing.T) {})
	t.Run("postgres down", func(t *testing.T) {})
	t.Run("without env vars", func(t *testing.T) {})
	t.Run("monitor routers", func(t *testing.T) {})
	t.Run("upload frequency exceeded", func(t *testing.T) {})
	t.Run("last processed marker", func(t *testing.T) {})
	t.Run("trigger upload", func(t *testing.T) {})
}
