package cluster_test

import (
	"context"
	"testing"

	"github.com/rudderlabs/rudder-server/app/cluster"
)


var _ cluster.ModeChange = &mockModeChange{}

type mockModeChange struct {
	value string
	acked bool
	wait  chan bool
}

func newMode(value string) mockModeChange {
	return mockModeChange{value: value, wait: make(chan bool)}
}

func (m *mockModeChange) Value() string {
	return m.value
}

func (m *mockModeChange) Ack() error {
	m.acked = true
	close(m.wait)
	return nil
}

func (m *mockModeChange) WaitAck() {
	<-m.wait
}

type mockModeProvider struct {
	ch chan mockModeChange
}

func (m *mockModeProvider) ServerMode() chan cluster.ModeChange {
	return m.ch
}

func (m *mockModeProvider) SendMode(newMode mockModeChange) {
	m.ch <- newMode
}

func TestDynamicCluster(t *testing.T) {
	provider := &mockModeProvider{ch: make(chan mockModeChange)}

	dc := cluster.Dynamic{
		Provider: provider,
	}

	ctx, cancel := context.WithCancel(context.Background())

	wait := make(chan bool)
	go func() {
		dc.Run(ctx)
		close(wait)
	}()

	newMode := newMode("test")
	provider.SendMode(newMode)


	newMode.WaitAck()

	cancel()
	<-wait
}
