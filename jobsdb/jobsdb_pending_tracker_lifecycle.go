package jobsdb

import (
	"context"

	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-server/utils/crash"
)

func (t *pendingEventsTracker) Start() error {
	if err := t.JobsDB.Start(); err != nil {
		return err
	}
	t.lifecycle.mu.Lock()
	defer t.lifecycle.mu.Unlock()
	if t.lifecycle.started {
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	g := &errgroup.Group{}
	g.Go(crash.Wrapper(func() error {
		t.loop(ctx)
		return nil
	}))
	t.lifecycle.started, t.lifecycle.cancel, t.lifecycle.group = true, cancel, g
	return nil
}

func (t *pendingEventsTracker) Stop() {
	t.lifecycle.mu.Lock()
	if t.lifecycle.started {
		t.lifecycle.cancel()
		_ = t.lifecycle.group.Wait()
		t.lifecycle.started = false
	}
	t.lifecycle.mu.Unlock()
	t.JobsDB.Stop()
}
