package testcompose

import (
	"context"
	"testing"

	"github.com/rudderlabs/compose-test/compose"
)

type TestingCompose struct {
	compose *compose.Compose
	t       testing.TB
}

func New(t testing.TB, file compose.File) *TestingCompose {
	t.Helper()

	c, err := compose.New(file)
	if err != nil {
		t.Fatalf("open compose: %v", err)
	}

	return &TestingCompose{
		compose: c,
		t:       t,
	}
}

func (tc *TestingCompose) Start(ctx context.Context) {
	tc.t.Helper()
	err := tc.compose.Start(ctx)
	if err != nil {
		tc.t.Fatalf("compose library start: %v", err)
	}

	tc.t.Cleanup(func() {
		tc.Stop(context.Background())
	})
}

func (tc *TestingCompose) Stop(ctx context.Context) {
	tc.t.Helper()

	err := tc.compose.Stop(ctx)
	if err != nil {
		tc.t.Fatalf("compose library stop: %v", err)
	}
}

func (tc *TestingCompose) Port(service string, port int) int {
	tc.t.Helper()

	p, err := tc.compose.Port(service, port)
	if err != nil {
		tc.t.Fatalf("compose library port: %v", err)
	}

	return p
}

func (tc *TestingCompose) Exec(ctx context.Context, service string, command ...string) string {
	tc.t.Helper()

	out, err := tc.compose.Exec(ctx, service, command...)
	if err != nil {
		tc.t.Fatalf("compose library exec: %v", err)
	}

	return out
}

func (tc *TestingCompose) Env(service, name string) string {
	tc.t.Helper()

	v, err := tc.compose.Env(service, name)
	if err != nil {
		tc.t.Fatalf("compose library port: %v", err)
	}

	return v
}
