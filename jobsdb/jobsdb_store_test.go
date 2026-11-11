package jobsdb

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"
)

// TestStoreCancelledDuringCopy cancels the context while a store's COPY is still streaming rows
// and verifies that the store returns instead of leaving the connection stuck.
func TestStoreCancelledDuringCopy(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pg, err := postgres.Setup(pool, t)
	require.NoError(t, err)

	db := NewForReadWrite("copy_cancel", WithDBHandle(pg.DB), WithConfig(config.New()))
	require.NoError(t, db.Start())
	t.Cleanup(db.Stop)

	payload := `{"data":"` + strings.Repeat("x", 2048) + `"}`
	jobs := make([]*JobT, 2000)
	for i := range jobs {
		jobs[i] = &JobT{
			UUID:         uuid.New(),
			UserID:       "user-" + strconv.Itoa(i),
			CustomVal:    "test",
			EventCount:   1,
			Parameters:   []byte(`{}`),
			EventPayload: []byte(payload),
			WorkspaceId:  "workspace-1",
		}
	}

	var cancelled int
	for i := range 50 {
		ctx, cancel := context.WithCancel(t.Context())
		time.AfterFunc(time.Duration(i%10)*time.Millisecond, cancel)
		done := make(chan error, 1)
		go func() {
			done <- db.Store(ctx, jobs)
		}()
		select {
		case err := <-done:
			if err != nil {
				cancelled++
			}
		case <-time.After(30 * time.Second):
			t.Fatalf("iteration %d: store did not return after its context was cancelled", i)
		}
		cancel()
	}
	require.Positive(t, cancelled, "no store was cancelled before it completed")
}
