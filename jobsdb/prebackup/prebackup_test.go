package prebackup

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_DropSourceIdsHandler_Without_SourceIds(t *testing.T) {
	handler := DropSourceIds(func() []string { return []string{} })
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var txn *sql.Tx

	// when I execute the dropSourceIds handler for an empty source ids list, a canceled context and nil transaction
	err := handler.Handle(ctx, txn, "jobs", "job_status")

	// then handler doesn't do anything and no error is returned
	require.NoError(t, err)

}
