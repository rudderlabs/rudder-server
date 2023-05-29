package replay

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/lib/pq"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

var MessageTableName = "historic_message_id"

type DBHandle struct {
	db *sql.DB
}

type MessageJob struct {
	UserAgent   string
	MessageID   string
	WorkspaceID string
	SourceID    string
	SDKVersion  string
	AnonymousID string
	UserID      string
	CreatedAt   time.Time
}

func (h *DBHandle) SetupTables(ctx context.Context) error {
	sqlStatement := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %q (
    	id SERIAL PRIMARY KEY,
		user_agent TEXT NOT NULL DEFAULT '',
		message_id TEXT NOT NULL DEFAULT '',
		workspace_id TEXT NOT NULL,
		source_id TEXT NOT NULL,
		sdk_version TEXT NOT NULL DEFAULT '',
		anonymous_id TEXT NOT NULL DEFAULT '',
		user_id TEXT NOT NULL DEFAULT '',
		created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW());`, MessageTableName)
	_, err := h.db.ExecContext(ctx, sqlStatement)
	if err != nil {
		return err
	}
	return nil
}

func (h *DBHandle) Store(ctx context.Context, jobs []*MessageJob) error {
	tx, err := h.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	stmt, err := tx.PrepareContext(ctx, pq.CopyIn("historic_message_id", "user_agent", "message_id", "workspace_id", "source_id", "sdk_version", "anonymous_id", "user_id", "created_at"))
	if err != nil {
		return err
	}
	defer func() { _ = stmt.Close() }()
	for _, job := range jobs {
		if _, err = stmt.ExecContext(ctx, job.UserAgent, job.MessageID, job.WorkspaceID, job.SourceID, job.SDKVersion, job.AnonymousID, job.UserID, job.CreatedAt); err != nil {
			return err
		}
	}
	err = tx.Commit()
	return err
}

func setupMessageDB(_ context.Context) (*DBHandle, error) {
	psqlInfo := misc.GetConnectionString()
	sqlDB, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		return nil, err
	}
	return &DBHandle{db: sqlDB}, nil
}
