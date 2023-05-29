package replay

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

var MessageTableName = "historic_message_id"

type DBHandle struct {
	db  *sql.DB
	log logger.Logger
}

type MessageJob struct {
	UserAgent   string
	MessageID   string
	WorkspaceID string
	SourceID    string
	SDKVersion  string
	SDKName     string
	AnonymousID string
	UserID      string
	FilePath    string
	LineNumber  int
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
		sdk_name TEXT NOT NULL DEFAULT '',
		anonymous_id TEXT NOT NULL DEFAULT '',
		user_id TEXT NOT NULL DEFAULT '',
		file_path TEXT NOT NULL DEFAULT '',
		line_number INTEGER NOT NULL DEFAULT 0,
		created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW());`, MessageTableName)
	_, err := h.db.ExecContext(ctx, sqlStatement)
	if err != nil {
		h.log.Info("Error creating table: ", err)
		return err
	}
	return nil
}

func (h *DBHandle) Store(ctx context.Context, jobs []*MessageJob) error {
	sqlStatement := fmt.Sprintf(`INSERT INTO %q (user_agent, message_id, workspace_id, source_id, sdk_version,sdk_name, anonymous_id, user_id,file_path,line_number, created_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10,$)`, MessageTableName)
	for _, job := range jobs {
		_, err := h.db.ExecContext(ctx, sqlStatement, job.UserAgent, job.MessageID, job.WorkspaceID, job.SourceID, job.SDKVersion, job.SDKName, job.AnonymousID, job.UserID, job.FilePath, job.LineNumber, job.CreatedAt)
		if err != nil {
			h.log.Info("Error inserting row: ", err)
		}
	}
	return nil
}

func setupMessageDB(_ context.Context, log logger.Logger) (*DBHandle, error) {
	psqlInfo := misc.GetConnectionString()
	sqlDB, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		return nil, err
	}
	return &DBHandle{db: sqlDB, log: log}, nil
}
