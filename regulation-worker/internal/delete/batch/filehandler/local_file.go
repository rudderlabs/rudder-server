package filehandler

import (
	"context"

	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
)

type LocalFileHandler interface {
	Read(ctx context.Context, path string) error
	RemoveIdentity(ctx context.Context, attributes []model.User) error
	Write(ctx context.Context, path string) error
}
