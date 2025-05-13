package loadfiles

import (
	"context"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
)

type Generator interface {
	ForceCreateLoadFiles(ctx context.Context, job *model.UploadJob) (int64, int64, error)
	CreateLoadFiles(ctx context.Context, job *model.UploadJob) (int64, int64, error)
}
