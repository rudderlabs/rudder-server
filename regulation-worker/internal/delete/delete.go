package delete

import (
	"context"
	"sync"

	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
)

//go:generate mockgen -source=delete.go -destination=mock_delete_test.go -package=delete github.com/rudderlabs/rudder-server/regulation-worker/internal/delete
type deleteManager interface {
	Delete(ctx context.Context, job model.Job, destConfig map[string]interface{}, destName string) model.JobStatus
	GetSupportedDestination() []string
}

type DeleteRouter struct {
	Managers []deleteManager
	router   map[string]deleteManager
	once     sync.Once
}

func (r *DeleteRouter) Delete(ctx context.Context, job model.Job, destDetail model.Destination) model.JobStatus {

	r.once.Do(func() {
		r.router = make(map[string]deleteManager)

		for _, m := range r.Managers {
			destinations := m.GetSupportedDestination()
			for _, d := range destinations {
				r.router[d] = m
			}
		}
	})
	if _, ok := r.router[destDetail.Name]; ok {
		return r.router[destDetail.Name].Delete(ctx, job, destDetail.Config, destDetail.Name)
	}
	return model.JobStatusFailed
}
