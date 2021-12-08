package delete

import (
	"context"
	"fmt"
	"sync"

	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
)

//go:generate mockgen -source=delete.go -destination=mock_delete_test.go -package=delete github.com/rudderlabs/rudder-server/regulation-worker/internal/delete
type deleteManager interface {
	Delete(ctx context.Context, job model.Job, destConfig map[string]interface{}, destName string) model.JobStatus
	GetSupportedDestinations() []string
}

type DeleteRouter struct {
	Managers []deleteManager
	router   map[string]deleteManager
	once     sync.Once
}

func New(managers ...deleteManager) *DeleteRouter {
	return &DeleteRouter{
		Managers: managers,
	}
}

func (r *DeleteRouter) Delete(ctx context.Context, job model.Job, destDetail model.Destination) model.JobStatus {

	r.once.Do(func() {
		r.router = make(map[string]deleteManager)

		for _, m := range r.Managers {
			destinations := m.GetSupportedDestinations()
			for _, d := range destinations {
				r.router[d] = m
			}
		}
		fmt.Println("r.router=", r.router)
	})
	if _, ok := r.router[destDetail.Name]; ok {
		return r.router[destDetail.Name].Delete(ctx, job, destDetail.Config, destDetail.Name)
	}
	return model.JobStatusFailed
}
