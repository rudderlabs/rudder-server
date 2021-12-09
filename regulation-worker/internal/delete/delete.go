package delete

//This is going to declare appropriate struct based on destination type & call `Deleter` method of it.
//to get deletion done.
//called by JobSvc with (model.Job, model.Destination).
//returns final status,error ({successful, failure}, err)

import (
	"context"
	"sync"

	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
)

//go:generate mockgen -source=delete.go -destination=mock_delete_test.go -package=delete github.com/rudderlabs/rudder-server/regulation-worker/internal/delete
type deleteManager interface {
	Delete(ctx context.Context, job model.Job, destConfig map[string]interface{}, destName string) model.JobStatus
	GetSupportedDestinations() []string
}

type Router struct {
	managers []deleteManager
	router   map[string]deleteManager
	once     sync.Once
}

func NewRouter(managers ...deleteManager) *Router {
	return &Router{
		managers: managers,
	}
}

func (r *Router) Delete(ctx context.Context, job model.Job, destDetail model.Destination) model.JobStatus {

	r.once.Do(func() {
		r.router = make(map[string]deleteManager)

		for _, m := range r.managers {
			destinations := m.GetSupportedDestinations()
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
