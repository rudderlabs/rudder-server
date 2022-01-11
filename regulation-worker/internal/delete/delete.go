package delete

import (
	"context"
	"sync"

	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

var pkgLogger = logger.NewLogger().Child("client")

//go:generate mockgen -source=delete.go -destination=mock_delete_test.go -package=delete github.com/rudderlabs/rudder-server/regulation-worker/internal/delete
type deleteManager interface {
	Delete(ctx context.Context, job model.Job, destConfig map[string]interface{}, destName string) model.JobStatus
	GetSupportedDestinations() []string
}

type Router struct {
	Managers []deleteManager
	router   map[string]deleteManager
	once     sync.Once
}

func NewRouter(managers ...deleteManager) *Router {
	return &Router{
		Managers: managers,
	}
}

func (r *Router) Delete(ctx context.Context, job model.Job, destDetail model.Destination) model.JobStatus {
	pkgLogger.Debugf("deleting job: %v", job, "from destination: %v", destDetail)
	r.once.Do(func() {
		pkgLogger.Info("getting all the supported destination")
		r.router = make(map[string]deleteManager, len(r.Managers))

		for _, m := range r.Managers {
			destinations := m.GetSupportedDestinations()
			pkgLogger.Infof("deletion manager: %v", m, "support deletion from: %v", m, destinations)
			for _, d := range destinations {
				r.router[d] = m
			}
		}
	})
	if _, ok := r.router[destDetail.Name]; ok {
		pkgLogger.Debugf("calling deletion manager: %v", r.router[destDetail.Name])
		return r.router[destDetail.Name].Delete(ctx, job, destDetail.Config, destDetail.Name)
	}

	pkgLogger.Errorf("no deletion manager support deletion from destination: %v", destDetail.Name)
	return model.JobStatusFailed
}
