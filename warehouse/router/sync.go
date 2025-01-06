package router

import (
	"context"
	"time"

	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/manager"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/internal/repo"
	"github.com/rudderlabs/rudder-server/warehouse/schema"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func (r *Router) sync(ctx context.Context) error {
	for {
		r.configSubscriberLock.RLock()
		warehouses := append([]model.Warehouse{}, r.warehouses...)
		r.configSubscriberLock.RUnlock()
		execTime := time.Now()
		for _, warehouse := range warehouses {
			whManager, err := manager.New(r.destType, r.conf, r.logger, r.statsFactory)
			if err != nil {
				r.logger.Errorn("create warehouse manager: %w", obskit.Error(err))
				continue
			}
			err = whManager.Setup(ctx, warehouse, warehouseutils.NewNoOpUploader())
			if err != nil {
				r.logger.Errorn("failed to setup WH Manager", obskit.Error(err))
				continue
			}
			if err := schema.FetchAndSaveSchema(ctx, whManager, warehouse, repo.NewWHSchemas(r.db), r.logger.Child("syncer")); err != nil {
				r.logger.Errorn("failed to sync schema", obskit.Error(err))
				continue
			}
			whManager.Cleanup(ctx)
		}
		nextExecTime := execTime.Add(r.config.syncSchemaFrequency)
		select {
		case <-ctx.Done():
			r.logger.Infon("context is cancelled, stopped running schema syncer")
			return nil
		case <-time.After(time.Until(nextExecTime)):
		}
	}
}
