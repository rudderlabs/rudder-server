package router

import (
	"context"
	"fmt"
	"time"

	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/manager"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/schema"
)

func (r *Router) syncRemoteSchema(ctx context.Context) error {
	for {
		r.configSubscriberLock.RLock()
		warehouses := append([]model.Warehouse{}, r.warehouses...)
		r.configSubscriberLock.RUnlock()
		execTime := time.Now()
		whManager, err := manager.New(r.destType, r.conf, r.logger, r.statsFactory)
		if err != nil {
			return fmt.Errorf("failed to create warehouse manager: %w", err)
		}
		for _, warehouse := range warehouses {
			err := whManager.Setup(ctx, warehouse, nil)
			if err != nil {
				return err
			}
			sh := schema.New(
				r.db,
				warehouse,
				r.conf,
				r.logger.Child("syncer"),
				r.statsFactory,
			)
			if err := r.SyncRemoteSchema(ctx, whManager, sh); err != nil {
				r.logger.Errorn("failed to sync schema", obskit.Error(err))
				continue
			}
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

func (r *Router) SyncRemoteSchema(ctx context.Context, m manager.Manager, sh *schema.Schema) error {
	localSchema, err := sh.GetLocalSchema(ctx)
	if err != nil {
		return fmt.Errorf("fetching schema from local: %w", err)
	}

	var schemaFromWarehouse model.Schema
	if schemaFromWarehouse, err = sh.FetchSchemaFromWarehouse(ctx, m); err != nil {
		return fmt.Errorf("fetching schema from warehouse: %w", err)
	}

	schemaChanged := sh.HasSchemaChanged(localSchema)
	if schemaChanged {
		err := sh.UpdateLocalSchemaWithWarehouse(ctx, schemaFromWarehouse)
		if err != nil {
			return fmt.Errorf("updating local schema: %w", err)
		}
	}
	return nil
}
