package router

import (
	"context"
	"fmt"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/manager"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/schema"
)

func (r *Router) syncRemoteSchema(ctx context.Context) error {
	r.configSubscriberLock.RLock()
	warehouses := append([]model.Warehouse{}, r.warehouses...)
	r.configSubscriberLock.RUnlock()

	whManager, err := manager.New(r.destType, r.conf, r.logger, r.statsFactory)
	if err != nil {
		return fmt.Errorf("failed to create warehouse manager: %w", err)
	}

	for _, warehouse := range warehouses {
		sh := schema.New(
			r.db,
			warehouse,
			r.conf,
			r.logger.Child("syncer"),
			r.statsFactory,
		)
		if err := r.SyncRemoteSchema(ctx, whManager, sh); err != nil {
			return err
		}
	}
	return nil
}

func (r *Router) SyncRemoteSchema(ctx context.Context, m manager.Manager, sh *schema.Schema) error {
	localSchema, err := sh.GetLocalSchema(ctx)
	if err != nil {
		return fmt.Errorf("fetching schema from local: %w", err)
	}

	if err := sh.FetchSchemaFromWarehouse(ctx, m); err != nil {
		return fmt.Errorf("fetching schema from warehouse: %w", err)
	}

	schemaChanged := sh.hasSchemaChanged(localSchema)
	if schemaChanged {
		err := sh.updateLocalSchema(ctx, uploadID, sh.schemaInWarehouse)
		if err != nil {
			return false, fmt.Errorf("updating local schema: %w", err)
		}
	}

	return schemaChanged, nil
}
