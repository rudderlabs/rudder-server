package router

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-go-kit/stats"

	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/manager"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/schema"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type syncSchemaRepo interface {
	GetLocalSchema(ctx context.Context) (model.Schema, error)
	UpdateLocalSchemaWithWarehouse(ctx context.Context, schema model.Schema) error
	HasSchemaChanged(schema model.Schema) bool
	FetchSchemaFromWarehouse(ctx context.Context, m schema.FetchSchemaRepo) (model.Schema, error)
}

func (r *Router) sync(ctx context.Context) error {
	for {
		r.configSubscriberLock.RLock()
		warehouses := append([]model.Warehouse{}, r.warehouses...)
		r.configSubscriberLock.RUnlock()
		execTime := r.now()
		log := r.logger.Child("syncer")

		var wg sync.WaitGroup
		ch := make(chan int, r.conf.GetInt("Warehouse.syncer.concurrency", 10))

		for _, warehouse := range warehouses {
			wg.Add(1)
			ch <- 1
			go func() {
				defer func() { wg.Done(); <-ch }()
				whManager, err := manager.New(r.destType, r.conf, r.logger, r.statsFactory)
				if err != nil {
					log.Warnn("create warehouse manager: %w", obskit.Error(err))
					r.statsFactory.NewTaggedStat("schema_sync_error", stats.CountType, map[string]string{
						"reason":        "manager_creation_error",
						"sourceId":      warehouse.Source.ID,
						"destinationId": warehouse.Destination.ID,
						"workspaceId":   warehouse.WorkspaceID,
					}).Increment()
					return
				}
				err = whManager.Setup(ctx, warehouse, warehouseutils.NewNoOpUploader())
				if err != nil {
					log.Warnn("failed to setup WH Manager", obskit.Error(err))
					r.statsFactory.NewTaggedStat("schema_sync_error", stats.CountType, map[string]string{
						"reason":        "manager_setup_error",
						"sourceId":      warehouse.Source.ID,
						"destinationId": warehouse.Destination.ID,
						"workspaceId":   warehouse.WorkspaceID,
					}).Increment()
					return
				}
				sh := schema.New(
					r.db,
					warehouse,
					r.conf,
					r.logger.Child("syncer"),
					r.statsFactory,
				)
				timerStat := stats.Default.NewTaggedStat("sync_remote_schema_time", stats.TimerType, map[string]string{
					"sourceId":      warehouse.Source.ID,
					"destinationId": warehouse.Destination.ID,
					"workspaceId":   warehouse.WorkspaceID,
				})
				startTime := r.now()
				if err := r.syncRemoteSchema(ctx, whManager, sh); err != nil {
					log.Warnn("failed to sync schema", obskit.Error(err))
					r.statsFactory.NewTaggedStat("schema_sync_error", stats.CountType, map[string]string{
						"reason":        "sync_remote_schema_error",
						"sourceId":      warehouse.Source.ID,
						"destinationId": warehouse.Destination.ID,
						"workspaceId":   warehouse.WorkspaceID,
					}).Increment()
				}
				timerStat.Since(startTime)
				whManager.Close()
			}()
		}
		wg.Wait()
		nextExecTime := execTime.Add(r.config.syncSchemaFrequency)
		select {
		case <-ctx.Done():
			log.Infon("context is cancelled, stopped running schema syncer")
			return nil
		case <-time.After(time.Until(nextExecTime)):
		}
	}
}

func (r *Router) syncRemoteSchema(ctx context.Context, m schema.FetchSchemaRepo, sh syncSchemaRepo) error {
	_, err := sh.GetLocalSchema(ctx)
	if err != nil {
		return fmt.Errorf("fetching schema from local: %w", err)
	}

	var schemaFromWarehouse model.Schema
	if schemaFromWarehouse, err = sh.FetchSchemaFromWarehouse(ctx, m); err != nil {
		return fmt.Errorf("fetching schema from warehouse: %w", err)
	}

	if sh.HasSchemaChanged(schemaFromWarehouse) {
		err := sh.UpdateLocalSchemaWithWarehouse(ctx, schemaFromWarehouse)
		if err != nil {
			return fmt.Errorf("updating local schema: %w", err)
		}
	}
	return nil
}
