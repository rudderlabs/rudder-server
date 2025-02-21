package router

import (
	"fmt"

	"github.com/rudderlabs/rudder-server/warehouse/integrations/manager"
)

func (job *UploadJob) createRemoteSchema(whManager manager.Manager) error {
	if job.schemaHandle.IsWarehouseSchemaEmpty(job.ctx) {
		if err := whManager.CreateSchema(job.ctx); err != nil {
			return fmt.Errorf("creating schema: %w", err)
		}
	}
	return nil
}
