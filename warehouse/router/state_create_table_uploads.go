package router

import (
	"slices"

	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func (job *UploadJob) createTableUploads() error {
	schemaForUpload := job.upload.UploadSchema
	destType := job.warehouse.Type
	tables := make([]string, 0, len(schemaForUpload))
	for t := range schemaForUpload {
		tables = append(tables, t)
		// also track upload to rudder_identity_mappings if the upload has records for rudder_identity_merge_rules
		if slices.Contains(whutils.IdentityEnabledWarehouses, destType) && t == whutils.ToProviderCase(destType, whutils.IdentityMergeRulesTable) {
			if _, ok := schemaForUpload[whutils.ToProviderCase(destType, whutils.IdentityMappingsTable)]; !ok {
				tables = append(tables, whutils.ToProviderCase(destType, whutils.IdentityMappingsTable))
			}
		}
	}
	return job.tableUploadsRepo.Insert(
		job.ctx,
		job.upload.ID,
		tables,
	)
}
