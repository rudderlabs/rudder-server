package uploadjob

import (
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
)

type state struct {
	inProgress string
	completed  string
	failed     string
}

var stateTransitions = map[string]*state{
	model.GeneratedUploadSchema: {
		inProgress: model.GeneratingUploadSchema,
		completed:  model.GeneratedUploadSchema,
		failed:     model.GeneratingUploadSchemaFailed,
	},
	model.CreatedTableUploads: {
		inProgress: model.CreatingTableUploads,
		completed:  model.CreatedTableUploads,
		failed:     model.CreatingTableUploadsFailed,
	},
	model.GeneratedLoadFiles: {
		inProgress: model.GeneratingLoadFiles,
		completed:  model.GeneratedLoadFiles,
		failed:     model.GeneratingLoadFilesFailed,
	},
	model.UpdatedTableUploadsCounts: {
		inProgress: model.UpdatingTableUploadsCounts,
		completed:  model.UpdatedTableUploadsCounts,
		failed:     model.UpdatingTableUploadsCountsFailed,
	},
	model.CreatedRemoteSchema: {
		inProgress: model.CreatingRemoteSchema,
		completed:  model.CreatedRemoteSchema,
		failed:     model.CreatingRemoteSchemaFailed,
	},
	model.ExportedData: {
		inProgress: model.ExportingData,
		completed:  model.ExportedData,
		failed:     model.ExportingDataFailed,
	},
}

func nextState(currentState string) *state {
	return stateTransitions[currentState]
}
