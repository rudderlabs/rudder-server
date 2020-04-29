package migrator

import (
	"net/http"

	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/pathfinder"
)

//ExportImporter handles migrations for inplace cluster
type ExportImporter struct {
	exporter Exporter
	importer Importer
}

//Setup sets up the underlying exporter and importer
func (exportImporter *ExportImporter) Setup(jobsDB *jobsdb.HandleT, pf pathfinder.Pathfinder) {
	exportImporter.exporter.Setup(jobsDB, pf)
	exportImporter.importer.Setup(jobsDB, pf)
}

func (exportImporter *ExportImporter) ExportStatusHandler() bool {
	return exportImporter.exporter.ExportStatusHandler()
}

func (exportImporter *ExportImporter) ImportHandler(w http.ResponseWriter, r *http.Request) {
	exportImporter.importer.ImportHandler(w, r)
}

func (exportImporter *ExportImporter) ImportStatusHandler() bool {
	return exportImporter.importer.ImportStatusHandler()
}
