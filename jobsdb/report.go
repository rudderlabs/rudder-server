package jobsdb

import (
	"fmt"
	"os"
	"path"

	"github.com/rudderlabs/rudder-server/app/crash"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

// Crash report handling - On a crash, a JobsDB will dump basic metadata and table contents

func handleReportError() {
	if err := recover(); err != nil {
		logger.Error(err)
	}
}

func (jd *HandleT) registerCrashReportHandlers() {
	reportDir := fmt.Sprintf("jobsdb/%s/report.json", jd.tablePrefix)
	crash.Default.Report.RegisterFileHandler(reportDir, jd.reportFileHandler)
}

func datasetsMetadata(datasets []dataSetT) map[string]interface{} {
	metadata := make(map[string]interface{})

	metadata["count"] = len(datasets)

	return metadata
}

func (jd *HandleT) reportFileHandler(file *os.File) error {
	defer handleReportError()
	datasets := jd.getDSList(true)

	dir := path.Dir(file.Name())

	jd.createTableDataReports(datasets, dir)
	jd.createReportMetadata(datasets, file)

	return nil
}

func (jd *HandleT) createTableDataReports(datasets []dataSetT, dir string) error {
	jd.createTableFile(dir, jd.GetTableName("journal"))
	jd.createTableFile(dir, jd.GetTableName("schema_migrations"))

	for _, ds := range datasets {
		jd.createTableFileWithFilter(dir, ds.JobTable, jobTableDataFilter)
		jd.createTableFile(dir, ds.JobStatusTable)
	}

	return nil
}

func jobTableDataFilter(m map[string]interface{}) map[string]interface{} {
	delete(m, "event_payload")
	return m
}

func (jd *HandleT) createReportMetadata(datasets []dataSetT, file *os.File) error {
	metadata := make(map[string]interface{})
	metadata["datasets"] = datasetsMetadata(datasets)

	return misc.WriteMapToWriter(metadata, file)
}

func (jd *HandleT) createTableFile(dir string, table string) {
	jd.createTableFileWithFilter(dir, table, nil)
}

func (jd *HandleT) createTableFileWithFilter(dir string, table string, filter misc.DumpQueryFilter) {
	logger.Infof("JobsDB: %[1]s: Dumping data for table %[2]s", jd.tablePrefix, table)
	filename := path.Join(dir, fmt.Sprintf("%s.jsonl", table))

	// create file
	file, err := os.Create(filename)
	if err != nil {
		logger.Errorf("Could not create table file %s for jobsdb %s: %v", table, jd.tablePrefix, err)
	}
	defer file.Close()

	query := fmt.Sprintf("SELECT * FROM %s", table)

	if err = misc.DumpQueryToWriter(jd.dbHandle, query, file, filter); err != nil {
		logger.Errorf("Could not dump data of table %s for jobsdb %s: %v", table, jd.tablePrefix, err)
	}
}
