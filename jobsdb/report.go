package jobsdb

import (
	"fmt"
	"os"
	"path"

	"github.com/rudderlabs/rudder-server/app/crash"
	"github.com/rudderlabs/rudder-server/utils/logger"
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
		jd.createTableFile(dir, ds.JobTable)
		jd.createTableFile(dir, ds.JobStatusTable)
	}

	return nil
}

func (jd *HandleT) createReportMetadata(datasets []dataSetT, file *os.File) error {
	metadata := make(map[string]interface{})
	metadata["datasets"] = datasetsMetadata(datasets)

	return crash.WriteMapToFile(metadata, file)
}

func (jd *HandleT) createTableFile(dir string, table string) error {
	logger.Infof("JobsDB: %[1]s: Dumping data for table %[2]s", jd.tablePrefix, table)

	// create file
	file, err := os.Create(path.Join(dir, fmt.Sprintf("%s.jsonl", table)))
	if err != nil {
		panic(err)
	}
	defer file.Close()

	// query all data
	query := fmt.Sprintf("SELECT * FROM %s", table)
	rows, err := jd.dbHandle.Query(query)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		panic(err)
	}

	// scan all rows, for each write a map to file, with columns as keys and query results as values.
	values := make([][]byte, len(columns))
	valuePointers := make([]interface{}, len(columns))

	for i := range values {
		valuePointers[i] = &values[i]
	}

	for rows.Next() {
		row := make(map[string]interface{})
		err := rows.Scan(valuePointers...)
		if err != nil {
			panic(err)
		}

		for i, raw := range values {
			row[columns[i]] = string(raw)
		}

		crash.WriteMapToFile(row, file)
	}

	return nil
}
