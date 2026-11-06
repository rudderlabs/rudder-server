package eloqua

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/uuid"
	"github.com/samber/lo"
	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

const bufferSize = 5000 * 1024

func getEventDetails(file *os.File) (*EventDetails, error) {
	_, _ = file.Seek(0, 0)
	scanner := bufio.NewScanner(file)
	scanner.Buffer(nil, bufferSize)
	eventDetails := EventDetails{}
	if scanner.Scan() {
		line := scanner.Text()
		var data TransformedData
		if err := jsonrs.Unmarshal([]byte(line), &data); err != nil {
			return &eventDetails, fmt.Errorf("error in unmarshalling data, %v", err)
		}
		if data.Message.Type == "track" && data.Message.CustomObjectId != "" {
			eventDetails.Type = "track"
			eventDetails.CustomObjectId = data.Message.CustomObjectId
			eventDetails.Fields = getKeys(data.Message.Data)
			eventDetails.IdentifierFieldName = data.Message.IdentifierFieldName
			return &eventDetails, nil
		} else if data.Message.Type == "identify" && data.Message.CustomObjectId == "contacts" {
			eventDetails.Type = "identify"
			eventDetails.Fields = getKeys(data.Message.Data)
			eventDetails.IdentifierFieldName = data.Message.IdentifierFieldName
			return &eventDetails, nil
		} else {
			return nil, fmt.Errorf("unable to find event format")
		}
	}
	return &eventDetails, fmt.Errorf("unable to scan data from the file")
}

func getKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func createCSVFile(fields []string, file *os.File, uploadJobInfo *JobInfo, jobIdRowMap map[int64]int64) (string, int64, error) {
	_, _ = file.Seek(0, 0)
	scanner := bufio.NewScanner(file)
	scanner.Buffer(nil, bufferSize)
	tmpDirPath, err := misc.GetTmpDir()
	if err != nil {
		return "", 0, err
	}
	folderPath := filepath.Join(tmpDirPath, misc.RudderAsyncDestinationLogs)
	_, e := os.Stat(folderPath)
	if os.IsNotExist(e) {
		folderPath, _ = os.MkdirTemp(folderPath, "")
	}
	path := filepath.Join(folderPath, uuid.NewString())
	csvFilePath := fmt.Sprintf(`%v.csv`, path)
	csvFile, err := os.Create(csvFilePath)
	if err != nil {
		return "", 0, err
	}
	csvWriter := csv.NewWriter(csvFile)
	err = csvWriter.Write(fields)
	if err != nil {
		return "", 0, err
	}
	csvWriter.Flush()
	var line string
	index := 0
	for scanner.Scan() {
		line = scanner.Text()
		var data TransformedData
		if err := jsonrs.Unmarshal([]byte(line), &data); err != nil {
			return "", 0, err
		}
		var values []string
		for _, field := range fields {
			val, ok := data.Message.Data[field].(string)
			if !ok || val == "null" {
				values = append(values, "")
			} else {
				values = append(values, val)
			}
		}
		fileInfo, err := csvFile.Stat()
		if err != nil {
			return "", 0, err
		}
		if fileInfo.Size() > uploadJobInfo.fileSizeLimit {
			left, _ := lo.Difference(uploadJobInfo.importingJobs, uploadJobInfo.succeededJobs)
			uploadJobInfo.failedJobs = append(uploadJobInfo.failedJobs, left...)
			break
		}
		jobIdRowMap[int64(index+1)] = uploadJobInfo.importingJobs[index]
		uploadJobInfo.succeededJobs = append(uploadJobInfo.succeededJobs, uploadJobInfo.importingJobs[index])
		index += 1
		err = csvWriter.Write(values)
		if err != nil {
			return "", 0, err
		}
		csvWriter.Flush()
	}
	fileInfo, _ := csvFile.Stat()
	return csvFilePath, fileInfo.Size(), nil
}

func createBodyForImportDefinition(eventDetails *EventDetails, eloquaFields *Fields) (map[string]any, error) {
	fieldStatement := make(map[string]string)
	for _, val := range eventDetails.Fields {
		for _, val1 := range eloquaFields.Items {
			if val1.InternalName == val {
				fieldStatement[val] = val1.Statement
			}
		}
	}
	switch eventDetails.Type {
	case "identify":
		return map[string]any{
			"name":                    "Rudderstack-Contact-Import",
			"fields":                  fieldStatement,
			"identifierFieldName":     eventDetails.IdentifierFieldName,
			"isSyncTriggeredOnImport": false,
		}, nil
	case "track":
		return map[string]any{
			"name":                    "Rudderstack-CustomObject-Import",
			"updateRule":              "always",
			"fields":                  fieldStatement,
			"identifierFieldName":     eventDetails.IdentifierFieldName,
			"isSyncTriggeredOnImport": false,
		}, nil
	default:
		return nil, fmt.Errorf("unable to create body for import definition")
	}
}

func generateErrorString(item RejectedItem) string {
	var invalidItems strings.Builder
	len := len(item.InvalidFields)
	for index, invalidField := range item.InvalidFields {
		invalidItems.WriteString(invalidField + " : " + item.FieldValues[invalidField])
		if index != len-1 {
			invalidItems.WriteString(", ")
		}
	}
	return item.StatusCode + " : " + item.Message + " " + invalidItems.String()
}

const correlationFailureReason = "Eloqua result cannot be reconciled to an uploaded CSV row; records may already have been imported"

func abortAmbiguousJobs(importingList []*jobsdb.JobT, eloqua *EloquaBulkUploader, reason string) *common.EventStatMeta {
	eloqua.logger.Errorn("Ambiguous Eloqua import result; aborting importing jobs", logger.NewStringField("reason", reason), logger.NewIntField("importing_jobs", int64(len(importingList))))
	eloqua.statsFactory.NewTaggedStat("result_reconciliation_ambiguity", stats.CountType, map[string]string{
		"module": "batch_router", "destType": eloqua.destName,
	}).Count(len(importingList))
	meta := &common.EventStatMeta{AbortedReasons: make(map[int64]string)}
	for _, job := range importingList {
		meta.AbortedKeys = append(meta.AbortedKeys, job.JobID)
		meta.AbortedReasons[job.JobID] = correlationFailureReason + ": " + reason
	}
	return meta
}

func parseRejectedData(data *HttpRequestData, importingList []*jobsdb.JobT, eloqua *EloquaBulkUploader, parameters []byte) (*common.EventStatMeta, error) {
	importID := gjson.GetBytes(parameters, "importId").String()
	if importID == "" || !strings.HasPrefix(importID, data.DynamicPart+":") {
		return abortAmbiguousJobs(importingList, eloqua, "missing or mismatched sync ID"), nil
	}
	if expected := gjson.GetBytes(parameters, "importCount"); expected.Exists() {
		if expected.Type != gjson.Number || expected.Int() < 0 || expected.Raw != fmt.Sprint(expected.Int()) {
			return nil, fmt.Errorf("eloqua importing list has invalid import count")
		}
		if expected.Int() != int64(len(importingList)) {
			return nil, fmt.Errorf("eloqua importing list incomplete: expected %d jobs, got %d", expected.Int(), len(importingList))
		}
	} else {
		return nil, fmt.Errorf("eloqua importing list has missing import count")
	}
	jobIDs := make([]int64, 0, len(importingList))
	rowToJob := make(map[int64]int64, len(importingList))
	for _, job := range importingList {
		jobIDs = append(jobIDs, job.JobID)
		if gjson.GetBytes(job.LastJobStatus.Parameters, "importId").String() != importID {
			return abortAmbiguousJobs(importingList, eloqua, "mixed importing sync IDs"), nil
		}
		row := gjson.GetBytes(job.LastJobStatus.Parameters, "metadata.csvRowIndex")
		if !row.Exists() || row.Int() < 1 || row.Type != gjson.Number || row.Raw != fmt.Sprint(row.Int()) {
			return abortAmbiguousJobs(importingList, eloqua, "missing or invalid CSV row index"), nil
		}
		if _, exists := rowToJob[row.Int()]; exists {
			return abortAmbiguousJobs(importingList, eloqua, "duplicate CSV row index"), nil
		}
		rowToJob[row.Int()] = job.JobID
	}
	// Rows are sequential and one-based; gaps mean the importing set is incomplete.
	for i := 1; i <= len(importingList); i++ {
		if _, ok := rowToJob[int64(i)]; !ok {
			return abortAmbiguousJobs(importingList, eloqua, "incomplete CSV row mapping"), nil
		}
	}
	var uniqueKeys []string
	if policy := gjson.GetBytes(parameters, "uniqueKeys"); policy.Exists() {
		if policy.Type != gjson.JSON || !strings.HasPrefix(policy.Raw, "[") {
			return abortAmbiguousJobs(importingList, eloqua, "invalid unique keys policy"), nil
		}
		if err := jsonrs.Unmarshal([]byte(policy.Raw), &uniqueKeys); err != nil {
			// The policy is persisted with this import; malformed metadata is
			// irrecoverable, unlike a transient rejects-fetch error.
			return abortAmbiguousJobs(importingList, eloqua, "invalid unique keys policy"), nil //nolint:nilerr
		}
	} else if config.GetBoolVar(true, "BatchRouter.ELOQUA.strictResultReconciliation") {
		return abortAmbiguousJobs(importingList, eloqua, "missing unique keys policy"), nil
	}

	abortedJobIDs := []int64{}
	abortedReasons := map[int64]string{}
	seen := make(map[int64]bool)
	// Use the API's reported page size rather than assuming a 1000-row page.
	// Fetch and validate all pages before assigning any job a terminal status.
	for offset, total := 0, -1; total < 0 || offset < total; {
		data.Offset = offset
		page, err := eloqua.service.CheckRejectedData(data)
		if err != nil {
			return nil, fmt.Errorf("fetching Eloqua rejects at offset %d: %w", offset, err)
		}
		if page == nil || page.TotalResults < 0 || page.Count != len(page.Items) || page.Count < 0 || page.Offset != offset ||
			(page.Limit <= 0 && page.HasMore) || (page.Limit > 0 && page.Count > page.Limit) ||
			(total >= 0 && total != page.TotalResults) || offset+page.Count > page.TotalResults ||
			(page.HasMore != (offset+page.Count < page.TotalResults)) || (page.HasMore && page.Count == 0) {
			return abortAmbiguousJobs(importingList, eloqua, "incomplete or inconsistent rejects pagination"), nil
		}
		total = page.TotalResults
		for _, val := range page.Items {
			jobID, ok := rowToJob[val.RecordIndex]
			if !ok || seen[val.RecordIndex] {
				return abortAmbiguousJobs(importingList, eloqua, fmt.Sprintf("unknown or duplicate reject row index %d", val.RecordIndex)), nil
			}
			seen[val.RecordIndex] = true
			if lo.Contains(eloqua.successStatusCode, val.StatusCode) && len(lo.Intersect(uniqueKeys, val.InvalidFields)) == 0 {
				continue
			}
			abortedJobIDs = append(abortedJobIDs, jobID)
			abortedReasons[jobID] = generateErrorString(val)
		}
		offset += page.Count
	}
	left, _ := lo.Difference(jobIDs, abortedJobIDs)
	return &common.EventStatMeta{AbortedKeys: abortedJobIDs, AbortedReasons: abortedReasons, SucceededKeys: left}, nil
}

func parseFailedData(syncId string, importingList []*jobsdb.JobT) *common.EventStatMeta {
	jobIDs := []int64{}
	abortedReasons := map[int64]string{}
	for _, job := range importingList {
		jobIDs = append(jobIDs, job.JobID)
		abortedReasons[job.JobID] = "some error occurred please check the logs, using this syncId: " + syncId
	}

	eventStatMeta := common.EventStatMeta{
		AbortedKeys:    jobIDs,
		AbortedReasons: abortedReasons,
		SucceededKeys:  []int64{},
	}
	return &eventStatMeta
}

func getUniqueKeys(eloquaFields *Fields) []string {
	uniqueKeys := []string{}
	for _, item := range eloquaFields.Items {
		if item.HasUniquenessConstraint {
			uniqueKeys = append(uniqueKeys, item.InternalName)
		}
	}
	return uniqueKeys
}
