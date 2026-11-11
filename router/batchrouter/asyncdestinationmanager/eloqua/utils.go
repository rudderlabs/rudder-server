package eloqua

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"math"
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
	strict := config.GetBoolVar(true, "BatchRouter.ELOQUA.strictResultReconciliation")
	importID := gjson.GetBytes(parameters, "importId").String()
	byRow := make(map[int64][]int64)
	missing := make(map[int64]bool)
	for _, job := range importingList {
		row := gjson.GetBytes(job.LastJobStatus.Parameters, "metadata.csvRowIndex")
		if !row.Exists() || row.Type != gjson.Number || row.Int() < 1 || row.Raw != fmt.Sprint(row.Int()) ||
			(strict && gjson.GetBytes(job.LastJobStatus.Parameters, "importId").String() != importID) {
			missing[job.JobID] = true
			continue
		}
		byRow[row.Int()] = append(byRow[row.Int()], job.JobID)
	}
	if len(byRow) == 0 {
		return abortAmbiguousJobs(importingList, eloqua, "no CSV row correlation metadata"), nil
	}

	complete := true
	count := gjson.GetBytes(parameters, "importCount")
	if strict && (importID == "" || !strings.HasPrefix(importID, data.DynamicPart+":") ||
		count.Type != gjson.Number || count.Raw != fmt.Sprint(count.Int()) || count.Int() != int64(len(importingList))) {
		complete = false
	}
	var uniqueKeys []string
	policy := gjson.GetBytes(parameters, "uniqueKeys")
	if policy.Exists() {
		if err := jsonrs.Unmarshal([]byte(policy.Raw), &uniqueKeys); err != nil {
			complete = false
		}
	} else if strict {
		complete = false
	}

	metadata := &common.EventStatMeta{AbortedReasons: make(map[int64]string)}
	seenRows := make(map[int64]bool)
	failedJobs := make(map[int64]bool)
	matchedRows := 0
	resultCount := 0
	applyPage := func(items []RejectedItem) {
		resultCount += len(items)
		for _, item := range items {
			ids := byRow[item.RecordIndex]
			if len(ids) == 0 || seenRows[item.RecordIndex] {
				complete = false
				continue
			}
			seenRows[item.RecordIndex] = true
			matchedRows++
			if lo.Contains(eloqua.successStatusCode, item.StatusCode) && len(lo.Intersect(uniqueKeys, item.InvalidFields)) == 0 && policy.Exists() {
				continue
			}
			for _, id := range ids {
				if !failedJobs[id] {
					failedJobs[id] = true
					metadata.AbortedKeys = append(metadata.AbortedKeys, id)
					metadata.AbortedReasons[id] = generateErrorString(item)
				}
			}
		}
	}

	// Eloqua returns up to 1000 rejects per call, starting at offset zero.
	page, err := eloqua.service.CheckRejectedData(data)
	if err != nil {
		return nil, fmt.Errorf("fetching Eloqua rejects: %w", err)
	}
	if page == nil {
		return abortAmbiguousJobs(importingList, eloqua, "missing rejects response"), nil
	}
	applyPage(page.Items)
	if page.Count > 0 {
		iterations := int(math.Ceil(float64(page.TotalResults) / 1000))
		for i := 1; i < iterations; i++ {
			data.Offset = i * 1000
			next, fetchErr := eloqua.service.CheckRejectedData(data)
			if fetchErr != nil {
				return nil, fmt.Errorf("fetching Eloqua rejects: %w", fetchErr)
			}
			if next == nil {
				complete = false
				break
			}
			applyPage(next.Items)
		}
	}
	if page.TotalResults < 0 || resultCount != page.TotalResults {
		complete = false
	}
	if matchedRows != page.TotalResults {
		complete = false
	}
	ambiguousCount := 0
	for _, job := range importingList {
		if failedJobs[job.JobID] {
			continue
		}
		if missing[job.JobID] || !complete {
			ambiguousCount++
			metadata.AbortedKeys = append(metadata.AbortedKeys, job.JobID)
			metadata.AbortedReasons[job.JobID] = correlationFailureReason
		} else {
			metadata.SucceededKeys = append(metadata.SucceededKeys, job.JobID)
		}
	}
	if ambiguousCount > 0 {
		eloqua.logger.Errorn("Ambiguous Eloqua import result", logger.NewIntField("jobs", int64(ambiguousCount)), logger.NewIntField("matched_rows", int64(matchedRows)))
		eloqua.statsFactory.NewTaggedStat("result_reconciliation_ambiguity", stats.CountType, map[string]string{"module": "batch_router", "destType": eloqua.destName}).Count(ambiguousCount)
	}
	return metadata, nil
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
