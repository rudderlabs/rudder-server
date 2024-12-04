package eloqua

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path"

	"github.com/google/uuid"
	"github.com/samber/lo"

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
		if err := json.Unmarshal([]byte(line), &data); err != nil {
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

func getKeys(m map[string]interface{}) []string {
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
	localTmpDirName := fmt.Sprintf(`/%s/`, misc.RudderAsyncDestinationLogs)
	tmpDirPath, err := misc.CreateTMPDIR()
	if err != nil {
		return "", 0, err
	}
	folderPath := path.Join(tmpDirPath, localTmpDirName)
	_, e := os.Stat(folderPath)
	if os.IsNotExist(e) {
		folderPath, _ = os.MkdirTemp(folderPath, "")
	}
	path := path.Join(folderPath, uuid.NewString())
	// path := path.Join(tmpDirPath, localTmpDirName, uuid.NewString())
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
		if err := json.Unmarshal([]byte(line), &data); err != nil {
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

func createBodyForImportDefinition(eventDetails *EventDetails, eloquaFields *Fields) (map[string]interface{}, error) {
	fieldStatement := make(map[string]string)
	for _, val := range eventDetails.Fields {
		for _, val1 := range eloquaFields.Items {
			if val1.InternalName == val {
				fieldStatement[val] = val1.Statement
			}
		}
	}
	if eventDetails.Type == "identify" {
		return map[string]interface{}{
			"name":                    "Rudderstack-Contact-Import",
			"fields":                  fieldStatement,
			"identifierFieldName":     eventDetails.IdentifierFieldName,
			"isSyncTriggeredOnImport": false,
		}, nil
	} else if eventDetails.Type == "track" {
		return map[string]interface{}{
			"name":                    "Rudderstack-CustomObject-Import",
			"updateRule":              "always",
			"fields":                  fieldStatement,
			"identifierFieldName":     eventDetails.IdentifierFieldName,
			"isSyncTriggeredOnImport": false,
		}, nil
	}

	return nil, fmt.Errorf("unable to create body for import definition")
}

func generateErrorString(item RejectedItem) string {
	var invalidItems string
	len := len(item.InvalidFields)
	for index, invalidField := range item.InvalidFields {
		invalidItems += invalidField + " : " + item.FieldValues[invalidField]
		if index != len-1 {
			invalidItems += ", "
		}
	}
	return item.StatusCode + " : " + item.Message + " " + invalidItems
}

func parseRejectedData(data *HttpRequestData, importingList []*jobsdb.JobT, eloqua *EloquaBulkUploader) (*common.EventStatMeta, error) {
	jobIDs := []int64{}
	for _, job := range importingList {
		jobIDs = append(jobIDs, job.JobID)
	}
	rejectResponse, err := eloqua.service.CheckRejectedData(data)
	if err != nil {
		return nil, err
	}
	abortedJobIDs := []int64{}
	abortedReasons := map[int64]string{}
	/**
	We have a limit of 1000 rejected events per api call. And the offset starts from 0. So we are fetching 1000 rejected events every time making the api call and updating the offset accordingly.
	*/
	if rejectResponse.Count > 0 {
		iterations := int(math.Ceil(float64(rejectResponse.TotalResults) / float64(1000)))
		var offset int
		for i := 0; i < iterations; i++ {
			offset = i * 1000
			data.Offset = offset
			if offset != 0 {
				rejectResponse, err = eloqua.service.CheckRejectedData(data)
				if err != nil {
					return nil, err
				}
			}
			for _, val := range rejectResponse.Items {
				uniqueInvalidFields := lo.Intersect(eloqua.uniqueKeys, val.InvalidFields)
				successStatusCode := lo.Intersect(eloqua.successStatusCode, []string{val.StatusCode})
				if len(successStatusCode) != 0 && len(uniqueInvalidFields) == 0 {
					continue
				}
				abortedJobIDs = append(abortedJobIDs, eloqua.jobToCSVMap[val.RecordIndex])
				abortedReasons[eloqua.jobToCSVMap[val.RecordIndex]] = generateErrorString(val)
			}
		}
	}
	left, _ := lo.Difference(jobIDs, abortedJobIDs)
	eventStatMeta := common.EventStatMeta{
		AbortedKeys:    abortedJobIDs,
		AbortedReasons: abortedReasons,
		SucceededKeys:  left,
	}
	return &eventStatMeta, nil
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
