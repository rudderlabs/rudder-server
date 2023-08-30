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
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/samber/lo"
	"golang.org/x/exp/slices"
)

func getEventDetails(file *os.File) (string, string, []string, string, error) {
	_, _ = file.Seek(0, 0)
	scanner := bufio.NewScanner(file)
	scanner.Buffer(nil, 50000*1024)
	if scanner.Scan() {
		line := scanner.Text()
		var data TransformedData
		if err := json.Unmarshal([]byte(line), &data); err != nil {
			return "", "", nil, "", fmt.Errorf("error in unmarshalling data, %v", err)
		}
		if data.Message.Type == "track" && data.Message.CustomObjectId != "" {
			return "track", data.Message.CustomObjectId, getKeys(data.Message.Data), data.Message.IdentifierFieldName, nil
		} else if data.Message.Type == "identify" && data.Message.CustomObjectId == "contacts" {
			return "identify", "", getKeys(data.Message.Data), data.Message.IdentifierFieldName, nil
		} else {
			return "", "", nil, "", fmt.Errorf("unable to find event format")
		}
	}
	return "", "", nil, "", fmt.Errorf("unable to scan data from the file")
}

func getKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func createCSVFile(fields []string, file *os.File, uploadJobInfo *JobInfo) (string, error) {
	_, _ = file.Seek(0, 0)
	scanner := bufio.NewScanner(file)
	scanner.Buffer(nil, 50000*1024)
	localTmpDirName := fmt.Sprintf(`/%s/`, misc.RudderAsyncDestinationLogs)
	tmpDirPath, err := misc.CreateTMPDIR()
	if err != nil {
		return "", err
	}
	path := path.Join(tmpDirPath, localTmpDirName, uuid.NewString())
	csvFilePath := fmt.Sprintf(`%v.csv`, path)
	csvFile, err := os.Create(csvFilePath)
	if err != nil {
		return "", err
	}
	csvWriter := csv.NewWriter(csvFile)
	err = csvWriter.Write(fields)
	if err != nil {
		return "", err
	}
	csvWriter.Flush()
	var line string
	index := 0
	for scanner.Scan() {
		line = scanner.Text()
		var data TransformedData
		if err := json.Unmarshal([]byte(line), &data); err != nil {
			fmt.Println("Error in unmarshalling data")
		}
		var values []string
		for _, field := range fields {
			values = append(values, data.Message.Data[field].(string))
		}
		fileInfo, err := csvFile.Stat()
		if err != nil {
			return "", err
		}
		if fileInfo.Size() > uploadJobInfo.fileSizeLimit {
			left, _ := lo.Difference(uploadJobInfo.importingJobs, uploadJobInfo.succeededJobs)
			uploadJobInfo.failedJobs = append(uploadJobInfo.failedJobs, left...)
			break
		}
		uploadJobInfo.succeededJobs = append(uploadJobInfo.succeededJobs, uploadJobInfo.importingJobs[index])
		index += 1
		err = csvWriter.Write(values)
		if err != nil {
			return "", err
		}
		csvWriter.Flush()
	}
	return csvFilePath, nil
}

func createBodyForImportDefinition(evenType string, fields []string, eloquaFields *Fields, identifierFieldName string) (map[string]interface{}, error) {

	var fieldStatement = make(map[string]string)
	for _, val := range fields {
		for _, val1 := range eloquaFields.Items {
			if val1.InternalName == val {
				fieldStatement[val] = val1.Statement
			}
		}
	}
	if evenType == "identify" {
		return map[string]interface{}{
			"name":                    "Rudderstack-Contact-Import",
			"fields":                  fieldStatement,
			"identifierFieldName":     identifierFieldName,
			"isSyncTriggeredOnImport": false,
		}, nil
	} else if evenType == "track" {
		return map[string]interface{}{
			"name":                    "Rudderstack-CustomObject-Import",
			"updateRule":              "always",
			"fields":                  fieldStatement,
			"identifierFieldName":     identifierFieldName,
			"isSyncTriggeredOnImport": false,
		}, nil
	}

	return nil, fmt.Errorf("unable to create body for import definition")
}

func difference(a, b []int64) []int64 {
	mb := make(map[int64]struct{}, len(b))
	for _, x := range b {
		mb[x] = struct{}{}
	}
	var diff []int64
	for _, x := range a {
		if _, found := mb[x]; !found {
			diff = append(diff, x)
		}
	}
	return diff
}

func parseRejectedData(data *HttpRequestData, importingList []*jobsdb.JobT, service Eloqua) (*common.EventStatMeta, error) {
	jobIDs := []int64{}
	for _, job := range importingList {
		jobIDs = append(jobIDs, job.JobID)
	}
	slices.Sort(jobIDs)
	initialJOBId := jobIDs[0]
	rejectResponse, err := service.CheckRejectedData(data)
	if err != nil {
		return nil, err
	}
	failedJobIDs := []int64{}
	failedReasons := map[int64]string{}
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
				rejectResponse, err = service.CheckRejectedData(data)
				if err != nil {
					return nil, err
				}
			}
			for _, val := range rejectResponse.Items {
				failedJobIDs = append(failedJobIDs, val.RecordIndex+initialJOBId-1)
				failedReasons[val.RecordIndex+initialJOBId-1] = val.Message
			}
		}
	}
	eventStatMeta := common.EventStatMeta{
		FailedKeys:    failedJobIDs,
		FailedReasons: failedReasons,
		SucceededKeys: difference(jobIDs, failedJobIDs),
	}
	return &eventStatMeta, nil
}

func createUploadData(file *os.File, uploaJobInfo *JobInfo) []map[string]interface{} {
	_, _ = file.Seek(0, 0)
	scanner := bufio.NewScanner(file)
	scanner.Buffer(nil, 50000*1024)
	var data1 []map[string]interface{}
	for scanner.Scan() {
		line := scanner.Text()
		var data TransformedData
		if err := json.Unmarshal([]byte(line), &data); err != nil {
			fmt.Println("Error in unmarshalling data")
		}
		data1 = append(data1, data.Message.Data)
	}
	uploaJobInfo.succeededJobs = uploaJobInfo.importingJobs
	uploaJobInfo.failedJobs = []int64{}
	return data1
}
