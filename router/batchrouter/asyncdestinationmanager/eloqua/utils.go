package eloqua

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path"

	"github.com/google/uuid"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"golang.org/x/exp/slices"
)

func checkEventType(file *os.File) (string, string, error) {
	file.Seek(0, 0)
	scanner := bufio.NewScanner(file)
	scanner.Buffer(nil, 50000*1024)
	if scanner.Scan() {
		line := scanner.Text()
		var data TransformedData
		if err := json.Unmarshal([]byte(line), &data); err != nil {
			return "", "", fmt.Errorf("error in unmarshalling data, %v", err)
		}
		if data.Message.IdentifierFieldName == "" && data.Message.CustomObjectId != "" && data.Message.MapDataCardsSourceField != "" {
			return "track", data.Message.CustomObjectId, nil
		} else if data.Message.IdentifierFieldName != "" && data.Message.CustomObjectId == "" && data.Message.MapDataCardsSourceField == "" {
			return "identify", "", nil
		} else {
			return "", "", fmt.Errorf("unable to find evnet format")
		}
	}
	return "", "", fmt.Errorf("unable to scan data from the file")
}

func getKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func getFields(file *os.File) []string {
	file.Seek(0, 0)
	scanner := bufio.NewScanner(file)
	scanner.Buffer(nil, 50000*1024)
	var line string
	fmt.Print(scanner.Err())
	if scanner.Scan() {
		line = scanner.Text()
	}
	var data TransformedData
	if err := json.Unmarshal([]byte(line), &data); err != nil {
		fmt.Println("Error in unmarshalling data")
	}
	return getKeys(data.Message.Data)
}
func createCSVFile(fields []string, file *os.File) (string, error) {
	file.Seek(0, 0)
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
	csvWriter := csv.NewWriter(csvFile)
	csvWriter.Write(fields)
	csvWriter.Flush()
	var line string
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
		csvWriter.Write(values)
	}

	csvWriter.Flush()
	return csvFilePath, nil
}

func createBodyForImportDefinition(evenType string, fields []string, eloquaFields *Fields, file *os.File) (map[string]interface{}, error) {
	file.Seek(0, 0)
	scanner := bufio.NewScanner(file)
	scanner.Buffer(nil, 50000*1024)
	data := TransformedData{}
	if scanner.Scan() {
		line := scanner.Text()
		if err := json.Unmarshal([]byte(line), &data); err != nil {
			return nil, fmt.Errorf("error in unmarshalling data while creating body for import definition. %v", err)
		}
	}

	if evenType == "identify" {
		var contactFieldStatement = make(map[string]string)
		for _, val := range fields {
			for _, val1 := range eloquaFields.Items {
				if val1.InternalName == val {
					contactFieldStatement[val] = val1.Statement
				}
			}
		}
		return map[string]interface{}{
			"name":                    "Rudderstack-Contact-Import",
			"fields":                  contactFieldStatement,
			"identifierFieldName":     data.Message.IdentifierFieldName,
			"isSyncTriggeredOnImport": false,
		}, nil
	} else if evenType == "track" {
		var customObjectFieldStatement = make(map[string]string)
		for _, val := range fields {
			for _, val1 := range eloquaFields.Items {
				if val1.InternalName == val {
					customObjectFieldStatement[val] = val1.Statement
				}
			}
		}
		return map[string]interface{}{
			"name":                           "Rudderstack-CustomObject-Import",
			"mapDataCards":                   "true",
			"mapDataCardsEntityField":        "{{Contact.Field(C_EmailAddress)}}",
			"mapDataCardsEntityType":         "Contact",
			"mapDataCardsCaseSensitiveMatch": "false",
			"updateRule":                     "always",
			"fields":                         customObjectFieldStatement,
			"mapDataCardsSourceField":        data.Message.MapDataCardsSourceField,
			"identifierFieldName":            data.Message.MapDataCardsSourceField,
			"isSyncTriggeredOnImport":        false,
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

func parseRejectedData(data *Data, importingList []*jobsdb.JobT) (*common.EventStatMeta, error) {
	jobIDs := []int64{}
	for _, job := range importingList {
		jobIDs = append(jobIDs, job.JobID)
	}
	slices.Sort(jobIDs)
	initialJOBId := jobIDs[0]
	rejectResponse, err := CheckRejectedData(data)
	if err != nil {
		return nil, err
	}
	failedJobIDs := []int64{}
	failedReasons := map[int64]string{}
	if rejectResponse.Count > 0 {
		iterations := rejectResponse.TotalResults / 1000
		var offset int
		for i := 0; i <= iterations; i++ {
			offset = i * 1000
			data.Offset = offset
			if offset != 0 {
				rejectResponse, err = CheckRejectedData(data)
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
