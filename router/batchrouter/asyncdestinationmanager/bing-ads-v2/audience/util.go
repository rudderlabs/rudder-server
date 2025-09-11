package audience

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"os"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/bing-ads-v2/common"
)

// Upload related utils

// returns the clientID struct
func newClientID(jobID int64, hashedEmail string) ClientID {
	return ClientID{
		JobID:       jobID,
		HashedEmail: hashedEmail,
	}
}

func CreateActionFileTemplate(csvFile *os.File, audienceId, actionType string) (*csv.Writer, error) {
	csvWriter := csv.NewWriter(csvFile)
	err := csvWriter.WriteAll([][]string{
		{"Type", "Status", "Id", "Parent Id", "Client Id", "Modified Time", "Name", "Description", "Scope", "Audience", "Action Type", "Sub Type", "Text"},
		{"Format Version", "", "", "", "", "", "6.0", "", "", "", "", "", ""},
		{"Customer List", "", audienceId, "", "", "", "", "", "", "", actionType, "", ""},
	})
	if err != nil {
		return nil, fmt.Errorf("error in writing csv header: %v", err)
	}
	return csvWriter, nil
}

/*
returns the csv file and zip file path, along with the csv writer that
contains the template of the uploadable file.
*/
func createActionFile(audienceId, actionType string) (*common.ActionFileInfo, error) {
	templateFunc := func(csvFile *os.File) (*csv.Writer, error) {
		return CreateActionFileTemplate(csvFile, audienceId, actionType)
	}

	return common.CreateActionFile(actionType, templateFunc)
}

// populateZipFile only if it is within the file size limit 100mb and row number limit 4000000
// Otherwise event is appended to the failedJobs and will be retried.
func (b *BingAdsBulkUploader) populateZipFile(actionFile *common.ActionFileInfo, audienceId, line string, data Data) error {
	newFileSize := actionFile.FileSize + int64(len(line))
	if newFileSize < b.fileSizeLimit &&
		actionFile.EventCount < b.eventsLimit {
		actionFile.FileSize = newFileSize
		actionFile.EventCount += 1
		for _, uploadData := range data.Message.List {
			clientIdI := newClientID(data.Metadata.JobID, uploadData.HashedEmail)
			clientIdStr := clientIdI.ToString()
			err := actionFile.CSVWriter.Write([]string{"Customer List Item", "", "", audienceId, clientIdStr, "", "", "", "", "", "", "Email", uploadData.HashedEmail})
			if err != nil {
				return err
			}
		}
		actionFile.SuccessfulJobIDs = append(actionFile.SuccessfulJobIDs, data.Metadata.JobID)
	} else {
		actionFile.FailedJobIDs = append(actionFile.FailedJobIDs, data.Metadata.JobID)
	}
	return nil
}

/*
Depending on add, remove and update action we are creating 3 different zip files using this function
It is also returning the list of succeed and failed events lists.
The following map indicates the index->actionType mapping
0-> Add
1-> Remove
2-> Update
*/
func (b *BingAdsBulkUploader) createZipFile(filePath, audienceId string) ([]*common.ActionFileInfo, error) {
	if audienceId == "" {
		return nil, fmt.Errorf("audienceId is empty")
	}
	textFile, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer textFile.Close()
	actionFiles := map[string]*common.ActionFileInfo{}
	for _, actionType := range actionTypes {
		actionFiles[actionType], err = createActionFile(audienceId, actionType)
		if err != nil {
			return nil, err
		}
	}
	scanner := bufio.NewScanner(textFile)
	scanner.Buffer(nil, 50000*1024)
	for scanner.Scan() {
		line := scanner.Text()
		var data Data
		if err := jsonrs.Unmarshal([]byte(line), &data); err != nil {
			return nil, err
		}
		actionFile := actionFiles[data.Message.Action]
		err := b.populateZipFile(actionFile, audienceId, line, data)
		if err != nil {
			return nil, err
		}
	}
	scannerErr := scanner.Err()
	if scannerErr != nil {
		return nil, scannerErr
	}
	actionFilesList := common.ProcessActionFiles(actionFiles, actionTypes[:])
	return actionFilesList, nil
}

/*
records is the output of ReadPollResults function which is in the below format

	[][]string{
		{"Client Id", "Error", "Type"},
		{"1<<>>client1", "error1", "Customer List Error"},
		{"1<<>>client2", "error1", "Customer List Item Error"},
		{"1<<>>client2", "error2", "Customer List Item Error"},
	}

This function processes the CSV records and returns the JobIDs and the corresponding error messages
In the below format:

	map[string]map[string]struct{}{
		"1": {
			"error1": {},
		},
		"2": {
			"error1": {},
			"error2": {},
		},
	}

** we are using map[int64]map[string]struct{} for storing the error messages
** because we want to avoid duplicate error messages
*/
func processPollStatusData(records [][]string) (map[int64]map[string]struct{}, error) {
	config := common.PollStatusConfig{
		IDColumnName:    "Client Id",
		ErrorColumnName: "Error",
		TypeColumnIndex: 0,
		ErrorTypeFilter: "Customer List Item Error",
		ParseIDFunc:     common.ParseClientID,
	}

	return common.ProcessPollStatusData(records, config)
}
