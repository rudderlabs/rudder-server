package offline_conversions

import (
	"bufio"
	"crypto/sha256"
	"encoding/csv"
	"fmt"
	"os"
	"strconv"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/bing-ads-v2/common"
)

func CreateActionFileTemplate(csvFile *os.File, actionType string) (*csv.Writer, error) {
	var err error
	csvWriter := csv.NewWriter(csvFile)
	switch actionType {
	case "insert":
		err = csvWriter.WriteAll([][]string{
			{"Type", "Status", "Id", "Parent Id", "Client Id", "Name", "Conversion Currency Code", "Conversion Name", "Conversion Time", "Conversion Value", "Microsoft Click Id", "Hashed Email Address", "Hashed Phone Number", "External Attribution Credit", "External Attribution Model"},
			{"Format Version", "", "", "", "", "6.0", "", "", "", "", "", "", "", "", ""},
		})
	case "update":
		err = csvWriter.WriteAll([][]string{
			{"Type", "Adjustment Type", "Client Id", "Id", "Name", "Conversion Name", "Conversion Time", "Adjustment Value", "Microsoft Click Id", "Hashed Email Address", "Hashed Phone Number", "Adjusted Currency Code", "Adjustment Time"},
			{"Format Version", "", "", "", "6.0", "", "", "", "", "", "", "", ""},
		})
	default:
		// For deleting conversion
		err = csvWriter.WriteAll([][]string{
			{"Type", "Adjustment Type", "Client Id", "Id", "Name", "Conversion Name", "Conversion Time", "Microsoft Click Id", "Hashed Email Address", "Hashed Phone Number", "Adjustment Time"},
			{"Format Version", "", "", "", "6.0", "", "", "", "", "", ""},
		})
	}
	if err != nil {
		return nil, fmt.Errorf("error in writing csv header: %v", err)
	}

	return csvWriter, nil
}

// Upload related utils

/*
returns the csv file and zip file path, along with the csv writer that
contains the template of the uploadable file.
*/
func createActionFile(actionType string) (*common.ActionFileInfo, error) {
	templateFunc := func(csvFile *os.File) (*csv.Writer, error) {
		return CreateActionFileTemplate(csvFile, actionType)
	}

	return common.CreateActionFile(actionType, templateFunc)
}

// populateZipFile only if it is within the file size limit 100mb and row number limit 4000000
// Otherwise event is appended to the failedJobs and will be retried.
func (b *BingAdsBulkUploader) populateZipFile(actionFile *common.ActionFileInfo, line string, data Data) error {
	newFileSize := actionFile.FileSize + int64(len(line))
	fileType := "Offline Conversion"
	if newFileSize < b.fileSizeLimit &&
		actionFile.EventCount < b.eventsLimit {
		actionFile.FileSize = newFileSize
		actionFile.EventCount += 1
		jobId := data.Metadata.JobID
		fields := data.Message.Fields

		var err error
		switch data.Message.Action {
		case "insert":
			err = actionFile.CSVWriter.Write([]string{fileType, "", strconv.FormatInt(jobId, 10), "", "", "", fields.ConversionCurrencyCode, fields.ConversionName, fields.ConversionTime, fields.ConversionValue, fields.MicrosoftClickId, fields.Email, fields.Phone, fields.ExternalAttributionCredit, fields.ExternalAttributionModel})
		case "update":
			err = actionFile.CSVWriter.Write([]string{fileType, "Restate", "", strconv.FormatInt(jobId, 10), "", fields.ConversionName, fields.ConversionTime, fields.ConversionValue, fields.MicrosoftClickId, fields.Email, fields.Phone, fields.ConversionCurrencyCode, fields.ConversionAdjustedTime})
		case "delete":
			err = actionFile.CSVWriter.Write([]string{fileType, "Retract", "", strconv.FormatInt(jobId, 10), "", fields.ConversionName, fields.ConversionTime, fields.MicrosoftClickId, fields.Email, fields.Phone, fields.ConversionAdjustedTime})
		default:
			return fmt.Errorf("%v action is invalid", data.Message.Action)
		}
		if err != nil {
			return err
		}
		actionFile.SuccessfulJobIDs = append(actionFile.SuccessfulJobIDs, data.Metadata.JobID)
	} else {
		actionFile.FailedJobIDs = append(actionFile.FailedJobIDs, data.Metadata.JobID)
	}
	return nil
}

/*
Depending on insert, delete and update action we are creating 3 different zip files using this function
It is also returning the list of succeed and failed events lists.
The following is the list of actions
-> Insert a conversion
-> Delete a conversion
-> Update a conversion
*/
func (b *BingAdsBulkUploader) createZipFile(filePath string) ([]*common.ActionFileInfo, error) {
	textFile, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer textFile.Close()
	actionFiles := map[string]*common.ActionFileInfo{}
	for _, actionType := range actionTypes {
		actionFiles[actionType], err = createActionFile(actionType)
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
		err := b.populateZipFile(actionFile, line, data)
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
		{"1", "error1", "Customer List Error"},
		{"1", "error1", "Customer List Item Error"},
		{"1", "error2", "Customer List Item Error"},
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
		IDColumnName:    "Id",
		ErrorColumnName: "Error",
		TypeColumnIndex: 0,
		ErrorTypeFilter: "Error",
		ParseIDFunc:     common.ParseJobID,
	}

	return common.ProcessPollStatusData(records, config)
}

// GetUploadStats Related utils

func calculateHashCode(data string) string {
	// Join the strings into a single string with a separator
	hash := sha256.New()
	hash.Write([]byte(data))
	hashBytes := hash.Sum(nil)
	hashCode := fmt.Sprintf("%x", hashBytes)

	return hashCode
}
