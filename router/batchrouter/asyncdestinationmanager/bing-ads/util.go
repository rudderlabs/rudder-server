package bingads

import (
	"archive/zip"
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

// Upload related utils

// returns the clientID struct
func newClientID(jobID string, hashedEmail string) ClientID {
	return ClientID{
		JobID:       jobID,
		HashedEmail: hashedEmail,
	}
}

// returns the string representation of the clientID struct which is of format
// jobId<<>>hashedEmail
func (c *ClientID) ToString() string {
	return fmt.Sprintf("%s%s%s", c.JobID, clientIDSeparator, c.HashedEmail)
}

/*
returns the csv file and zip file path, along with the csv writer that
contains the template of the uploadable file.
*/
func createActionFile(audienceId, actionType string) (*ActionFileInfo, error) {
	localTmpDirName := fmt.Sprintf(`/%s/`, misc.RudderAsyncDestinationLogs)
	uuid := uuid.New()
	tmpDirPath, _ := misc.CreateTMPDIR()
	path := path.Join(tmpDirPath, localTmpDirName, uuid.String())
	csvFilePath := fmt.Sprintf(`%v.csv`, path)
	zipFilePath := fmt.Sprintf(`%v.zip`, path)
	csvFile, err := os.Create(csvFilePath)
	if err != nil {
		return nil, err
	}
	csvWriter := csv.NewWriter(csvFile)
	_ = csvWriter.Write([]string{"Type", "Status", "Id", "Parent Id", "Client Id", "Modified Time", "Name", "Description", "Scope", "Audience", "Action Type", "Sub Type", "Text"})
	_ = csvWriter.Write([]string{"Format Version", "", "", "", "", "", "6.0", "", "", "", "", "", ""})
	_ = csvWriter.Write([]string{"Customer List", "", audienceId, "", "", "", "", "", "", "", actionType, "", ""})
	return &ActionFileInfo{
		Action:      actionType,
		ZipFilePath: zipFilePath,
		CSVFilePath: csvFilePath,
		CSVWriter:   csvWriter,
	}, nil
}

func convertCsvToZip(actionFile *ActionFileInfo) error {
	csvFile, err := os.Open(actionFile.CSVFilePath)
	if err != nil {
		return err
	}
	zipFile, err := os.Create(actionFile.ZipFilePath)
	if actionFile.EventCount == 0 {
		os.Remove(actionFile.CSVFilePath)
		os.Remove(actionFile.ZipFilePath)
		return nil
	}
	if err != nil {
		return err
	}
	defer zipFile.Close()

	zipWriter := zip.NewWriter(zipFile)

	csvFileInZip, err := zipWriter.Create(filepath.Base(actionFile.ZipFilePath))
	if err != nil {
		return err
	}

	_, err = csvFile.Seek(0, 0)
	if err != nil {
		return err
	}
	_, err = io.Copy(csvFileInZip, csvFile)
	if err != nil {
		return err
	}

	// Close the ZIP writer
	err = zipWriter.Close()
	if err != nil {
		return err
	}
	// Remove the csv file after creating the zip file
	err = os.Remove(actionFile.CSVFilePath)
	if err != nil {
		return err
	}
	return nil
}

/*
Populates the csv file only if it is within the file size limit 100mb and row number limit 4000000
Otherwise event is appended to the failedJobs and will be retried.
*/
func populateZipFile(actionFile *ActionFileInfo, audienceId, line, destName string, data Data) {
	newFileSize := actionFile.FileSize + len(line)
	if int64(newFileSize) < common.GetBatchRouterConfigInt64("MaxUploadLimit", destName, 100*bytesize.MB) && actionFile.EventCount < 4000000 {
		actionFile.FileSize = newFileSize
		actionFile.EventCount += 1
		for _, uploadData := range data.Message.List {
			clientIdI := newClientID(strconv.FormatInt(data.Metadata.JobID, 10), uploadData.HashedEmail)
			clientIdStr := clientIdI.ToString()
			actionFile.CSVWriter.Write([]string{"Customer List Item", "", "", audienceId, clientIdStr, "", "", "", "", "", "", "Email", uploadData.HashedEmail})
		}
		actionFile.SuccessfulJobIDs = append(actionFile.SuccessfulJobIDs, data.Metadata.JobID)
	} else {
		actionFile.FailedJobIDs = append(actionFile.FailedJobIDs, data.Metadata.JobID)
	}
}

/*
Depending on add, remove and update action we are creating 3 different zip files using this function
It is also returning the list of succeed and failed events lists.
The following map indicates the index->actionType mapping
0-> Add
1-> Remove
2-> Update
*/
func (b *BingAdsBulkUploader) CreateZipFile(filePath, audienceId string) ([]*ActionFileInfo, error) {
	textFile, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer textFile.Close()

	if err != nil {
		return nil, err
	}
	actionFiles := map[string]*ActionFileInfo{}
	for _, actionType := range actionTypes {
		actionFiles[actionType], err = createActionFile(audienceId, actionType)
		if err != nil {
			return nil, err
		}
	}
	scanner := bufio.NewScanner(textFile)
	for scanner.Scan() {
		line := scanner.Text()
		var data Data
		err := json.Unmarshal([]byte(line), &data)
		if err != nil {
			return nil, err
		}

		payloadSizeStat := stats.Default.NewTaggedStat("payload_size", stats.HistogramType,
			map[string]string{
				"module":   "batch_router",
				"destType": b.destName,
			})
		payloadSizeStat.Observe(float64(len(data.Message.List)))
		actionFile := actionFiles[data.Message.Action]
		populateZipFile(actionFile, audienceId, line, b.destName, data)

	}
	for _, actionType := range actionTypes {
		actionFile := actionFiles[actionType]
		actionFile.CSVWriter.Flush()
		convertCsvToZip(actionFile)
	}
	// Create the ZIP file and add the CSV file to it
	return lo.Values(actionFiles), nil
}

// Poll Related Utils

/*
From the ResultFileUrl, it downloads the zip file and extracts the contents of the zip file
and finally Provides file paths containing error information as an array string
*/
func (b *BingAdsBulkUploader) DownloadAndGetUploadStatusFile(ResultFileUrl string) ([]string, error) {
	// the final status file needs to be downloaded
	fileAccessUrl := ResultFileUrl
	modifiedUrl := strings.ReplaceAll(fileAccessUrl, "amp;", "")
	outputDir := "/tmp"
	// Create output directory if it doesn't exist
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		panic(fmt.Errorf("error creating output directory: err: %w", err))
	}

	// Download the zip file
	fileLoadResp, err := http.Get(modifiedUrl)
	if err != nil {
		b.logger.Errorf("Error downloading zip file: %w", err)
		panic(fmt.Errorf("BRT: Failed creating temporary file. Err: %w", err))
	}
	defer fileLoadResp.Body.Close()

	uuid := uuid.New()
	// Create a temporary file to save the downloaded zip file
	tempFile, err := os.CreateTemp("", fmt.Sprintf("bingads_%s_*.zip", uuid.String()))
	if err != nil {
		panic(fmt.Errorf("BRT: Failed creating temporary file. Err: %w", err))
	}
	defer os.Remove(tempFile.Name())

	// Save the downloaded zip file to the temporary file
	_, err = io.Copy(tempFile, fileLoadResp.Body)
	if err != nil {
		panic(fmt.Errorf("BRT: Failed saving zip file. Err: %w", err))
	}
	// Extract the contents of the zip file to the output directory
	filePaths, err := Unzip(tempFile.Name(), outputDir)
	return filePaths, err
}

// unzips the file downloaded from bingads, which contains error informations
// of a particular event.
func Unzip(zipFile, targetDir string) ([]string, error) {
	var filePaths []string

	r, err := zip.OpenReader(zipFile)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	for _, f := range r.File {
		// Open each file in the zip archive
		rc, err := f.Open()
		if err != nil {
			return nil, err
		}
		defer rc.Close()

		// Create the corresponding file in the target directory
		path := filepath.Join(targetDir, f.Name)
		if f.FileInfo().IsDir() {
			// Create directories if the file is a directory
			err = os.MkdirAll(path, f.Mode())
			if err != nil {
				return nil, err
			}
		} else {
			// Create the file and copy the contents
			file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())
			if err != nil {
				return nil, err
			}
			defer file.Close()

			_, err = io.Copy(file, rc)
			if err != nil {
				return nil, err
			}

			// Append the file path to the list
			filePaths = append(filePaths, path)
		}
	}

	return filePaths, nil
}

/*
ReadPollResults reads the CSV file and returns the records
In the below format (only adding relevant keys)

	[][]string{
		{"Client Id", "Error", "Type"},
		{"1<<>>client1", "error1", "Customer List Error"},
		{"1<<>>client2", "error1", "Customer List Item Error"},
		{"1<<>>client2", "error2", "Customer List Item Error"},
	}
*/
func (b *BingAdsBulkUploader) ReadPollResults(filePath string) ([][]string, error) {
	// Open the CSV file
	file, err := os.Open(filePath)
	if err != nil {
		b.logger.Errorf("Error opening the CSV file: %w", err)
		return nil, err
	}
	// defer file.Close() and remove
	defer func() {
		closeErr := file.Close()
		if closeErr != nil {
			b.logger.Errorf("Error closing the CSV file: %w", err)
			if err == nil {
				err = closeErr
			}
		}
		// remove the file after the response has been written
		removeErr := os.Remove(filePath)
		if removeErr != nil {
			b.logger.Errorf("Error removing the CSV file: %w", removeErr)
			if err == nil {
				err = removeErr
			}

		}
	}()
	// Create a new CSV reader
	reader := csv.NewReader(file)

	// Read all records from the CSV file
	records, err := reader.ReadAll()
	if err != nil {
		b.logger.Errorf("Error reading CSV: %w", err)
		return nil, err
	}
	return records, nil
}

// converting the string clientID to ClientID struct

func newClientIDFromString(clientID string) (*ClientID, error) {
	clientIDParts := strings.Split(clientID, clientIDSeparator)
	if len(clientIDParts) != 2 {
		return nil, fmt.Errorf("invalid client id: %s", clientID)
	}
	return &ClientID{
		JobID:       clientIDParts[0],
		HashedEmail: clientIDParts[1],
	}, nil
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

** we are using map[string]map[string]struct{} for storing the error messages
** because we want to avoid duplicate error messages
*/
func ProcessPollStatusData(records [][]string) (map[string]map[string]struct{}, error) {
	clientIDIndex := -1
	errorIndex := -1
	typeIndex := 0
	if len(records) > 0 {
		header := records[0]
		for i, column := range header {
			if column == "Client Id" {
				clientIDIndex = i
			} else if column == "Error" {
				errorIndex = i
			}
		}
	}

	// Declare variables for storing data

	clientIDErrors := make(map[string]map[string]struct{})

	// Iterate over the remaining rows and filter based on the 'Type' field containing the substring 'Error'
	// The error messages are present on the rows where the corresponding Type column values are "Customer List Error", "Customer List Item Error" etc
	for _, record := range records[1:] {
		rowname := string(record[typeIndex])
		if typeIndex < len(record) && strings.Contains(rowname, "Error") {
			if clientIDIndex >= 0 && clientIDIndex < len(record) {
				// expecting the client ID is present as jobId<<>>clientId
				clientId, err := newClientIDFromString(record[clientIDIndex])
				if err != nil {
					return nil, err
				}
				errorSet, ok := clientIDErrors[clientId.JobID]
				if !ok {
					errorSet = make(map[string]struct{})
					// making the structure as jobId: [error1, error2]
					clientIDErrors[clientId.JobID] = errorSet
				}
				errorSet[record[errorIndex]] = struct{}{}

			}
		}
	}
	return clientIDErrors, nil
}

// GetUploadStats Related utils

// create array of failed job Ids from clientIDErrors
func GetFailedJobIDs(clientIDErrors map[string]map[string]struct{}) []int64 {
	keys := make([]int64, 0, len(clientIDErrors))
	for key := range clientIDErrors {
		intKey, _ := strconv.ParseInt(key, 10, 64)
		keys = append(keys, intKey)
	}
	return keys
}

// get the list of unique error messages for a particular jobId.
func GetFailedReasons(clientIDErrors map[string]map[string]struct{}) map[string]string {
	reasons := make(map[string]string)
	for key, errors := range clientIDErrors {
		errorList := make([]string, 0, len(errors))
		for k := range errors {
			errorList = append(errorList, k)
		}
		reasons[key] = strings.Join(errorList, ", ")
	}
	return reasons
}

// filtering out failed jobIds from the total array of jobIds
// in order to get jobIds of the successful jobs
func GetSuccessJobIDs(failedEventList, initialEventList []int64) []int64 {
	successfulEvents, _ := lo.Difference(initialEventList, failedEventList)
	return successfulEvents
}

// If any one importId is in progress, then the overall completion status is false
func generateCompletionStatus(array []bool) bool {
	for _, element := range array {
		if !element {
			return false
		}
	}
	return true
}
