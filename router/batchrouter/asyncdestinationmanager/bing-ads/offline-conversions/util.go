package offline_conversions

import (
	"archive/zip"
	"bufio"
	"crypto/sha256"
	"encoding/csv"
	"encoding/json"
	stdjson "encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-server/utils/misc"
)

func CreateActionFileTemplate(csvFile *os.File, actionType string) (*csv.Writer, error) {
	var err error
	csvWriter := csv.NewWriter(csvFile)
	if actionType == "insert" {
		err = csvWriter.WriteAll([][]string{
			{"Type", "Status", "Id", "Parent Id", "Client Id", "Name", "Conversion Currency Code", "Conversion Name", "Conversion Time", "Conversion Value", "Microsoft Click Id", "Hashed Email Address", "Hashed Phone Number", "External Attribution Credit", "External Attribution Model"},
			{"Format Version", "", "", "", "", "6.0", "", "", "", "", "", "", "", "", ""},
		})
	} else if actionType == "update" {
		err = csvWriter.WriteAll([][]string{
			{"Type", "Adjustment Type", "Client Id", "Id", "Name", "Conversion Name", "Conversion Time", "Adjustment Value", "Microsoft Click Id", "Hashed Email Address", "Hashed Phone Number", "Adjusted Currency Code", "Adjustment Time"},
			{"Format Version", "", "", "", "6.0", "", "", "", "", "", "", "", ""},
		})
	} else {
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
func createActionFile(actionType string) (*ActionFileInfo, error) {
	localTmpDirName := fmt.Sprintf(`/%s/`, misc.RudderAsyncDestinationLogs)
	tmpDirPath, err := misc.CreateTMPDIR()
	if err != nil {
		return nil, err
	}
	path := path.Join(tmpDirPath, localTmpDirName, uuid.NewString())
	csvFilePath := fmt.Sprintf(`%v.csv`, path)
	zipFilePath := fmt.Sprintf(`%v.zip`, path)
	csvFile, err := os.Create(csvFilePath)
	if err != nil {
		return nil, err
	}
	csvWriter, err := CreateActionFileTemplate(csvFile, actionType)
	if err != nil {
		return nil, err
	}
	return &ActionFileInfo{
		Action:      actionType,
		ZipFilePath: zipFilePath,
		CSVFilePath: csvFilePath,
		CSVWriter:   csvWriter,
	}, nil
}

func convertCsvToZip(actionFile *ActionFileInfo) error {
	if actionFile.EventCount == 0 {
		os.Remove(actionFile.CSVFilePath)
		os.Remove(actionFile.ZipFilePath)
		return nil
	}
	zipFile, err := os.Create(actionFile.ZipFilePath)
	if err != nil {
		return err
	}
	defer zipFile.Close()

	zipWriter := zip.NewWriter(zipFile)

	csvFileInZip, err := zipWriter.Create(filepath.Base(actionFile.CSVFilePath))
	if err != nil {
		return err
	}
	csvFile, err := os.Open(actionFile.CSVFilePath)
	if err != nil {
		return err
	}
	if _, err := csvFile.Seek(0, 0); err != nil {
		return err
	}

	if _, err = io.Copy(csvFileInZip, csvFile); err != nil {
		return err
	}

	// Close the ZIP writer
	if err = zipWriter.Close(); err != nil {
		return err
	}
	// Remove the csv file after creating the zip file
	if err = os.Remove(actionFile.CSVFilePath); err != nil {
		return err
	}
	return nil
}

// populateZipFile only if it is within the file size limit 100mb and row number limit 4000000
// Otherwise event is appended to the failedJobs and will be retried.
func (b *BingAdsBulkUploader) populateZipFile(actionFile *ActionFileInfo, line string, data Data) error {
	newFileSize := actionFile.FileSize + int64(len(line))
	fileType := "Offline Conversion"
	if newFileSize < b.fileSizeLimit &&
		actionFile.EventCount < b.eventsLimit {
		actionFile.FileSize = newFileSize
		actionFile.EventCount += 1
		jobId := data.Metadata.JobID
		var fields RecordFields
		unmarshallingErr := json.Unmarshal(data.Message.Fields, &fields)
		if unmarshallingErr != nil {
			return fmt.Errorf("unmarshalling event %w:", unmarshallingErr)
		}

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
func (b *BingAdsBulkUploader) createZipFile(filePath string) ([]*ActionFileInfo, error) {
	textFile, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer textFile.Close()
	actionFiles := map[string]*ActionFileInfo{}
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
		if err := json.Unmarshal([]byte(line), &data); err != nil {
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
	actionFilesList := []*ActionFileInfo{}
	for _, actionType := range actionTypes {
		actionFile := actionFiles[actionType]
		actionFile.CSVWriter.Flush()
		err := convertCsvToZip(actionFile)
		if err != nil {
			actionFile.FailedJobIDs = append(actionFile.FailedJobIDs, actionFile.SuccessfulJobIDs...)
			actionFile.SuccessfulJobIDs = []int64{}
		}
		if actionFile.EventCount > 0 {
			actionFilesList = append(actionFilesList, actionFile)
		}

	}
	return actionFilesList, nil
}

// Poll Related Utils

/*
From the ResultFileUrl, it downloads the zip file and extracts the contents of the zip file
and finally Provides file paths containing error information as an array string
*/
func (b *BingAdsBulkUploader) downloadAndGetUploadStatusFile(ResultFileUrl string) ([]string, error) {
	// the final status file needs to be downloaded
	fileAccessUrl := ResultFileUrl
	modifiedUrl := strings.ReplaceAll(fileAccessUrl, "&amp;", "&")

	// Download the zip file
	fileLoadResp, err := http.Get(modifiedUrl)
	if err != nil {
		b.logger.Errorf("Error downloading zip file: %w", err)
		panic(fmt.Errorf("BRT: Error downloading zip file:. Err: %w", err))
	}
	defer fileLoadResp.Body.Close()

	// Create a temporary file to save the downloaded zip file
	tempFile, err := os.CreateTemp("", fmt.Sprintf("bingads_%s_*.zip", uuid.NewString()))
	if err != nil {
		panic(fmt.Errorf("BRT: Failed creating temporary file. Err: %w", err))
	}
	defer os.Remove(tempFile.Name())

	// Save the downloaded zip file to the temporary file
	_, err = io.Copy(tempFile, fileLoadResp.Body)
	if err != nil {
		panic(fmt.Errorf("BRT: Failed saving zip file. Err: %w", err))
	}
	localTmpDirName := fmt.Sprintf(`/%s/`, misc.RudderAsyncDestinationLogs)
	tmpDirPath, err := misc.CreateTMPDIR()
	if err != nil {
		panic(fmt.Errorf("Error while creating tmp directory Err: %w", err))
	}
	outputDir := path.Join(tmpDirPath, localTmpDirName)
	// Create output directory if it doesn't exist
	_, err = os.Stat(outputDir)
	if os.IsNotExist(err) {
		err = os.MkdirAll(outputDir, 0o755)
		if err != nil {
			panic(fmt.Errorf("Error while creating output directory err: %w", err))
		}
	}

	// Extract the contents of the zip file to the output directory
	filePaths, err := unzip(tempFile.Name(), outputDir)
	return filePaths, err
}

// unzips the file downloaded from bingads, which contains error informations
// of a particular event.
func unzip(zipFile, targetDir string) ([]string, error) {
	var filePaths []string

	r, err := zip.OpenReader(zipFile)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	for _, f := range r.File {
		rc, err := f.Open()
		if err != nil {
			return nil, err
		}
		defer rc.Close()

		// Create the corresponding file in the target directory
		path := filepath.Join(targetDir, uuid.NewString())
		csvFilePath := fmt.Sprintf(`%s.csv`, path)
		if f.FileInfo().IsDir() {
			// Create directories if the file is a directory
			err = os.MkdirAll(csvFilePath, f.Mode())
			if err != nil {
				return nil, err
			}
		} else {
			// Create the file and copy the contents
			file, err := os.Create(csvFilePath)
			if err != nil {
				return nil, err
			}
			defer file.Close()

			_, err = io.Copy(file, rc)
			if err != nil {
				return nil, err
			}

			// Append the file path to the list
			filePaths = append(filePaths, csvFilePath)
		}
	}

	return filePaths, nil
}

/*
ReadPollResults reads the CSV file and returns the records
In the below format (only adding relevant keys)

	[][]string{
		{"Client Id", "Error", "Type"},
		{"1", "error1", "Customer List Error"},
		{"1", "error1", "Customer List Item Error"},
		{"1", "error2", "Customer List Item Error"},
	}
*/
func (b *BingAdsBulkUploader) readPollResults(filePath string) ([][]string, error) {
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
	jobIdIndex := -1
	errorIndex := -1
	typeIndex := 0
	if len(records) > 0 {
		header := records[0]
		for i, column := range header {
			if column == "Id" {
				jobIdIndex = i
			} else if column == "Error" {
				errorIndex = i
			}
		}
	}

	// Declare variables for storing data

	jobIdErrors := make(map[int64]map[string]struct{})

	// Iterate over the remaining rows and filter based on the 'Type' field containing the substring 'Error'
	// The error messages are present on the rows where the corresponding Type column values are "Customer List Error", "Customer List Item Error" etc
	for _, record := range records[1:] {
		rowname := record[typeIndex]
		if typeIndex < len(record) && strings.Contains(rowname, "Error") {
			if jobIdIndex >= 0 && jobIdIndex < len(record) {
				jobId, err := strconv.ParseInt(record[jobIdIndex], 10, 64)
				if err != nil {
					return jobIdErrors, err
				}

				errorSet, ok := jobIdErrors[jobId]
				if !ok {
					errorSet = make(map[string]struct{})
					// making the structure as jobId: [error1, error2]
					jobIdErrors[jobId] = errorSet
				}
				errorSet[record[errorIndex]] = struct{}{}
			}
		}
	}
	return jobIdErrors, nil
}

// GetUploadStats Related utils

// get the list of unique error messages for a particular jobId.
func getAbortedReasons(clientIDErrors map[int64]map[string]struct{}) map[int64]string {
	reasons := make(map[int64]string)
	for key, errors := range clientIDErrors {
		reasons[key] = strings.Join(lo.Keys(errors), commaSeparator)
	}
	return reasons
}

// filtering out failed jobIds from the total array of jobIds
// in order to get jobIds of the successful jobs
func getSuccessJobIDs(failedEventList, initialEventList []int64) []int64 {
	successfulEvents, _ := lo.Difference(initialEventList, failedEventList)
	return successfulEvents
}

/*
This function validates if a `field` is present, not null and have a valid value or not in the `fields“ object
*/
func validateField(fields map[string]interface{}, field string) error {
	val, ok := fields[field]
	if !ok {
		return fmt.Errorf(" %v field not defined", field) // Field not defined
	}
	if val == nil {
		return fmt.Errorf("%v field is null", field) // Field is null
	}
	// Check if the field value is empty for strings
	if reflect.TypeOf(val) != reflect.TypeOf("") || val == "" {
		return fmt.Errorf("%v field is either not string or an empty string", field)
	}
	return nil
}

func calculateHashCode(data string) string {
	// Join the strings into a single string with a separator
	hash := sha256.New()
	hash.Write([]byte(data))
	hashBytes := hash.Sum(nil)
	hashCode := fmt.Sprintf("%x", hashBytes)

	return hashCode
}

func hashFields(input map[string]interface{}) (stdjson.RawMessage, error) {
	// Create a new map to hold the hashed fields
	hashedMap := make(map[string]interface{})

	// Iterate over the input map
	for key, value := range input {
		// Check if the key is "email" or "phone"
		if key == "email" || key == "phone" {
			// Ensure the value is a string before hashing
			if strVal, ok := value.(string); ok {
				hashedMap[key] = calculateHashCode(strVal)
			} else {
				// If not a string, preserve the original value
				hashedMap[key] = value
			}
		} else {
			// Preserve other fields unchanged
			hashedMap[key] = value
		}
	}

	// Convert the resulting map to JSON RawMessage
	result, err := json.Marshal(hashedMap)
	if err != nil {
		return nil, err
	}

	return json.RawMessage(result), nil
}
