package common

import (
	"archive/zip"
	"encoding/csv"
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

	"github.com/rudderlabs/rudder-server/utils/misc"
)

// get the list of unique error messages for a particular jobId.
func GetAbortedReasons(clientIDErrors map[int64]map[string]struct{}) map[int64]string {
	reasons := make(map[int64]string)
	for key, errors := range clientIDErrors {
		reasons[key] = strings.Join(lo.Keys(errors), CommaSeparator)
	}
	return reasons
}

// filtering out failed jobIds from the total array of jobIds
// in order to get jobIds of the successful jobs
func GetSuccessJobIDs(failedEventList, initialEventList []int64) []int64 {
	successfulEvents, _ := lo.Difference(initialEventList, failedEventList)
	return successfulEvents
}

func ConvertCSVToZip(actionFile *ActionFileInfo) error {
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

// Unzip extracts the contents of a zip file into targetDir
// and returns a list of extracted file paths.
func Unzip(zipFile, targetDir string) ([]string, error) {
	if err := os.MkdirAll(targetDir, 0o755); err != nil {
		return nil, err
	}

	r, err := zip.OpenReader(zipFile)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	var filePaths []string

	for _, f := range r.File {
		path := filepath.Join(targetDir, f.Name)

		// ZipSlip protection
		if !strings.HasPrefix(path, filepath.Clean(targetDir)+string(os.PathSeparator)) {
			return nil, fmt.Errorf("illegal file path: %s", path)
		}

		// Always ensure parent directory exists (works for both files & dirs)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return nil, err
		}

		// Skip if it's just a directory entry
		if f.FileInfo().IsDir() {
			continue
		}

		if err := extractFile(f, path); err != nil {
			return nil, err
		}
		filePaths = append(filePaths, path)
	}

	return filePaths, nil
}

func extractFile(f *zip.File, path string) error {
	src, err := f.Open()
	if err != nil {
		return err
	}
	defer src.Close()

	dst, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, f.Mode())
	if err != nil {
		return err
	}
	defer dst.Close()

	_, err = io.Copy(dst, src)
	return err
}

func DownloadAndGetUploadStatusFile(destinationName, resultFileURL string) ([]string, error) {
	// Replace &amp; with &
	modifiedURL := strings.ReplaceAll(resultFileURL, "&amp;", "&")

	// Download the zip file
	resp, err := http.Get(modifiedURL)
	if err != nil {
		return nil, fmt.Errorf("failed downloading zip file: %w", err)
	}
	defer resp.Body.Close()

	// Ensure base tmp dir exists
	tmpDirPath, err := misc.CreateTMPDIR()
	if err != nil {
		return nil, fmt.Errorf("failed creating tmp directory: %w", err)
	}

	// Create a temporary zip file inside our tmp dir
	tempFile, err := os.CreateTemp(tmpDirPath, fmt.Sprintf("%s_%s_*.zip", destinationName, uuid.NewString()))
	if err != nil {
		return nil, fmt.Errorf("failed creating temporary zip file: %w", err)
	}
	defer os.Remove(tempFile.Name())

	// Save zip content to temp file
	if _, err = io.Copy(tempFile, resp.Body); err != nil {
		return nil, fmt.Errorf("failed saving zip file: %w", err)
	}

	// Create a unique output directory under RudderAsyncDestinationLogs
	outputDir := path.Join(
		tmpDirPath,
		misc.RudderAsyncDestinationLogs,
		uuid.NewString(),
	)
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return nil, fmt.Errorf("failed creating output directory: %w", err)
	}

	// Extract contents
	filePaths, err := Unzip(tempFile.Name(), outputDir)
	if err != nil {
		return nil, fmt.Errorf("failed extracting zip file: %w", err)
	}

	return filePaths, nil
}

// ReadAndCleanupCSV opens a CSV file, reads all records,
// then closes and removes the file. Caller gets [][]string and error.
func ReadAndCleanupCSV(filePath string) ([][]string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}

	var retErr error
	defer func() {
		if closeErr := file.Close(); closeErr != nil && retErr == nil {
			retErr = closeErr
		}
		if removeErr := os.Remove(filePath); removeErr != nil && retErr == nil {
			retErr = removeErr
		}
	}()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	return records, retErr
}

// CreateActionFile creates a CSV file and zip file path along with the CSV writer
// that contains the template of the uploadable file. It accepts a template function
// to allow different packages to provide their own CSV template logic.
func CreateActionFile(actionType string, templateFunc func(*os.File) (*csv.Writer, error)) (*ActionFileInfo, error) {
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
	csvWriter, err := templateFunc(csvFile)
	if err != nil {
		return nil, err
	}
	return &ActionFileInfo{
		CSVFilePath: csvFilePath,
		ZipFilePath: zipFilePath,
		CSVWriter:   csvWriter,
		Action:      actionType,
	}, nil
}

// PollStatusConfig holds configuration for processing poll status data
type PollStatusConfig struct {
	IDColumnName    string                      // Column name for ID (e.g., "Client Id", "Id")
	ErrorColumnName string                      // Column name for error (e.g., "Error")
	TypeColumnIndex int                         // Index of type column (usually 0)
	ErrorTypeFilter string                      // Filter for error types (e.g., "Customer List Item Error", "Error")
	ParseIDFunc     func(string) (int64, error) // Function to parse ID from string
}

// ProcessPollStatusData processes CSV records and returns job IDs with corresponding error messages
// This is a common utility that can handle both audience and offline-conversions use cases
func ProcessPollStatusData(records [][]string, config PollStatusConfig) (map[int64]map[string]struct{}, error) {
	idIndex := -1
	errorIndex := -1

	if len(records) == 0 {
		return make(map[int64]map[string]struct{}), nil
	}

	header := records[0]
	for i, column := range header {
		switch column {
		case config.IDColumnName:
			idIndex = i
		case config.ErrorColumnName:
			errorIndex = i
		}
	}

	// Validate required columns are found
	if idIndex == -1 {
		return nil, fmt.Errorf("required column '%s' not found in header", config.IDColumnName)
	}
	if errorIndex == -1 {
		return nil, fmt.Errorf("required column '%s' not found in header", config.ErrorColumnName)
	}

	// Initialize result map
	idErrors := make(map[int64]map[string]struct{})

	// Iterate over the remaining rows and filter based on the error type filter
	for _, record := range records[1:] {
		if config.TypeColumnIndex >= len(record) {
			continue
		}

		rowType := record[config.TypeColumnIndex]
		if strings.Contains(rowType, config.ErrorTypeFilter) {
			if idIndex >= 0 && idIndex < len(record) {
				// Parse the ID using the provided function
				id, err := config.ParseIDFunc(record[idIndex])
				if err != nil {
					return nil, err
				}

				errorSet, ok := idErrors[id]
				if !ok {
					errorSet = make(map[string]struct{})
					idErrors[id] = errorSet
				}
				errorSet[record[errorIndex]] = struct{}{}
			}
		}
	}

	return idErrors, nil
}

// ParseClientID parses client ID in format "jobId<<>>clientId" for audience use case
func ParseClientID(clientID string) (int64, error) {
	clientIDParts := strings.Split(clientID, "<<>>")
	if len(clientIDParts) != 2 {
		return 0, fmt.Errorf("invalid client id format: %s", clientID)
	}

	// Check if job ID part is empty
	if clientIDParts[0] == "" {
		return 0, fmt.Errorf("invalid job id in clientId: %s", clientID)
	}

	// Check if client ID part is empty
	if clientIDParts[1] == "" {
		return 0, fmt.Errorf("invalid job id in clientId: %s", clientID)
	}

	jobID, err := strconv.ParseInt(clientIDParts[0], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid job id in clientId: %s", clientID)
	}

	return jobID, nil
}

// ParseJobID parses job ID directly as integer for offline-conversions use case
func ParseJobID(jobID string) (int64, error) {
	return strconv.ParseInt(jobID, 10, 64)
}

// ProcessActionFiles processes action files by flushing CSV writers, converting to zip,
// and handling errors by moving successful job IDs to failed job IDs on conversion failure
func ProcessActionFiles(actionFiles map[string]*ActionFileInfo, actionTypes []string) []*ActionFileInfo {
	actionFilesList := []*ActionFileInfo{}
	for _, actionType := range actionTypes {
		actionFile := actionFiles[actionType]
		actionFile.CSVWriter.Flush()
		err := ConvertCSVToZip(actionFile)
		if err != nil {
			actionFile.FailedJobIDs = append(actionFile.FailedJobIDs, actionFile.SuccessfulJobIDs...)
			actionFile.SuccessfulJobIDs = []int64{}
		}
		if actionFile.EventCount > 0 {
			actionFilesList = append(actionFilesList, actionFile)
		}
	}
	return actionFilesList
}
