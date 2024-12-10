package clevertapSegment

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/uuid"
	jsoniter "github.com/json-iterator/go"

	"github.com/rudderlabs/rudder-go-kit/stats"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

func (u *ClevertapBulkUploader) PopulateCsvFile(actionFile *ActionFileInfo, line string, data Data) error {
	newFileSize := actionFile.FileSize + int64(len(line))
	if newFileSize < u.fileSizeLimit {
		actionFile.FileSize = newFileSize
		actionFile.EventCount += 1

		// Unmarshal Properties into a map of json.RawMessage
		var fields map[string]interface{}
		if err := jsoniter.Unmarshal(data.Message.Fields, &fields); err != nil {
			return err
		}

		// Check for presence of "i" and "g" values
		if valueG, okG := fields["g"]; okG {
			// If "g" exists, prioritize it and omit "i"
			csvRow := []string{"g", fmt.Sprintf("%v", valueG)} // Type: g
			if err := actionFile.CSVWriter.Write(csvRow); err != nil {
				return err
			}
		} else if valueI, okI := fields["i"]; okI {
			// Write "i" value only if "g" does not exist
			csvRow := []string{"i", fmt.Sprintf("%v", valueI)} // Type: i
			if err := actionFile.CSVWriter.Write(csvRow); err != nil {
				return err
			}
		}

		// Write the CSV header only once
		if actionFile.EventCount == 1 {
			// Fixed headers
			headers := []string{"Type", "Identity"}
			if err := actionFile.CSVWriter.Write(headers); err != nil {
				return err
			}
		}
		actionFile.CSVWriter.Flush()
		actionFile.SuccessfulJobIDs = append(actionFile.SuccessfulJobIDs, data.Metadata.JobID)
	} else {
		// fmt.println("size exceeding")
		actionFile.FailedJobIDs = append(actionFile.FailedJobIDs, data.Metadata.JobID)
	}
	return nil
}

func createCSVWriter(fileName string) (*ActionFileInfo, error) {
	// Open or create the file where the CSV will be written
	file, err := os.Create(fileName)
	if err != nil {
		return nil, fmt.Errorf("failed to create file: %v", err)
	}

	// Create a new CSV writer using the file
	csvWriter := csv.NewWriter(file)

	// Return the ActionFileInfo struct with the CSV writer, file, and file path
	return &ActionFileInfo{
		CSVWriter:   csvWriter,
		File:        file,
		CSVFilePath: fileName,
	}, nil
}

func (u *ClevertapBulkUploader) createCSVFile(existingFilePath string) (*ActionFileInfo, error) {
	// Create a temporary directory using misc.CreateTMPDIR
	tmpDirPath, err := misc.CreateTMPDIR()
	if err != nil {
		return nil, fmt.Errorf("failed to create temporary directory: %v", err)
	}

	// Define a local directory name within the temp directory
	localTmpDirName := fmt.Sprintf("/%s/", misc.RudderAsyncDestinationLogs)

	// Combine the temporary directory with the local directory name and generate a unique file path
	path := filepath.Join(tmpDirPath, localTmpDirName, uuid.NewString())
	csvFilePath := fmt.Sprintf("%v.csv", path)

	// Initialize the CSV writer with the generated file path
	actionFile, err := createCSVWriter(csvFilePath)
	if err != nil {
		return nil, err
	}
	defer actionFile.File.Close() // Ensure the file is closed when done

	// Store the CSV file path in the ActionFileInfo struct
	actionFile.CSVFilePath = csvFilePath

	// Create a scanner to read the existing file line by line
	existingFile, err := os.Open(existingFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open existing file")
	}
	defer existingFile.Close()

	scanner := bufio.NewScanner(existingFile)
	scanner.Buffer(nil, 50000*1024) // Adjust the buffer size if necessary

	for scanner.Scan() {
		line := scanner.Text()
		var data Data
		if err := jsoniter.Unmarshal([]byte(line), &data); err != nil {
			// Collect the failed job ID only if it's valid
			if data.Metadata.JobID != 0 { // Check if JobID is not zero
				actionFile.FailedJobIDs = append(actionFile.FailedJobIDs, data.Metadata.JobID)
			}
			continue
		}

		// Calculate the payload size and observe it
		payloadSizeStat := u.statsFactory.NewTaggedStat("payload_size", stats.HistogramType,
			map[string]string{
				"module":   "batch_router",
				"destType": u.destName,
			})
		payloadSizeStat.Observe(float64(len(data.Message.Fields)))

		// Populate the CSV file and collect success/failure job IDs
		err := u.PopulateCsvFile(actionFile, line, data)
		if err != nil {
			// Collect the failed job ID only if it's valid
			if data.Metadata.JobID != 0 { // Check if JobID is not zero
				actionFile.FailedJobIDs = append(actionFile.FailedJobIDs, data.Metadata.JobID)
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error while scanning file: %v", err)
	}

	// After processing, calculate the final file size
	fileInfo, err := os.Stat(actionFile.CSVFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve file info: %v", err)
	}
	actionFile.FileSize = fileInfo.Size()

	return actionFile, nil
}

// GetCleverTapEndpoint returns the API endpoint for the given region
func (u *ClevertapServiceImpl) getCleverTapEndpoint(region string) (string, error) {
	// Mapping of regions to endpoints
	endpoints := map[string]string{
		"IN":        "in1.api.clevertap.com",
		"SINGAPORE": "sg1.api.clevertap.com",
		"US":        "us1.api.clevertap.com",
		"INDONESIA": "aps3.api.clevertap.com",
		"UAE":       "mec1.api.clevertap.com",
		"EU":        "api.clevertap.com",
	}

	// Normalize the region input to uppercase for case-insensitivity
	region = strings.ToUpper(region)

	// Check if the region exists in the map
	if endpoint, exists := endpoints[region]; exists {
		return endpoint, nil
	}

	// Return an error if the region is not recognized
	return "", fmt.Errorf("unknown region: %s", region)
}

func (u *ClevertapServiceImpl) getBulkApi(destConfig DestinationConfig) *ClevertapServiceImpl {
	endpoint, err := u.getCleverTapEndpoint(destConfig.Region)
	if err != nil {
		return nil
	}
	return &ClevertapServiceImpl{
		BulkApi:   fmt.Sprintf("https://%s/get_custom_list_segment_url", endpoint),
		NotifyApi: fmt.Sprintf("https://%s/upload_custom_list_segment_completed", endpoint),
	}
}

// Function to convert *backendconfig.Connection to ConnectionConfig using marshal and unmarshal
func (u *ClevertapServiceImpl) convertToConnectionConfig(conn *backendconfig.Connection) (*ConnectionConfig, error) {
	if conn == nil {
		return nil, fmt.Errorf("connection is nil")
	}

	// Marshal the backendconfig.Connection to JSON
	data, err := json.Marshal(conn)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal connection: %w", err)
	}

	// Unmarshal the JSON into ConnectionConfig
	var connConfig ConnectionConfig
	err = json.Unmarshal(data, &connConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal to ConnectionConfig: %w", err)
	}

	// Set default SenderName if it is empty
	if connConfig.Config.Destination.SenderName == "" {
		connConfig.Config.Destination.SenderName = DEFAULT_SENDER_NAME
	}

	return &connConfig, nil
}
