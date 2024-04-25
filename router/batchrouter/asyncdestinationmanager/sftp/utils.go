package sftp

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	sftp "github.com/rudderlabs/rudder-go-kit/sftp"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	"github.com/tidwall/gjson"
)

// createSSHConfig creates SSH configuration based on destination
func createSSHConfig(destination *backendconfig.DestinationT) (*sftp.SSHConfig, error) {
	destConfigJSON, err := json.Marshal(destination.Config)
	if err != nil {
		return nil, err
	}

	authMethod := gjson.Get(string(destConfigJSON), "authMethod").String()
	username := gjson.Get(string(destConfigJSON), "username").String()
	host := gjson.Get(string(destConfigJSON), "host").String()
	port := int(gjson.Get(string(destConfigJSON), "port").Int())
	password := gjson.Get(string(destConfigJSON), "password").String()
	privateKey := gjson.Get(string(destConfigJSON), "privateKey").String()

	if authMethod == "passwordAuth" && password == "" {
		return nil, errors.New("password is required for password authentication")
	}

	if authMethod == "keyAuth" && privateKey == "" {
		return nil, errors.New("private key is required for key authentication")
	}

	sshConfig := &sftp.SSHConfig{
		User:        username,
		HostName:    host,
		Port:        port,
		AuthMethod:  authMethod,
		DialTimeout: 10 * time.Second,
	}

	if authMethod == "passwordAuth" {
		sshConfig.Password = password
	} else if authMethod == "keyAuth" {
		sshConfig.PrivateKey = string(privateKey)
	}

	return sshConfig, nil
}

// getFieldNames extracts the field names from the first JSON record.
func getFieldNames(records []Record) ([]string, error) {
	if len(records) == 0 {
		return nil, fmt.Errorf("no records found")
	}
	fields, ok := records[0]["message"].(map[string]interface{})["fields"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("fields not found in the first record")
	}
	var header []string
	for key := range fields {
		header = append(header, key)
	}
	header = append(header, "action")
	return header, nil
}

// parseRecords parses JSON records from the input text file.
func parseRecords(filePath string) ([]Record, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var records []Record
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var record Record
		if err := json.Unmarshal([]byte(scanner.Text()), &record); err != nil {
			return nil, fmt.Errorf("error parsing JSON record: %v", err)
		}
		records = append(records, record)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return records, nil
}

func generateFile(filePath string, format string, fileName string) (string, error) {
	switch strings.ToLower(format) {
	case "json":
		return generateJSONFile(filePath, fileName)
	case "csv":
		return generateCSVFile(filePath, fileName)
	default:
		return "", errors.New("unsupported format")
	}
}
func generateJSONFile(filePath string, fileName string) (string, error) {
	// Open the input file
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	var data interface{}
	err = json.NewDecoder(file).Decode(&data)
	if err != nil {
		return "", err
	}

	// Generate the desired file path
	desiredFilePath := filepath.Join(os.TempDir(), fileName)

	// Create a temporary CSV file
	tempFile, err := os.Create(desiredFilePath)
	if err != nil {
		return "", err
	}
	defer tempFile.Close()

	// Write JSON data to the temporary file
	encoder := json.NewEncoder(tempFile)
	encoder.SetIndent("", "  ")
	err = encoder.Encode(data)
	if err != nil {
		return "", err
	}

	// Get the absolute path of the temporary file
	absPath, err := filepath.Abs(tempFile.Name())
	if err != nil {
		return "", err
	}

	return absPath, nil
}

func generateCSVFile(filePath string, fileName string) (string, error) {
	// Parse JSON records
	records, err := parseRecords(filePath)
	if err != nil {
		return "", err
	}

	// Extract field names
	fieldNames, err := getFieldNames(records)
	if err != nil {
		return "", err
	}

	// Generate the desired file path
	desiredFilePath := filepath.Join(os.TempDir(), fileName)

	// Create a temporary CSV file
	tempFile, err := os.Create(desiredFilePath)
	if err != nil {
		return "", err
	}
	defer tempFile.Close()

	// Create a CSV writer
	writer := csv.NewWriter(tempFile)

	// Write header to the CSV file
	if err := writer.Write(fieldNames); err != nil {
		return "", err
	}

	// Write records to the CSV file
	for _, record := range records {
		var row []string
		fields, ok := record["message"].(map[string]interface{})["fields"].(map[string]interface{})
		if !ok {
			return "", fmt.Errorf("fields not found in a record")
		}
		action, ok := record["message"].(map[string]interface{})["action"].(string)
		if !ok {
			return "", fmt.Errorf("action not found in a record")
		}
		fields["action"] = action
		for _, key := range fieldNames {
			row = append(row, fmt.Sprintf("%v", fields[key]))
		}
		if err := writer.Write(row); err != nil {
			return "", err
		}
	}

	// Flush any buffered data to the underlying writer
	writer.Flush()
	if err := writer.Error(); err != nil {
		return "", err
	}

	// Get the absolute path of the temporary file
	absPath, err := filepath.Abs(tempFile.Name())
	if err != nil {
		return "", err
	}

	return absPath, nil
}


func getUploadFilePath(path string) (string, error) {
	// Define a regular expression to match dynamic variables
	re := regexp.MustCompile(`{([^}]+)}`)

	// Get the current date and time
	now := time.Now()
	// Replace dynamic variables with their actual values
	result := re.ReplaceAllStringFunc(path, func(match string) string {
		switch match {
		case "{YYYY}":
			return strconv.Itoa(now.Year())
		case "{MM}":
			return fmt.Sprintf("%02d", now.Month())
		case "{DD}":
			return fmt.Sprintf("%02d", now.Day())
		case "{hh}":
			return fmt.Sprintf("%02d", now.Hour())
		case "{mm}":
			return fmt.Sprintf("%02d", now.Minute())
		case "{ss}":
			return fmt.Sprintf("%02d", now.Second())
		case "{ms}":
			return fmt.Sprintf("%03d", now.Nanosecond()/1e6)
		case "{timestampInSec}":
			return strconv.FormatInt(now.Unix(), 10)
		case "{timestampInMs}":
			return strconv.FormatInt(now.UnixNano()/1e6, 10)
		default:
			// If the dynamic variable is not recognized, keep it unchanged
			return match
		}
	})

	return result, nil
}

func generateErrorOutput(errorString string, err error, importingJobIds []int64, destinationID string) common.AsyncUploadOutput {
	return common.AsyncUploadOutput{
		DestinationID: destinationID,
		AbortCount:    len(importingJobIds),
		AbortJobIDs:   importingJobIds,
		AbortReason:   fmt.Sprintf("%s %v", errorString, err.Error()),
	}
}
