package sftp

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	jsoniter "github.com/json-iterator/go"
	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/sftp"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

var (
	json = jsoniter.ConfigCompatibleWithStandardLibrary
	re   = regexp.MustCompile(`{([^}]+)}`)
)

// createSSHConfig creates SSH configuration based on destination
func createSSHConfig(destination *backendconfig.DestinationT) (*sftp.SSHConfig, error) {
	destinationConfigJson, err := json.Marshal(destination.Config)
	if err != nil {
		return nil, fmt.Errorf("marshalling destination config: %w", err)
	}
	var config destConfig
	if err := json.Unmarshal(destinationConfigJson, &config); err != nil {
		return nil, fmt.Errorf("unmarshalling destination config: %w", err)
	}

	if err := validate(config); err != nil {
		return nil, fmt.Errorf("invalid sftp configuration: %w", err)
	}
	port, _ := strconv.Atoi(config.Port)
	sshConfig := &sftp.SSHConfig{
		User:       config.Username,
		HostName:   config.Host,
		Port:       port,
		AuthMethod: config.AuthMethod,
		Password:   config.Password,
		PrivateKey: config.PrivateKey,
	}

	return sshConfig, nil
}

// getFieldNames extracts the field names from the first JSON record.
func getFieldNames(records []record) ([]string, error) {
	if len(records) == 0 {
		return nil, errors.New("no records found")
	}
	result, ok := records[0]["message"].(map[string]any)
	if !ok {
		return nil, errors.New("message not found in the first record")
	}
	fields, ok := result["fields"].(map[string]any)
	if !ok {
		return nil, errors.New("fields not found in the first record")
	}
	header := lo.Keys(fields)
	header = append(header, "action")
	return header, nil
}

// parseRecords parses JSON records from the input text file.
func parseRecords(filePath string) ([]record, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("opening file: %w", err)
	}
	defer file.Close()

	var records []record
	decoder := json.NewDecoder(file)
	for decoder.More() {
		var record record
		if err := decoder.Decode(&record); err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("error parsing JSON record: %v", err)
		}
		records = append(records, record)
	}
	return records, nil
}

func generateFile(filePath, format string) (string, error) {
	switch strings.ToLower(format) {
	case "json":
		return generateJSONFile(filePath)
	case "csv":
		return generateCSVFile(filePath)
	default:
		return "", errors.New("unsupported file format")
	}
}

func generateJSONFile(filePath string) (string, error) {
	// Parse JSON records
	records, err := parseRecords(filePath)
	if err != nil {
		return "", err
	}

	tmpFilePath, err := getTempFilePath()
	if err != nil {
		return "", err
	}
	tmpFilePath = fmt.Sprintf(`%v.json`, tmpFilePath)

	// Create a temporary CSV file
	tempFile, err := os.Create(tmpFilePath)
	if err != nil {
		return "", err
	}
	defer tempFile.Close()

	// Write JSON data to the temporary file
	encoder := json.NewEncoder(tempFile)
	encoder.SetIndent("", "  ")
	err = encoder.Encode(records)
	if err != nil {
		return "", err
	}

	return tmpFilePath, nil
}

func generateCSVFile(filePath string) (string, error) {
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

	tmpFilePath, err := getTempFilePath()
	if err != nil {
		return "", err
	}
	tmpFilePath = fmt.Sprintf(`%v.csv`, tmpFilePath)

	// Create a temporary CSV file
	tempFile, err := os.Create(tmpFilePath)
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
		result, ok := record["message"].(map[string]any)
		if !ok {
			return "", errors.New("message not found in a record")
		}

		fields, ok := result["fields"].(map[string]any)
		if !ok {
			return "", errors.New("fields not found in a record")
		}

		action, ok := record["message"].(map[string]any)["action"].(string)
		if !ok {
			return "", errors.New("action not found in a record")
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

	return tmpFilePath, nil
}

func getTempFilePath() (string, error) {
	localTmpDirName := fmt.Sprintf(`/%s/`, misc.RudderAsyncDestinationLogs)
	tmpDirPath, err := misc.CreateTMPDIR()
	if err != nil {
		return "", err
	}
	folderPath := path.Join(tmpDirPath, localTmpDirName)
	_, e := os.Stat(folderPath)
	if os.IsNotExist(e) {
		if err := os.MkdirAll(folderPath, os.ModePerm); err != nil {
			return "", err
		}
	}
	tmpFilePath := filepath.Join(folderPath, uuid.NewString())
	return tmpFilePath, nil
}

func getUploadFilePath(path string, metadata map[string]any) (string, error) {
	if path == "" {
		return "", errors.New("upload file path can not be empty")
	}

	// Get the current date and time
	now, ok := metadata["timestamp"].(time.Time)
	if !ok {
		now = time.Now()
	}
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
		case "{timestampInMS}":
			return strconv.FormatInt(now.UnixNano()/1e6, 10)
		case "{destinationID}":
			return metadata["destinationID"].(string)
		case "{jobRunID}":
			return metadata["sourceJobRunID"].(string)
		default:
			// If the dynamic variable is not recognized, keep it unchanged
			return match
		}
	})

	return result, nil
}

func generateErrorOutput(err string, importingJobIds []int64, destinationID string) common.AsyncUploadOutput {
	return common.AsyncUploadOutput{
		DestinationID: destinationID,
		AbortCount:    len(importingJobIds),
		AbortJobIDs:   importingJobIds,
		AbortReason:   err,
	}
}

func validate(d destConfig) error {
	if d.AuthMethod == "" {
		return errors.New("authMethod cannot be empty")
	}

	if d.Username == "" {
		return errors.New("username cannot be empty")
	}

	if d.Host == "" {
		return errors.New("host cannot be empty")
	}

	if err := isValidPort(d.Port); err != nil {
		return err
	}

	if d.AuthMethod == "passwordAuth" && d.Password == "" {
		return errors.New("password is required for password authentication")
	}

	if d.AuthMethod == "keyAuth" && d.PrivateKey == "" {
		return errors.New("private key is required for key authentication")
	}

	if err := isValidFileFormat(d.FileFormat); err != nil {
		return err
	}

	if err := validateFilePath(d.FilePath); err != nil {
		return err
	}

	return nil
}

func validateFilePath(path string) error {
	if !strings.Contains(path, "{destinationID}") {
		return errors.New("destinationID placeholder is missing in the upload filePath")
	}

	if !strings.Contains(path, "{jobRunID}") {
		return errors.New("jobRunID placeholder is missing in the upload filePath")
	}
	return nil
}

func isValidPort(p string) error {
	port, err := strconv.Atoi(p)
	if err != nil {
		return err
	}
	if port < 1 || port > 65535 {
		return fmt.Errorf("invalid port: %d", port)
	}
	return nil
}

func isValidFileFormat(format string) error {
	if format != "json" && format != "csv" {
		return fmt.Errorf("invalid file format: %s", format)
	}
	return nil
}

func appendFileNumberInFilePath(path string, partFileNumber int) string {
	ext := filepath.Ext(path)
	base := strings.TrimSuffix(path, ext)
	return fmt.Sprintf("%s_%d%s", base, partFileNumber, ext)
}
