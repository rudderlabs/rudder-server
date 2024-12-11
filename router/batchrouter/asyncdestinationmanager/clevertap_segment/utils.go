package clevertapSegment

import (
	"encoding/csv"
	"fmt"
	"os"
	"strings"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
)

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

func getBulkApi(destConfig DestinationConfig) (*Endpoints, error) {
	endpoint, err := getCleverTapEndpoint(destConfig.Region)
	if err != nil {
		return nil, err
	}
	return &Endpoints{
		BulkApi:   fmt.Sprintf("https://%s/get_custom_list_segment_url", endpoint),
		NotifyApi: fmt.Sprintf("https://%s/upload_custom_list_segment_completed", endpoint),
	}, nil
}

// getCleverTapEndpoint returns the API endpoint for the given region
func getCleverTapEndpoint(region string) (string, error) {
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

type ConfigOutput interface {
	ConnectionConfig | DestinationConfig
}

// Generic function to convert input to output using JSON marshal/unmarshal
func convert[T any, U ConfigOutput](input T) (U, error) {
	var output U

	// Marshal the input to JSON
	data, err := jsonFast.Marshal(input)
	if err != nil {
		return output, fmt.Errorf("failed to marshal input: %w", err)
	}

	// Unmarshal the JSON into the output type
	err = jsonFast.Unmarshal(data, &output)
	if err != nil {
		return output, fmt.Errorf("failed to unmarshal to output type: %w", err)
	}

	return output, nil
}

func convertToConnectionConfig(conn *backendconfig.Connection) (ConnectionConfig, error) {
	connConfig, err := convert[map[string]interface{}, ConnectionConfig](conn.Config)
	if err != nil {
		return ConnectionConfig{}, fmt.Errorf("failed to convert to connection config: %w", err)
	}
	if connConfig.Config.Destination.SenderName == "" {
		connConfig.Config.Destination.SenderName = DEFAULT_SENDER_NAME
	}
	return connConfig, nil
}
