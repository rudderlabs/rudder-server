package testhelper

import (
	"encoding/json"
	"fmt"
	"os"
)

type TestCredentials struct {
	ProjectID   string `json:"projectID"`
	Location    string `json:"location"`
	BucketName  string `json:"bucketName"`
	Credentials string `json:"credentials"`
}

const TestKey = "BIGQUERY_INTEGRATION_TEST_CREDENTIALS"

func GetBQTestCredentials() (*TestCredentials, error) {
	cred, exists := os.LookupEnv(TestKey)
	if !exists {
		return nil, fmt.Errorf("bq credentials not found")
	}

	var credentials TestCredentials
	err := json.Unmarshal([]byte(cred), &credentials)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal bq credentials: %w", err)
	}

	return &credentials, nil
}

func IsBQTestCredentialsAvailable() bool {
	_, err := GetBQTestCredentials()
	return err == nil
}
