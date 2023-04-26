package testhelper

import (
	"encoding/json"
	"fmt"
	"os"
)

const (
	BigQueryTestKey = "BIGQUERY_INTEGRATION_TEST_CREDENTIALS"
)

type BQTestCredentials struct {
	ProjectID   string `json:"projectID"`
	Location    string `json:"location"`
	BucketName  string `json:"bucketName"`
	Credentials string `json:"credentials"`
}

func GetBQTestCredentials() (*BQTestCredentials, error) {
	cred, exists := os.LookupEnv(BigQueryTestKey)
	if !exists {
		return nil, fmt.Errorf("bq credentials not found")
	}

	var credentials BQTestCredentials
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
