package testhelper

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"cloud.google.com/go/bigquery"

	"github.com/samber/lo"
	"github.com/spf13/cast"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
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

func RecordsFromWarehouse(
	t testing.TB,
	db *bigquery.Client,
	query string,
) [][]string {
	t.Helper()

	it, err := db.Query(query).Read(context.Background())
	require.NoError(t, err)

	var records [][]string
	for {
		var row []bigquery.Value
		err = it.Next(&row)
		if err == iterator.Done {
			break
		}
		require.NoError(t, err)

		records = append(records, lo.Map(row, func(item bigquery.Value, index int) string {
			return cast.ToString(item)
		}))
	}

	return records
}
