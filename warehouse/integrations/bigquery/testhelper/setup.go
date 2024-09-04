package testhelper

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

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

// RetrieveRecordsFromWarehouse retrieves records from the warehouse based on the given query.
// It returns a slice of slices, where each inner slice represents a record's values.
func RetrieveRecordsFromWarehouse(
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
		if errors.Is(it.Next(&row), iterator.Done) {
			break
		}
		require.NoError(t, err)

		records = append(records, lo.Map(row, func(item bigquery.Value, index int) string {
			switch item := item.(type) {
			case time.Time:
				return item.Format(time.RFC3339)
			case string:
				if t, err := time.Parse(time.RFC3339Nano, item); err == nil {
					return t.Format(time.RFC3339)
				}
				return item
			default:
				return cast.ToString(item)
			}
		}))
	}
	return records
}
