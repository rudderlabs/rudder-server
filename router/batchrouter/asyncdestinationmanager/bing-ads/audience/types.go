package audience

import (
	"encoding/csv"
	"fmt"
	"net/http"
	"os"

	"github.com/rudderlabs/bing-ads-go-sdk/bingads"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
)

type Client struct {
	URL    string
	client *http.Client
}
type BingAdsBulkUploader struct {
	destName      string
	service       bingads.BulkServiceI
	logger        logger.Logger
	statsFactory  stats.Stats
	client        Client
	fileSizeLimit int64
	eventsLimit   int64
}
type User struct {
	Email       string `json:"email"`
	HashedEmail string `json:"hashedEmail"`
}
type Message struct {
	List   []User `json:"List"`
	Action string `json:"Action"`
}
type Metadata struct {
	JobID int64 `json:"job_id"`
}

// This struct represent each line of the text file created by the batchrouter
type Data struct {
	Message  Message  `json:"message"`
	Metadata Metadata `json:"metadata"`
}

type DestinationConfig struct {
	CustomerAccountID string `json:"customerAccountId"`
	CustomerID        string `json:"customerId"`
	RudderAccountID   string `json:"rudderAccountId"`
}

type ActionFileInfo struct {
	Action           string
	CSVWriter        *csv.Writer
	CSVFilePath      string
	ZipFilePath      string
	SuccessfulJobIDs []int64
	FailedJobIDs     []int64
	FileSize         int64
	EventCount       int64
}

var actionTypes = [3]string{"Replace", "Remove", "Add"}

const commaSeparator = ","

const clientIDSeparator = "<<>>"

type ClientID struct {
	JobID       int64
	HashedEmail string
}

// returns the string representation of the clientID struct which is of format
// jobId<<>>hashedEmail
func (c *ClientID) ToString() string {
	return fmt.Sprintf("%d%s%s", c.JobID, clientIDSeparator, c.HashedEmail)
}

func CreateActionFileTemplate(csvFile *os.File, audienceId, actionType string) (*csv.Writer, error) {
	csvWriter := csv.NewWriter(csvFile)
	err := csvWriter.WriteAll([][]string{
		{"Type", "Status", "Id", "Parent Id", "Client Id", "Modified Time", "Name", "Description", "Scope", "Audience", "Action Type", "Sub Type", "Text"},
		{"Format Version", "", "", "", "", "", "6.0", "", "", "", "", "", ""},
		{"Customer List", "", audienceId, "", "", "", "", "", "", "", actionType, "", ""},
	})
	if err != nil {
		return nil, fmt.Errorf("error in writing csv header: %v", err)
	}
	return csvWriter, nil
}
