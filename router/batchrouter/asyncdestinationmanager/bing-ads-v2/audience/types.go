package audience

import (
	"fmt"

	"github.com/rudderlabs/bing-ads-go-sdk/bingads"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/bing-ads-v2/common"
)

type Client struct {
	URL string
}
type BingAdsBulkUploader struct {
	destName      string
	service       bingads.BulkServiceI
	logger        logger.Logger
	statsFactory  stats.Stats
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
	common.BaseDestinationConfig
}

var actionTypes = [3]string{"Replace", "Remove", "Add"}

const commaSeparator = common.CommaSeparator

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
