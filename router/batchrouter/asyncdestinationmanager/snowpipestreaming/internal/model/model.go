package model

import (
	"regexp"

	"github.com/rudderlabs/rudder-server/warehouse/integrations/snowflake"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

var (
	reType = regexp.MustCompile(`(.+?)\([^)]*\)`)
)

type (
	CreateChannelRequest struct {
		RudderIdentifier string        `json:"rudderIdentifier"`
		Partition        string        `json:"partition"`
		AccountConfig    AccountConfig `json:"account"`
		TableConfig      TableConfig   `json:"table"`
	}
	AccountConfig struct {
		Account              string `json:"account"`
		User                 string `json:"user"`
		Role                 string `json:"role"`
		PrivateKey           string `json:"privateKey"`
		PrivateKeyPassphrase string `json:"privateKeyPassphrase"`
	}
	TableConfig struct {
		Database string `json:"database"`
		Schema   string `json:"schema"`
		Table    string `json:"table"`
	}

	ChannelResponse struct {
		Success                bool                      `json:"success"`
		ChannelID              string                    `json:"channelId"`
		ChannelName            string                    `json:"channelName"`
		ClientName             string                    `json:"clientName"`
		Valid                  bool                      `json:"valid"`
		Deleted                bool                      `json:"deleted"`
		TableSchema            map[string]map[string]any `json:"tableSchema"`
		Error                  string                    `json:"error"`
		Code                   string                    `json:"code"`
		SnowflakeSDKCode       string                    `json:"snowflakeSDKCode"`
		SnowflakeAPIHttpCode   int64                     `json:"snowflakeAPIHttpCode"`
		SnowflakeAPIStatusCode int64                     `json:"snowflakeAPIStatusCode"`
		SnowflakeAPIMessage    string                    `json:"snowflakeAPIMessage"`
	}

	InsertRequest struct {
		Rows   []Row  `json:"rows"`
		Offset string `json:"offset"`
	}
	Row map[string]any

	InsertResponse struct {
		Success bool          `json:"success"`
		Errors  []InsertError `json:"errors"`
		Code    string        `json:"code"`
	}
	InsertError struct {
		RowIndex                    int64    `json:"rowIndex"`
		ExtraColNames               []string `json:"extraColNames"`
		MissingNotNullColNames      []string `json:"missingNotNullColNames"`
		NullValueForNotNullColNames []string `json:"nullValueForNotNullColNames"`
		Message                     string   `json:"message"`
	}

	StatusResponse struct {
		Success bool   `json:"success"`
		Offset  string `json:"offset"`
		Valid   bool   `json:"valid"`
	}
)

func (c *ChannelResponse) SnowPipeSchema() whutils.ModelTableSchema {
	warehouseSchema := make(whutils.ModelTableSchema)

	for column, info := range c.TableSchema {
		dataType, isValidType := info["type"].(string)
		if !isValidType {
			continue
		}

		numericScale := int64(0)
		if scale, scaleExists := info["scale"].(float64); scaleExists {
			numericScale = int64(scale)
		}

		cleanedDataType := reType.ReplaceAllString(dataType, "$1")

		snowflakeType, _ := snowflake.CalculateDataType(cleanedDataType, numericScale)
		warehouseSchema[column] = snowflakeType
	}
	return warehouseSchema
}
