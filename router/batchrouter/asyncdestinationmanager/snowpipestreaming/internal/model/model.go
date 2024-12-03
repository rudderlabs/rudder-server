package model

import (
	"strings"

	jsoniter "github.com/json-iterator/go"

	"github.com/rudderlabs/rudder-server/warehouse/integrations/snowflake"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
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
		Success                bool                     `json:"success"`
		ChannelID              string                   `json:"channelId"`
		ChannelName            string                   `json:"channelName"`
		ClientName             string                   `json:"clientName"`
		Valid                  bool                     `json:"valid"`
		Deleted                bool                     `json:"deleted"`
		SnowpipeSchema         whutils.ModelTableSchema `json:"-"`
		Error                  string                   `json:"error"`
		Code                   string                   `json:"code"`
		SnowflakeSDKCode       string                   `json:"snowflakeSDKCode"`
		SnowflakeAPIHttpCode   int64                    `json:"snowflakeAPIHttpCode"`
		SnowflakeAPIStatusCode int64                    `json:"snowflakeAPIStatusCode"`
		SnowflakeAPIMessage    string                   `json:"snowflakeAPIMessage"`
	}

	ColumnInfo struct {
		Type  *string  `json:"type,omitempty"`
		Scale *float64 `json:"scale,omitempty"`
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

var json = jsoniter.ConfigCompatibleWithStandardLibrary

func (c *ChannelResponse) UnmarshalJSON(data []byte) error {
	type Alias ChannelResponse // Prevent recursion
	temp := &struct {
		TableSchema map[string]ColumnInfo `json:"tableSchema"`
		*Alias
	}{
		Alias: (*Alias)(c),
	}
	if err := json.Unmarshal(data, &temp); err != nil {
		return err
	}
	c.SnowpipeSchema = generateSnowpipeSchema(temp.TableSchema)
	return nil
}

func generateSnowpipeSchema(tableSchema map[string]ColumnInfo) whutils.ModelTableSchema {
	if len(tableSchema) == 0 {
		return nil
	}

	warehouseSchema := make(whutils.ModelTableSchema)
	for column, info := range tableSchema {
		if info.Type == nil {
			continue
		}

		numericScale := int64(0)
		if info.Scale != nil {
			numericScale = int64(*info.Scale)
		}

		dataType := cleanDataType(*info.Type)
		snowflakeDataType, ok := snowflake.CalculateDataType(dataType, numericScale)
		if !ok {
			continue
		}
		warehouseSchema[column] = snowflakeDataType
	}
	return warehouseSchema
}

func cleanDataType(input string) string {
	// Extract the portion before the first '('
	if idx := strings.Index(input, "("); idx != -1 {
		return input[:idx]
	}
	return input // Return as-is if no '(' is found
}
