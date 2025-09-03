package offline_conversions

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/rudderlabs/bing-ads-go-sdk/bingads"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
)

type BingAdsBulkUploader struct {
	destName       string
	service        bingads.BulkServiceI
	logger         logger.Logger
	statsFactory   stats.Stats
	fileSizeLimit  int64
	eventsLimit    int64
	isHashRequired bool
}
type Message struct {
	Fields RecordFields `json:"fields"`
	Action string       `json:"action"`
}
type Metadata struct {
	JobID int64 `json:"jobId"`
}

// This struct represent each line of the text file created by the batchrouter
type Data struct {
	Message  Message  `json:"message"`
	Metadata Metadata `json:"metadata"`
}

var actionTypes = [3]string{"update", "insert", "delete"}

type Record struct {
	Action   string          `json:"action"`
	Type     string          `json:"type"`
	Channel  string          `json:"channel"`
	Fields   RecordFields    `json:"fields"`
	RecordId string          `json:"recordId"`
	Context  json.RawMessage `json:"context"`
}

type RecordFields struct {
	ConversionCurrencyCode    string `json:"conversionCurrencyCode,omitempty"`
	ConversionValue           string `json:"conversionValue,omitempty"`
	ConversionName            string `json:"conversionName,omitempty"`
	ConversionTime            string `json:"conversionTime,omitempty"`
	Email                     string `json:"email,omitempty"`
	Phone                     string `json:"phone,omitempty"`
	MicrosoftClickId          string `json:"microsoftClickId,omitempty"`
	ConversionAdjustedTime    string `json:"adjustedConversionTime,omitempty"`
	ExternalAttributionCredit string `json:"externalAttributionCredit,omitempty"`
	ExternalAttributionModel  string `json:"externalAttributionModel,omitempty"`
}

func (rf *RecordFields) HashFields() {
	if rf.Email != "" {
		rf.Email = calculateHashCode(rf.Email)
	}
	if rf.Phone != "" {
		rf.Phone = calculateHashCode(rf.Phone)
	}
}

// parseAndFormatTime parses a time string and formats it to the required format
func (rf *RecordFields) parseAndFormatTime(timeStr, fieldName string) (string, error) {
	if timeStr == "" {
		return "", nil
	}

	parsedTime, parseErr := time.Parse(time.RFC3339, timeStr)
	if parseErr != nil {
		parsedTime, parseErr = time.Parse("1/2/2006 3:04:05 PM", timeStr)
		if parseErr != nil {
			return "", fmt.Errorf("%s must be in ISO 8601 (e.g. 2006-01-02T15:04:05Z07:00) or mm/dd/yyyy hh:mm:ss AM/PM (e.g. 7/2/2025 6:50:54 PM) format", fieldName)
		}
	}
	return parsedTime.Format("1/2/2006 3:04:05 PM"), nil
}

// ValidateAndTransformTimeFields validates and transforms time fields to the required format
func (rf *RecordFields) ValidateAndTransformTimeFields() error {
	var err error
	if rf.ConversionTime, err = rf.parseAndFormatTime(rf.ConversionTime, "conversionTime"); err != nil {
		return err
	}
	if rf.ConversionAdjustedTime, err = rf.parseAndFormatTime(rf.ConversionAdjustedTime, "adjustedConversionTime"); err != nil {
		return err
	}

	return nil
}

// HasEnhancedConversion checks if enhanced conversion is provided via microsoftClickId, email, or phone
func (rf *RecordFields) HasEnhancedConversion() bool {
	if rf.MicrosoftClickId != "" {
		return true
	}
	if rf.Email != "" {
		return true
	}
	if rf.Phone != "" {
		return true
	}
	return false
}

// ValidateAllFields performs comprehensive validation of all fields
func (rf *RecordFields) ValidateAllFields(action string) error {
	// Check if required fields are present and not empty
	if rf.ConversionName == "" {
		return fmt.Errorf(" conversionName field not defined")
	}
	if rf.ConversionTime == "" {
		return fmt.Errorf(" conversionTime field not defined")
	}

	// Action-specific validations
	if action == "delete" {
		if rf.ConversionAdjustedTime == "" {
			return fmt.Errorf(" adjustedConversionTime field not defined")
		}
	}
	if action == "update" {
		if rf.ConversionAdjustedTime == "" {
			return fmt.Errorf(" adjustedConversionTime field not defined")
		}
		if rf.ConversionValue == "" {
			return fmt.Errorf("conversionValue field not defined")
		}
	}

	return nil
}

// ValidateAndTransformAllFields performs comprehensive validation and transformation of all fields
func (rf *RecordFields) ValidateAndTransformAllFields(action string) error {
	// First validate and transform time fields
	if err := rf.ValidateAndTransformTimeFields(); err != nil {
		return err
	}

	// Then validate all fields
	if err := rf.ValidateAllFields(action); err != nil {
		return err
	}

	// Finally check for enhanced conversion
	if !rf.HasEnhancedConversion() {
		return fmt.Errorf("missing required field: microsoftClickId (or provide a hashed email/phone for enhanced conversions)")
	}

	return nil
}
