package augmenter

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/samber/lo"
	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-server/services/oauth/v2/common"
)

type requestAugmenter struct{}

var RequestAugmenter = &requestAugmenter{}

// Custom augmenter for Salesforce which sets token to Authorization header and instance URL to the request URL
func (s *requestAugmenter) Augment(r *http.Request, body []byte, secret json.RawMessage) error {
	if secret == nil {
		return errors.New("secret is nil")
	}
	accessToken := gjson.GetBytes(secret, "access_token").String()
	if accessToken == "" {
		return errors.New("access token is empty")
	}
	instanceURL := gjson.GetBytes(secret, "instance_url").String()
	if instanceURL == "" {
		return errors.New("instance URL is empty")
	}
	instanceURL = strings.Replace(instanceURL, "https://", "", 1)
	// format -> Authorization : OAuth <accessToken>
	r.Header.Set("Authorization", fmt.Sprintf("Bearer %s", accessToken))
	r.URL.Host = instanceURL
	r.Body = io.NopCloser(bytes.NewReader(body))
	return nil
}

// GetAuthErrorCategoryForSalesforce returns the error category for Salesforce authentication errors

func GetAuthErrorCategoryForSalesforce(responseBody []byte) (string, error) {
	/*
		Sample response for Salesforce
		[
			{
				"message": "Session expired or invalid",
				"errorCode": "INVALID_SESSION_ID"
			}
		]
	*/
	invalidResults := lo.Filter(gjson.GetBytes(responseBody, "#.errorCode").Array(), func(errorTypeResult gjson.Result, _ int) bool {
		return errorTypeResult.String() == "INVALID_SESSION_ID"
	})
	if len(invalidResults) > 0 {
		return common.CategoryRefreshToken, nil
	}
	return "", nil
}
