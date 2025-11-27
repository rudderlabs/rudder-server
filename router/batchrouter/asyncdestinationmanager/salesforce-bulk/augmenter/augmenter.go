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

type salesforceReqAugmenter struct{}

var SalesforceReqAugmenter = &salesforceReqAugmenter{}

// Custom augmenter for Yandex which sets token to Authorization header
func (s *salesforceReqAugmenter) Augment(r *http.Request, body []byte, secret json.RawMessage) error {
	if secret == nil {
		return errors.New("secret is nil")
	}
	token := gjson.GetBytes(secret, "access_token").String()
	instanceURL := gjson.GetBytes(secret, "instance_url").String()
	instanceURL = strings.Replace(instanceURL, "https://", "", 1)
	// format -> Authorization : OAuth <accessToken>
	r.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
	r.URL.Host = instanceURL
	r.Body = io.NopCloser(bytes.NewReader(body))
	return nil
}

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
	if len(lo.Filter(gjson.GetBytes(responseBody, "#.errorCode").Array(), func(errorTypeResult gjson.Result, _ int) bool {
		return errorTypeResult.String() == "INVALID_SESSION_ID"
	})) > 0 {
		return common.CategoryRefreshToken, nil
	}
	return "", nil
}
