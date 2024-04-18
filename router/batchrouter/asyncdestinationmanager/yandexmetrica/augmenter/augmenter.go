package augmenter

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/samber/lo"
	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-server/services/oauth/v2/common"
)

type yandexAugmenter struct{}

var YandexReqAugmenter = &yandexAugmenter{}

// Custom augmenter for Yandex which sets token to Authorization header
func (y *yandexAugmenter) Augment(r *http.Request, body []byte, secret json.RawMessage) error {
	if secret == nil {
		return errors.New("secret is nil")
	}
	token := gjson.GetBytes(secret, "accessToken").String()
	// format -> Authorization : OAuth <accessToken>
	r.Header.Set("Authorization", fmt.Sprintf("OAuth %s", token))
	r.Body = io.NopCloser(bytes.NewReader(body))
	return nil
}

func GetAuthErrorCategoryForYandex(responseBody []byte) (string, error) {
	/*
		Sample response for Yandex
		{
		    "errors": [
		        {
		            "error_type": "invalid_token",
		            "message": "Invalid oauth_token"
		        }
		    ],
		    "code": 403,
		    "message": "Invalid oauth_token"
		}
	*/
	allErrorTypes := lo.Map(gjson.GetBytes(responseBody, "errors.#.error_type").Array(), func(errorTypeResult gjson.Result, _ int) string {
		return errorTypeResult.String()
	})
	isInvalidToken := lo.Contains(allErrorTypes, "invalid_token")
	if isInvalidToken {
		return common.CategoryRefreshToken, nil
	}
	return "", nil
}
