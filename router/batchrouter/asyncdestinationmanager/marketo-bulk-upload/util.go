package marketobulkupload

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"
)

type AccessTokenResponse struct {
	AccessToken string `json:"access_token"`
	TokenType   string `json:"token_type"`
	ExpiresIn   int    `json:"expires_in"`
	Scope       string `json:"scope"`
}

func getAccessTokenURL(config map[string]interface{}) string {
	return fmt.Sprintf(
		"https://%s.mktorest.com/identity/oauth/token?client_id=%s&client_secret=%s&grant_type=client_credentials",
		config["MunchkinId"],
		config["ClientId"],
		config["ClientSecret"],
	)
}

func getAccessToken(config map[string]interface{}) (string, error) {
	url := getAccessTokenURL(config)

	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return "", err
	}

	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("failed to get access token: status code %d", resp.StatusCode)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	var accessTokenResponse AccessTokenResponse
	if err := json.Unmarshal(body, &accessTokenResponse); err != nil {
		return "", err
	}

	return accessTokenResponse.AccessToken, nil
}

func handleHttpRequest(method, url string, headers map[string]string) (*http.Response, error) {
	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		return nil, err
	}

	for key, value := range headers {
		req.Header.Add(key, value)
	}

	return client.Do(req)
}

func isHttpStatusSuccess(status int) bool {
	return status >= 200 && status < 300
}
