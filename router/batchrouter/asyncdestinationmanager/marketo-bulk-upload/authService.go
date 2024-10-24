package marketobulkupload

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type MarketoAccessToken struct {
	AccessToken string `json:"access_token"`
	TokenType   string `json:"token_type"`
	ExpiresIn   int64  `json:"expires_in"`
	FetchedAt   int64
	Scope       string `json:"scope"`
}

type MarketoAuthServiceInterface interface {
	GetAccessToken() (string, error)
}

type MarketoAuthService struct {
	httpCLient   *http.Client
	munchkinId   string
	clientId     string
	clientSecret string
	accessToken  MarketoAccessToken
}

func (m *MarketoAuthService) fetchOrUpdateAccessToken() error {
	accessTokenURL := fmt.Sprintf("https://%s.mktorest.com/identity/oauth/token?client_id=%s&client_secret=%s&grant_type=client_credentials", m.munchkinId, m.clientId, m.clientSecret)
	req, err := http.NewRequest("POST", accessTokenURL, nil)
	if err != nil {
		return err
	}

	resp, err := m.httpCLient.Do(req)
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	var accessToken MarketoAccessToken
	err = json.Unmarshal(body, &accessToken)
	if err != nil {
		return err
	}
	accessToken.FetchedAt = time.Now().Unix()
	m.accessToken = accessToken
	return nil
}

func (m *MarketoAuthService) GetAccessToken() (string, error) {
	// keeping simple logic for now
	err := m.fetchOrUpdateAccessToken()
	if err != nil {
		return "", err
	}
	return m.accessToken.AccessToken, nil
}
