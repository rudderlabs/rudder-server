package v2

import (
	"bytes"
	"fmt"
	"io"
	"net/http"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	oauth_exts "github.com/rudderlabs/rudder-server/services/oauth/v2/extensions"
	"github.com/tidwall/gjson"
)

type Handler interface {
	// Handler is used to handle the request and return the response
	Handler(r *http.Request, roundTrip http.RoundTripper, t OAuthHandler, a oauth_exts.Augmenter) (*http.Response, error)
}

type transformHandler struct{}
type deliveryHandler struct{}

var TransformHandler = &transformHandler{}
var DeliveryHandler = &deliveryHandler{}

func (t *transformHandler) Handler(r *http.Request, roundTrip http.RoundTripper, t1 OAuthHandler, a oauth_exts.Augmenter) (*http.Response, error) {
	destination := r.Context().Value("destination").(*backendconfig.DestinationT)
	if destination == nil {
		return nil, fmt.Errorf("no destination found in context")
	}
	if !destination.IsOAuthDestination() {
		return roundTrip.RoundTrip(r)
	}
	accountId := destination.GetAccountID("rudderAccountId")
	refreshTokenParams := &RefreshTokenParams{
		AccountId:   accountId,
		WorkspaceId: destination.WorkspaceID,
		DestDefName: destination.DestinationDefinition.Name,
	}
	fetchStatus, response := t1.FetchToken(refreshTokenParams)
	body, _ := io.ReadAll(r.Body)
	defer io.ReadCloser.Close(r.Body)
	if fetchStatus == 200 {
		a.Augment(r, body, response.Account.Secret)
	}
	// t.keyLocker.Lock(accountId)
	// t.keyLocker.Unlock(accountId)
	res, _ := roundTrip.RoundTrip(r)
	respData, _ := io.ReadAll(res.Body)
	defer res.Body.Close()
	res.Body = io.NopCloser(bytes.NewReader(respData))
	destinationJobs := []DestinationJobT{}
	err := jsonfast.Unmarshal([]byte(gjson.GetBytes(respData, "output").Raw), &destinationJobs)
	if err != nil {
		return res, err
	}
	destinationJob := destinationJobs[0]
	if destinationJob.AuthErrorCategory == "REFRESH_TOKEN" {
		t1.logger.Info("refreshing token")
		t1.RefreshToken(refreshTokenParams)
	}
	return res, err
}

type TransResponseT struct {
	Message             string      `json:"message"`
	DestinationResponse interface{} `json:"destinationResponse"`
	AuthErrorCategory   string      `json:"authErrorCategory"`
}

func (t *deliveryHandler) Handler(r *http.Request, roundTrip http.RoundTripper, t1 OAuthHandler, a oauth_exts.Augmenter) (*http.Response, error) {
	res, err := roundTrip.RoundTrip(r)
	if err != nil {
		return res, err
	}
	if res.StatusCode != http.StatusOK {
		respData, err := io.ReadAll(res.Body)
		if err != nil {
			return res, err
		}
		res.Body = io.NopCloser(bytes.NewReader(respData))
		defer res.Body.Close()
		transformerResponse := TransResponseT{}
		err = jsonfast.Unmarshal([]byte(gjson.GetBytes(respData, "output").Raw), &transformerResponse)
		if err != nil {
			return res, err
		}
		switch transformerResponse.AuthErrorCategory {
		case AUTH_STATUS_INACTIVE:
			//to do
		case REFRESH_TOKEN:
			{
				destination := r.Context().Value("destination").(*backendconfig.DestinationT)
				accountId := destination.GetAccountID("rudderAccountId")
				refreshTokenParams := &RefreshTokenParams{
					AccountId:   accountId,
					WorkspaceId: destination.WorkspaceID,
					DestDefName: destination.DestinationDefinition.Name,
				}
				t1.RefreshToken(refreshTokenParams)
			}
			res.StatusCode = 500
		}
	}
	return res, err
}
