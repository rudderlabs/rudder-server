package v2

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/rudderlabs/rudder-server/services/oauth/v2/common"
)

func InvalidGrantResponse(accountSecret AccountSecret) (int, *AuthResponse, error) {
	authResponse := &AuthResponse{
		Err:          common.RefTokenInvalidGrant,
		ErrorMessage: "invalid grant",
		Account:      accountSecret,
	}
	return http.StatusBadRequest, authResponse, fmt.Errorf("invalid grant")
}

func SuccessResponse(accountSecret AccountSecret) (int, *AuthResponse, error) {
	return http.StatusOK, &AuthResponse{
		Account: accountSecret,
	}, nil
}

func EmptySecretResponse() (int, *AuthResponse, error) {
	return http.StatusInternalServerError, nil, errors.New("empty secret")
}

func MarshalError(err error) (int, *AuthResponse, error) {
	return http.StatusInternalServerError, nil, err
}

func CpErrorResponse(errType, errMsg string, accountSecret AccountSecret) (int, *AuthResponse, error) {
	return http.StatusInternalServerError, &AuthResponse{
		Err:          errType,
		ErrorMessage: errMsg,
		Account:      accountSecret,
	}, fmt.Errorf("error occurred while fetching/refreshing account info from CP: %s", errMsg)
}
