package v2

import (
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/services/oauth/v2/common"
)

func TestCredentialErrorType(t *testing.T) {
	statusCodeError := func(errType string) error {
		return NewStatusCodeError(http.StatusBadRequest, &TypeMessageError{Type: errType, Message: "msg"})
	}
	// how a token error reaches a caller that wraps it, e.g. the Bing Ads token source and manager
	wrapped := func(err error) error {
		return fmt.Errorf("failed to generate oauth token: %w", fmt.Errorf("fetching access token: %w", err))
	}

	require.Equal(t, common.RefTokenInvalidGrant, CredentialErrorType(statusCodeError(common.RefTokenInvalidGrant)))
	require.Equal(t, common.RefTokenInvalidGrant, CredentialErrorType(wrapped(statusCodeError(common.RefTokenInvalidGrant))))
	require.Equal(t, common.CategoryAuthStatusInactive, CredentialErrorType(statusCodeError(common.CategoryAuthStatusInactive)))
	require.Empty(t, CredentialErrorType(statusCodeError(common.RefTokenInvalidResponse)), "not a dead credential")
	require.Empty(t, CredentialErrorType(errors.New("network error")))
	require.Empty(t, CredentialErrorType(nil))
}
