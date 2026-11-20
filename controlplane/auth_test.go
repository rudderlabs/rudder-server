package controlplane

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	proto "github.com/rudderlabs/rudder-server/proto/common"
)

func getSuccess(t *testing.T, a *authService) *proto.GetConnectionTokenSuccessResponse {
	t.Helper()
	resp, err := a.GetConnectionToken(context.Background(), &proto.GetConnectionTokenRequest{})
	require.NoError(t, err)
	return resp.GetSuccessResponse()
}

func TestGetConnectionToken_SendsIdentityToken(t *testing.T) {
	a := &authService{authInfo: AuthInfo{ConnectionToken: "ns", TokenType: "NAMESPACE", IdentityToken: func() string { return "t1" }}}
	require.Equal(t, "t1", getSuccess(t, a).GetIdentityToken())
}

func TestGetConnectionToken_ReadsTokenPerHandshake(t *testing.T) {
	tokens := []string{"t1", "t2"}
	a := &authService{authInfo: AuthInfo{ConnectionToken: "ns", IdentityToken: func() string {
		tok := tokens[0]
		tokens = tokens[1:]
		return tok
	}}}
	require.Equal(t, "t1", getSuccess(t, a).GetIdentityToken())
	require.Equal(t, "t2", getSuccess(t, a).GetIdentityToken())
}

func TestGetConnectionToken_NoSource(t *testing.T) {
	a := &authService{authInfo: AuthInfo{ConnectionToken: "ns", Service: "svc", InstanceID: "i", TokenType: "NAMESPACE"}}
	r := getSuccess(t, a)
	require.Empty(t, r.GetIdentityToken())
	require.Equal(t, "ns", r.GetConnectionToken())
	require.Equal(t, "svc", r.GetService())
	require.Equal(t, "i", r.GetInstanceID())
	require.Equal(t, "NAMESPACE", r.GetTokenType())
}
