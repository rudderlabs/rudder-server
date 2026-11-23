package controlplane

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-cp-sdk/tokenfile"

	proto "github.com/rudderlabs/rudder-server/proto/common"
)

func getSuccess(t *testing.T, a *authService) *proto.GetConnectionTokenSuccessResponse {
	t.Helper()
	resp, err := a.GetConnectionToken(context.Background(), &proto.GetConnectionTokenRequest{})
	require.NoError(t, err)
	return resp.GetSuccessResponse()
}

func TestGetConnectionToken_SendsIdentityToken(t *testing.T) {
	p := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(p, []byte("t1\n"), 0o600))
	tok := tokenfile.New(p)
	require.NoError(t, tok.Load())
	a := &authService{authInfo: AuthInfo{ConnectionToken: "ns", TokenType: "NAMESPACE", IdentityToken: tok}}
	require.Equal(t, "t1", getSuccess(t, a).GetIdentityToken())
}

func TestGetConnectionToken_ReadsTokenPerHandshake(t *testing.T) {
	// a Token that has never read its file re-reads it on every Get, so the second handshake sees the file
	p := filepath.Join(t.TempDir(), "token")
	a := &authService{authInfo: AuthInfo{ConnectionToken: "ns", TokenType: "NAMESPACE", IdentityToken: tokenfile.New(p)}}
	require.Empty(t, getSuccess(t, a).GetIdentityToken())
	require.NoError(t, os.WriteFile(p, []byte("t1\n"), 0o600))
	require.Equal(t, "t1", getSuccess(t, a).GetIdentityToken())
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
