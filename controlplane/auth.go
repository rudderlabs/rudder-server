package controlplane

import (
	"context"

	"github.com/rudderlabs/rudder-cp-sdk/tokenfile"

	proto "github.com/rudderlabs/rudder-server/proto/common"
)

type AuthInfo struct {
	Service         string
	ConnectionToken string
	InstanceID      string
	TokenType       string
	Labels          map[string]string
	// IdentityToken is read on each handshake; nil means send none.
	IdentityToken *tokenfile.Token
}

type authService struct {
	authInfo AuthInfo
	proto.UnimplementedDPAuthServiceServer
}

func (a *authService) GetConnectionToken(_ context.Context, _ *proto.GetConnectionTokenRequest) (*proto.GetConnectionTokenResponse, error) {
	if a.authInfo.ConnectionToken == "" {
		return &proto.GetConnectionTokenResponse{
			Response: &proto.GetConnectionTokenResponse_ErrorResponse{
				ErrorResponse: &proto.ErrorResponse{
					Error: "connection token is empty",
				},
			},
		}, nil
	}
	success := &proto.GetConnectionTokenSuccessResponse{
		ConnectionToken: a.authInfo.ConnectionToken,
		Service:         a.authInfo.Service,
		InstanceID:      a.authInfo.InstanceID,
		TokenType:       a.authInfo.TokenType,
		Labels:          a.authInfo.Labels,
	}
	if a.authInfo.IdentityToken != nil {
		success.IdentityToken = a.authInfo.IdentityToken.Get()
	}
	return &proto.GetConnectionTokenResponse{
		Response: &proto.GetConnectionTokenResponse_SuccessResponse{SuccessResponse: success},
	}, nil
}

func (a *authService) GetWorkspaceToken(_ context.Context, _ *proto.GetWorkspaceTokenRequest) (*proto.GetWorkspaceTokenResponse, error) {
	return &proto.GetWorkspaceTokenResponse{
		WorkspaceToken: a.authInfo.ConnectionToken,
		Service:        a.authInfo.Service,
		InstanceID:     a.authInfo.InstanceID,
	}, nil
}
