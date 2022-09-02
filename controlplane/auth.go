package controlplane

import (
	"context"

	proto "github.com/rudderlabs/rudder-server/proto/common"
)

type AuthInfo struct {
	Service         string
	ConnectionToken string
	InstanceID      string
}

type authService struct {
	authInfo AuthInfo
	proto.UnimplementedDPAuthServiceServer
}

func (a *authService) GetConnectionToken(ctx context.Context, request *proto.GetConnectionTokenRequest) (*proto.GetConnectionTokenResponse, error) {
	return &proto.GetConnectionTokenResponse{
		ConnectionToken: a.authInfo.ConnectionToken,
		Service:         a.authInfo.Service,
		InstanceID:      a.authInfo.InstanceID,
	}, nil
}

func (a *authService) GetWorkspaceToken(ctx context.Context, request *proto.GetWorkspaceTokenRequest) (*proto.GetWorkspaceTokenResponse, error) {
	return &proto.GetWorkspaceTokenResponse{
		WorkspaceToken: a.authInfo.ConnectionToken,
		Service:        a.authInfo.Service,
		InstanceID:     a.authInfo.InstanceID,
	}, nil
}
