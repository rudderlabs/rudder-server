package controlplane

import (
	"context"

	proto "github.com/rudderlabs/rudder-server/proto/common"
)

type AuthInfo struct {
	Service        string
	WorkspaceToken string
	InstanceID     string
}

type authService struct {
	authInfo AuthInfo
	proto.UnimplementedDPAuthServiceServer
}

func (a *authService) GetWorkspaceToken(ctx context.Context, request *proto.GetWorkspaceTokenRequest) (*proto.GetWorkspaceTokenResponse, error) {
	return &proto.GetWorkspaceTokenResponse{
		WorkspaceToken: a.authInfo.WorkspaceToken,
		Service:        a.authInfo.Service,
		InstanceID:     a.authInfo.InstanceID,
	}, nil
}
