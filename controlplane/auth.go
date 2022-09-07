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

func (a *authService) GetConnectionToken(_ context.Context, _ *proto.GetConnectionTokenRequest) (*proto.GetConnectionTokenResponse, error) {
	if a.authInfo.ConnectionToken != "" {
		return &proto.GetConnectionTokenResponse{
			Response: &proto.GetConnectionTokenResponse_ErrorResponse{
				ErrorResponse: &proto.ErrorResponse{
					Error: "connection token is empty",
				},
			},
		}, nil
	}
	return &proto.GetConnectionTokenResponse{
		Response: &proto.GetConnectionTokenResponse_SuccessResponse{
			SuccessResponse: &proto.SuccessResponse{
				ConnectionToken: a.authInfo.ConnectionToken,
				Service:         a.authInfo.Service,
				InstanceID:      a.authInfo.InstanceID,
			},
		},
	}, nil
}

func (a *authService) GetWorkspaceToken(ctx context.Context, request *proto.GetWorkspaceTokenRequest) (*proto.GetWorkspaceTokenResponse, error) {
	return &proto.GetWorkspaceTokenResponse{
		WorkspaceToken: a.authInfo.ConnectionToken,
		Service:        a.authInfo.Service,
		InstanceID:     a.authInfo.InstanceID,
	}, nil
}
