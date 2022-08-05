package controlplane

import (
	"context"

	proto "github.com/rudderlabs/rudder-server/proto/common"
)

type AuthInfo struct {
	Service              string
	ConnectionIdentifier string
	InstanceID           string
}

type authService struct {
	authInfo AuthInfo
	proto.UnimplementedDPAuthServiceServer
}

func (a *authService) GetWorkspaceToken(ctx context.Context, request *proto.GetConnectionIdentifierRequest) (*proto.GetConnectionIdentifierResponse, error) {
	return &proto.GetConnectionIdentifierResponse{
		ConnectionIdentifier: a.authInfo.ConnectionIdentifier,
		Service:              a.authInfo.Service,
		InstanceID:           a.authInfo.InstanceID,
	}, nil
}
