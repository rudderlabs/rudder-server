package client

import (
	"context"

	"github.com/rudderlabs/rudder-go-kit/logger"
	proto "github.com/rudderlabs/rudder-server/proto/databricks"
	"google.golang.org/grpc"
)

type Credentials struct {
	Host  string
	Port  string
	Path  string
	Token string
}

type Client struct {
	Logger         logger.Logger
	CredConfig     *proto.ConnectionConfig
	CredIdentifier string
	Conn           *grpc.ClientConn
	Client         proto.DatabricksClient
}

// Close closes sql connection as well as closes grpc connection
func (client *Client) Close(ctx context.Context) {
	closeConnectionResponse, err := client.Client.Close(ctx, &proto.CloseRequest{
		Config:     client.CredConfig,
		Identifier: client.CredIdentifier,
	})
	if err != nil {
		client.Logger.Errorf("Error closing connection in delta lake: %v", err)
	}
	if closeConnectionResponse.GetErrorCode() != "" {
		client.Logger.Errorf("Error closing connection in delta lake with response: %v", err, closeConnectionResponse.GetErrorMessage())
	}
	_ = client.Conn.Close()
}
