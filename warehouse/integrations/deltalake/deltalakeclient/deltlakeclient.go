package deltalakeclient

import (
	"context"

	proto "github.com/rudderlabs/rudder-server/proto/databricks"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"google.golang.org/grpc"
)

type Credentials struct {
	Host  string
	Port  string
	Path  string
	Token string
}

type DeltalakeClient struct {
	Logger         logger.Logger
	CredConfig     *proto.ConnectionConfig
	CredIdentifier string
	Context        context.Context
	Conn           *grpc.ClientConn
	Client         proto.DatabricksClient
	CloseStats     stats.Measurement
}

// Close closes sql connection as well as closes grpc connection
func (client *DeltalakeClient) Close() {
	defer client.CloseStats.RecordDuration()()

	closeConnectionResponse, err := client.Client.Close(client.Context, &proto.CloseRequest{
		Config:     client.CredConfig,
		Identifier: client.CredIdentifier,
	})
	if err != nil {
		client.Logger.Errorf("Error closing connection in delta lake: %v", err)
	}
	if closeConnectionResponse.GetErrorCode() != "" {
		client.Logger.Errorf("Error closing connection in delta lake with response: %v", err, closeConnectionResponse.GetErrorMessage())
	}
	client.Conn.Close()
}
