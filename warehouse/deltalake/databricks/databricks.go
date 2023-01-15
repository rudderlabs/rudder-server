package databricks

import (
	"context"

	proto "github.com/rudderlabs/rudder-server/proto/databricks"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"google.golang.org/grpc"
)

var pkgLogger logger.Logger

type Credentials struct {
	Host  string
	Port  string
	Path  string
	Token string
}

type DatabricksClient struct {
	CredConfig     *proto.ConnectionConfig
	CredIdentifier string
	Context        context.Context
	Conn           *grpc.ClientConn
	Client         proto.DatabricksClient
	CloseStats     stats.Measurement
}

func Init() {
	pkgLogger = logger.NewLogger().Child("warehouse").Child("databricks")
}

// Close closes sql connection as well as closes grpc connection
func (client *DatabricksClient) Close() {
	defer client.CloseStats.RecordDuration()()

	closeConnectionResponse, err := client.Client.Close(client.Context, &proto.CloseRequest{
		Config:     client.CredConfig,
		Identifier: client.CredIdentifier,
	})
	if err != nil {
		pkgLogger.Errorf("Error closing connection in delta lake: %v", err)
	}
	if closeConnectionResponse.GetErrorCode() != "" {
		pkgLogger.Errorf("Error closing connection in delta lake with response: %v", err, closeConnectionResponse.GetErrorMessage())
	}
	client.Conn.Close()
}
