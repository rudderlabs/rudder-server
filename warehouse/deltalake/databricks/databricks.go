package databricks

import (
	"context"
	"github.com/rudderlabs/rudder-server/proto/databricks"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"google.golang.org/grpc"
)

var (
	pkgLogger logger.LoggerI
)

type CredentialsT struct {
	Host  string
	Port  string
	Path  string
	Token string
}

type DBHandleT struct {
	CredIdentifier string
	Context        context.Context
	Conn           *grpc.ClientConn
	Client         proto.DatabricksClient
}

func Init() {
	pkgLogger = logger.NewLogger().Child("warehouse").Child("databricks")
}

// Close closes sql connection as well as closes grpc connection
func (dbT *DBHandleT) Close() {
	closeConnectionResponse, err := dbT.Client.Close(dbT.Context, &proto.CloseRequest{
		Identifier: dbT.CredIdentifier,
	})
	if err != nil {
		pkgLogger.Errorf("Error closing connection in delta lake: %v", err)
	}
	if closeConnectionResponse.GetErrorCode() != "" {
		pkgLogger.Errorf("Error closing connection in delta lake with response: %v", err, closeConnectionResponse.GetErrorMessage())
	}
	dbT.Conn.Close()
}
