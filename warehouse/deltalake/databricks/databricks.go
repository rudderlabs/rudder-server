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
	Cred           *CredentialsT
	CredIdentifier string
	Context        context.Context
	Conn           *grpc.ClientConn
	Client         proto.DatabricksClient
}

func Init() {
	pkgLogger = logger.NewLogger().Child("warehouse").Child("databricks")
}

// Close closes grpc connection and sql connection as well.
func (dbT *DBHandleT) Close() {
	closeConnectionResponse, err := dbT.Client.Close(dbT.Context, &proto.ConnectionRequest{
		Host:           dbT.Cred.Host,
		Port:           dbT.Cred.Port,
		Pwd:            dbT.Cred.Token,
		HttpPath:       dbT.Cred.Path,
		Identifier:     dbT.CredIdentifier,
		UserAgentEntry: "RudderStack",
	})
	if err != nil {
		pkgLogger.Errorf("Error closing connection in delta lake: %v", err)
	}
	if len(closeConnectionResponse.GetError()) != 0 {
		pkgLogger.Errorf("Error closing connection in delta lake with response: %v", err)
	}
	dbT.Conn.Close()
}
