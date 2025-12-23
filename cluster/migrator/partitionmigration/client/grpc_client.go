// GRPC client for partition migration
package client

import (
	"context"
	"net"
	"time"

	"google.golang.org/grpc"
	grpcbackoff "google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/config"
	proto "github.com/rudderlabs/rudder-server/proto/cluster"
)

// NewPartitionMigrationClient creates a new client for the PartitionMigration gRPC service
func NewPartitionMigrationClient(target string, conf *config.Config) (PartitionMigrationClient, error) {
	conn, err := grpc.NewClient(target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                conf.GetDurationVar(10, time.Second, "PartitionMigration.Grpc.Client.ClientParameters.Time"),
			Timeout:             conf.GetDurationVar(10, time.Second, "PartitionMigration.Grpc.Client.ClientParameters.Timeout"),
			PermitWithoutStream: conf.GetBoolVar(true, "PartitionMigration.Grpc.Client.ClientParameters.PermitWithoutStream"),
		}),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff: grpcbackoff.Config{
				BaseDelay:  conf.GetDurationVar(1, time.Second, "PartitionMigration.Grpc.Client.Backoff.BaseDelay"),
				Multiplier: conf.GetFloat64Var(1.6, "PartitionMigration.Grpc.Client.Backoff.Multiplier"),
				Jitter:     conf.GetFloat64Var(0.2, "PartitionMigration.Grpc.Client.Backoff.Jitter"),
				MaxDelay:   conf.GetDurationVar(120, time.Second, "PartitionMigration.Grpc.Client.Backoff.MaxDelay"),
			},
			MinConnectTimeout: conf.GetDurationVar(1, time.Second, "PartitionMigration.Grpc.Client.MinConnectTimeout"),
		}),
		grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
			var dialer net.Dialer
			return dialer.DialContext(ctx, "tcp", addr)
		}),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(conf.GetIntVar(int(4*bytesize.MB), 1, "PartitionMigration.Grpc.Client.MaxCallRecvMsgSize")),
			grpc.MaxCallSendMsgSize(conf.GetIntVar(int(200*bytesize.MB), 1, "PartitionMigration.Grpc.Client.MaxCallSendMsgSize")),
		),
	)
	if err != nil {
		return nil, err
	}
	return &partitionMigrator{
		conn:                     conn,
		PartitionMigrationClient: proto.NewPartitionMigrationClient(conn),
	}, nil
}

// PartitionMigrationClient is a wrapper around the generated gRPC client
type PartitionMigrationClient interface {
	proto.PartitionMigrationClient
	Close() error
}

type partitionMigrator struct {
	conn *grpc.ClientConn
	proto.PartitionMigrationClient
}

func (c *partitionMigrator) Close() error {
	return c.conn.Close()
}
