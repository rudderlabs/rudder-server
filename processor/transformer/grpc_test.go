package transformer

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"testing"

	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	transformerpb "github.com/rudderlabs/rudder-server/proto/transformer"
)

type mockGRPCServer struct {
	transformerpb.UnimplementedTransformerServiceServer
}

func (m *mockGRPCServer) Transform(ctx context.Context, req *transformerpb.TransformRequest, opts ...grpc.CallOption) (*transformerpb.TransformResponse, error) {
	return &transformerpb.TransformResponse{}, nil
}

func TestGrpcRequest(t *testing.T) {
	grpcServer := &mockGRPCServer{}

	tcpPort, err := kithelper.GetFreePort()
	require.NoError(t, err)

	tcpAddress := net.JoinHostPort("", strconv.Itoa(tcpPort))

	listener, err := net.Listen("tcp", tcpAddress)
	require.NoError(t, err)

	server := grpc.NewServer(grpc.Creds(insecure.NewCredentials()))
	transformerpb.RegisterTransformerServiceServer(server, grpcServer)

	ctx, stopTest := context.WithCancel(context.Background())
	defer stopTest()

	g, gCtx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return server.Serve(listener)
	})

	grpcClientConn, err := grpc.NewClient(
		fmt.Sprintf("localhost:%d", tcpPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, grpcClientConn.Close())
	})

	grpcClient := transformerpb.NewTransformerServiceClient(grpcClientConn)
	response, err := grpcClient.Transform(gCtx, &transformerpb.TransformRequest{})
	require.NoError(t, err)
}
