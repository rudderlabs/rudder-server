package controlplane

import (
	"crypto/tls"
	"fmt"
	"net"

	"github.com/hashicorp/yamux"
	"google.golang.org/grpc"

	proto "github.com/rudderlabs/rudder-server/proto/common"
)

type ConnHandler struct {
	GRPCServer *grpc.Server
	YamuxSess  *yamux.Session
	logger     LoggerI
}

func (cm *ConnectionManager) establishConnection() (*ConnHandler, error) {
	var conn net.Conn
	var err error
	cm.Logger.Infof(`Establishing connection to %s using TLS:%v`, cm.url, cm.UseTLS)
	if cm.UseTLS {
		conn, err = tls.Dial("tcp", cm.url, &tls.Config{})
	} else {
		conn, err = net.Dial("tcp", cm.url)
	}

	if err != nil {
		return nil, err
	}
	cm.Logger.Infof("connected to url: %s, using tls: %v", cm.url, cm.UseTLS)

	srvConn, err := yamux.Server(conn, yamux.DefaultConfig())
	if err != nil {
		cm.Logger.Errorf("couldn't create yamux server: %s", err.Error())
		return nil, err
	}

	grpcServer := grpc.NewServer(cm.Options...)
	service := &authService{authInfo: cm.AuthInfo}
	proto.RegisterDPAuthServiceServer(grpcServer, service)
	cn := &ConnHandler{
		GRPCServer: grpcServer,
		YamuxSess:  srvConn,
		logger:     cm.Logger,
	}
	return cn, nil
}

func (c *ConnHandler) ServeOnConnection() error {
	c.logger.Info("starting grpc server")
	if err := c.GRPCServer.Serve(c.YamuxSess); err != nil {
		return fmt.Errorf("failed to serve grpc: %w", err)
	}

	return nil
}

func (c *ConnHandler) Close() error {
	c.logger.Info("closing grpc connection")
	c.GRPCServer.GracefulStop()
	if err := c.YamuxSess.Close(); err != nil {
		return fmt.Errorf("failed to close grpc: %w", err)
	}

	return nil
}
