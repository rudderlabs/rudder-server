package client

import (
	"log"
	"net/rpc"
	"os"
	"path/filepath"
)

// Setup Unix Domain socket client to communicate with
// the rudder server running on the same machine as this cli
func GetUDSClient() *rpc.Client {
	tmpdirPath := os.Getenv("RUDDER_TMPDIR")
	if tmpdirPath == "" {
		tmpdirPath = "/tmp"
	}
	client, err := rpc.DialHTTP("unix", filepath.Join(tmpdirPath, "rudder-server.sock"))
	if err != nil {
		log.Fatal("dialing:", err)
	}
	return client
}
