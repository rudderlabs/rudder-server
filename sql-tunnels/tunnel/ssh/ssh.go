package ssh

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"golang.org/x/crypto/ssh"
)

type SSHTunnelConfig struct {
	SshUser    string
	SshHost    string
	SshPort    int
	RemoteHost string
	RemotePort int
	PrivateKey []byte
	LocalPort  int
}

type SSHTunnel struct {
	// TODO: Should we store some datapoints internally with us as well ?
	tunnel *ImportedSSHTunnel
}

func NewSSHTunnel(config *SSHTunnelConfig) (*SSHTunnel, error) {
	key, err := ssh.ParsePrivateKey(config.PrivateKey)
	if err != nil {
		return nil, fmt.Errorf("parsing private key: %s", err.Error())
	}

	auth := ssh.PublicKeys(key)

	response := &SSHTunnel{
		tunnel: NewImportedSSHTunnel(
			fmt.Sprintf("%s@%s:%d", config.SshUser, config.SshHost, config.SshPort),
			auth,
			fmt.Sprintf("%s:%d", config.RemoteHost, config.RemotePort), "0"),
	}

	response.tunnel.Log = log.New(os.Stdout, "", log.Ldate|log.Lmicroseconds)
	return response, nil
}

func (t *SSHTunnel) Open(ctx context.Context) error {
	fmt.Println("Received a call to open the tunnel")

	go t.tunnel.Start(ctx)
	time.Sleep(100 * time.Millisecond)

	return nil
}

func (t *SSHTunnel) Close(context.Context) error {
	fmt.Println("Received a call to close the sql tunnel")
	t.tunnel.Close()
	return nil
}

func (t *SSHTunnel) LocalConnectionString() string {
	return t.tunnel.Local.String()
}
