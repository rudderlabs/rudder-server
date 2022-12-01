package ssh

type SSHTunnelConfig struct {
	SshUser    string
	SshHost    string
	SshPort    int
	RemoteHost string
	RemotePort int
	PrivateKey []byte
	LocalPort  int
}
