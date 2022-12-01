package ssh

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

type SSHConfig struct {
	SshUser    string
	SshHost    string
	SshPort    int
	PrivateKey []byte
}

func (conf *SSHConfig) EncodeWithDSN(base string) (string, error) {

	parsed, err := url.Parse(base)
	if err != nil {
		return "", fmt.Errorf("url parsing the base url: %s, %s", base, err.Error())
	}

	values := parsed.Query()
	values.Add("ssh_private_key", string(conf.PrivateKey))

	parsed.RawQuery = values.Encode()
	updatedBase := parsed.String()
	split := strings.Split(updatedBase, "://")

	return fmt.Sprintf(
		"%s://%s@%s:%d/%s", split[0], conf.SshUser, conf.SshHost, conf.SshPort, split[1]), nil
}

func (conf *SSHConfig) DecodeFromDSN(encodedDSN string) (dsn string, err error) {

	parsed, err := url.Parse(encodedDSN)
	if err != nil {
		return "", fmt.Errorf("parsing the encoded dsn: %s, %s", encodedDSN, err.Error())
	}

	conf.SshUser = parsed.User.Username()
	conf.SshHost = parsed.Hostname()
	conf.SshPort, _ = strconv.Atoi(parsed.Port())
	conf.PrivateKey = []byte(parsed.Query().Get("ssh_private_key"))

	values := parsed.Query()
	values.Del("ssh_private_key")

	parsed.RawQuery = values.Encode()

	// remove the middle information of
	// scheme://ssh_user:ssh_password@ssh_host:ssh_port/
	splitted := strings.Split(parsed.String(), "://")
	idx := strings.Index(splitted[1], "/")

	if idx == -1 {
		return "", fmt.Errorf("fetching index of / to start reading the warehouse dsn: %s", err.Error())
	}

	return fmt.Sprintf("%s://%s", splitted[0], splitted[1][idx+1:]), nil
}
