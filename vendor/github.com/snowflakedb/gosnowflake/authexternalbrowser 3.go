package gosnowflake

//
// Copyright (c) 2019 Snowflake Computing Inc. All right reserved.
//

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/pkg/browser"
)

const (
	successHTML = `<!DOCTYPE html><html><head><meta charset="UTF-8"/>
<title>SAML Response for Snowflake</title></head>
<body>
Your identity was confirmed and propagated to Snowflake %v.
You can close this window now and go back where you started from.
</body></html>`
)

const (
	bufSize = 8192
)

// Builds a response to show to the user after successfully
// getting a response from Snowflake.
func buildResponse(application string) bytes.Buffer {
	body := fmt.Sprintf(successHTML, application)
	t := &http.Response{
		Status:        "200 OK",
		StatusCode:    200,
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Body:          ioutil.NopCloser(bytes.NewBufferString(body)),
		ContentLength: int64(len(body)),
		Request:       nil,
		Header:        make(http.Header),
	}
	var b bytes.Buffer
	t.Write(&b)
	return b
}

// This opens a socket that listens on all available unicast
// and any anycast IP addresses locally. By specifying "0", we are
// able to bind to a free port.
func bindToPort() (net.Listener, error) {
	l, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		glog.V(1).Infof("unable to bind to a port on localhost,  err: %v", err)
		return nil, err
	}
	return l, nil
}

// Opens a browser window (or new tab) with the configured IDP Url.
// This can / will fail if running inside a shell with no display, ie
// ssh'ing into a box attempting to authenticate via external browser.
func openBrowser(idpURL string) error {
	err := browser.OpenURL(idpURL)
	if err != nil {
		glog.V(1).Infof("failed to open a browser. err: %v", err)
		return err
	}
	return nil
}

// Gets the IDP Url and Proof Key from Snowflake.
// Note: FuncPostAuthSaml will return a fully qualified error if
// there is something wrong getting data from Snowflake.
func getIdpURLProofKey(
	sr *snowflakeRestful,
	authenticator string,
	application string,
	account string,
	callbackPort int) (string, string, error) {

	headers := make(map[string]string)
	headers["Content-Type"] = headerContentTypeApplicationJSON
	headers["accept"] = headerContentTypeApplicationJSON
	headers["User-Agent"] = userAgent

	clientEnvironment := authRequestClientEnvironment{
		Application: application,
		Os:          operatingSystem,
		OsVersion:   platform,
	}

	requestMain := authRequestData{
		ClientAppID:             clientType,
		ClientAppVersion:        SnowflakeGoDriverVersion,
		AccountName:             account,
		ClientEnvironment:       clientEnvironment,
		Authenticator:           authenticator,
		BrowserModeRedirectPort: strconv.Itoa(callbackPort),
	}

	authRequest := authRequest{
		Data: requestMain,
	}

	jsonBody, err := json.Marshal(authRequest)
	if err != nil {
		glog.V(1).Infof("failed to serialize json. err: %v", err)
		return "", "", err
	}

	respd, err := sr.FuncPostAuthSAML(sr, headers, jsonBody, sr.LoginTimeout)
	if err != nil {
		return "", "", err
	}
	return respd.Data.SSOURL, respd.Data.ProofKey, nil
}

// The response returned from Snowflake looks like so:
// GET /?token=encodedSamlToken
// Host: localhost:54001
// Connection: keep-alive
// Upgrade-Insecure-Requests: 1
// User-Agent: userAgentStr
// Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8
// Referer: https://myaccount.snowflakecomputing.com/fed/login
// Accept-Encoding: gzip, deflate, br
// Accept-Language: en-US,en;q=0.9
// This extracts the token portion of the response.
func getTokenFromResponse(response string) (string, error) {
	start := "GET /?token="
	arr := strings.Split(response, "\r\n")
	if !strings.HasPrefix(arr[0], start) {
		glog.V(1).Infof("response is malformed. resp: %s", arr[0])
		return "", &SnowflakeError{
			Number:      ErrFailedToParseResponse,
			SQLState:    SQLStateConnectionRejected,
			Message:     errMsgFailedToParseResponse,
			MessageArgs: []interface{}{response},
		}
	}
	token := strings.TrimLeft(arr[0], start)
	token = strings.Split(token, " ")[0]
	return token, nil
}

// Authentication by an external browser takes place via the following:
// - the golang snowflake driver communicates to Snowflake that the user wishes to
//   authenticate via external browser
// - snowflake sends back the IDP Url configured at the Snowflake side for the
//   provided account
// - the default browser is opened to that URL
// - user authenticates at the IDP, and is redirected to Snowflake
// - Snowflake directs the user back to the driver
// - authenticate is complete!
func authenticateByExternalBrowser(
	sr *snowflakeRestful,
	authenticator string,
	application string,
	account string,
	user string,
	password string,
) ([]byte, []byte, error) {
	l, err := bindToPort()
	if err != nil {
		return nil, nil, err
	}
	defer l.Close()

	callbackPort := l.Addr().(*net.TCPAddr).Port
	idpURL, proofKey, err := getIdpURLProofKey(
		sr, authenticator, application, account, callbackPort)
	if err != nil {
		return nil, nil, err
	}

	if err = openBrowser(idpURL); err != nil {
		return nil, nil, err
	}

	var encodedSamlResponse string
	var acceptErr error
	var tokenErr error
	acceptErr = nil
	for {
		conn, err := l.Accept()
		if err != nil {
			glog.V(1).Infof("unable to accept connection. err: %v", err)
			log.Fatal(err)
		}
		go func(c net.Conn) {
			var buf bytes.Buffer
			total := 0
			for {
				b := make([]byte, bufSize)
				n, err := c.Read(b)
				if err != nil {
					if err != io.EOF {
						glog.V(1).Infof("error reading from socket. err: %v", err)
						acceptErr = err
					}
					break
				}
				total += n
				buf.Write(b)
				if n < bufSize {
					// We successfully read all data
					s := string(buf.Bytes()[:total])
					encodedSamlResponse, tokenErr = getTokenFromResponse(s)
					break
				}
				buf.Grow(bufSize)
			}
			if encodedSamlResponse != "" {
				httpResponse := buildResponse(application)
				c.Write(httpResponse.Bytes())
			}
			c.Close()
		}(conn)
		if acceptErr != nil || encodedSamlResponse != "" {
			break
		}
	}

	if tokenErr != nil {
		return nil, nil, tokenErr
	}

	if acceptErr != nil {
		return nil, nil, &SnowflakeError{
			Number:      ErrFailedToGetExternalBrowserResponse,
			SQLState:    SQLStateConnectionRejected,
			Message:     errMsgFailedToGetExternalBrowserResponse,
			MessageArgs: []interface{}{acceptErr},
		}
	}

	escapedSamlResponse, err := url.QueryUnescape(encodedSamlResponse)
	if err != nil {
		glog.V(1).Infof("unable to unescape saml response. err: %v", err)
		return nil, nil, err
	}
	return []byte(escapedSamlResponse), []byte(proofKey), nil
}
