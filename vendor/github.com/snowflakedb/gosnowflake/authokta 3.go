// Copyright (c) 2017-2019 Snowflake Computing Inc. All right reserved.

package gosnowflake

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"html"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/google/uuid"
)

type authOKTARequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type authOKTAResponse struct {
	CookieToken string `json:"cookieToken"`
}

/*
authenticateBySAML authenticates a user by SAML
SAML Authentication
1.  query GS to obtain IDP token and SSO url
2.  IMPORTANT Client side validation:
	validate both token url and sso url contains same prefix
	(protocol + host + port) as the given authenticator url.
	Explanation:
	This provides a way for the user to 'authenticate' the IDP it is
	sending his/her credentials to.  Without such a check, the user could
	be coerced to provide credentials to an IDP impersonator.
3.  query IDP token url to authenticate and retrieve access token
4.  given access token, query IDP URL snowflake app to get SAML response
5.  IMPORTANT Client side validation:
	validate the post back url come back with the SAML response
	contains the same prefix as the Snowflake's server url, which is the
	intended destination url to Snowflake.
Explanation:
	This emulates the behavior of IDP initiated login flow in the user
	browser where the IDP instructs the browser to POST the SAML
	assertion to the specific SP endpoint.  This is critical in
	preventing a SAML assertion issued to one SP from being sent to
	another SP.
*/
func authenticateBySAML(
	sr *snowflakeRestful,
	oktaURL *url.URL,
	application string,
	account string,
	user string,
	password string,
) (samlResponse []byte, err error) {
	glog.V(2).Info("step 1: query GS to obtain IDP token and SSO url")
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
		ClientAppID:       clientType,
		ClientAppVersion:  SnowflakeGoDriverVersion,
		AccountName:       account,
		ClientEnvironment: clientEnvironment,
		Authenticator:     oktaURL.String(),
	}
	authRequest := authRequest{
		Data: requestMain,
	}
	params := &url.Values{}
	jsonBody, err := json.Marshal(authRequest)
	if err != nil {
		return nil, err
	}
	glog.V(2).Infof("PARAMS for Auth: %v, %v", params, sr)
	respd, err := sr.FuncPostAuthSAML(sr, headers, jsonBody, sr.LoginTimeout)
	if err != nil {
		return nil, err
	}
	if !respd.Success {
		glog.V(1).Infoln("Authentication FAILED")
		glog.Flush()
		sr.Token = ""
		sr.MasterToken = ""
		sr.SessionID = -1
		code, err := strconv.Atoi(respd.Code)
		if err != nil {
			code = -1
			return nil, err
		}
		return nil, &SnowflakeError{
			Number:   code,
			SQLState: SQLStateConnectionRejected,
			Message:  respd.Message,
		}
	}
	glog.V(2).Info("step 2: validate Token and SSO URL has the same prefix as oktaURL")
	var tokenURL *url.URL
	var ssoURL *url.URL
	if tokenURL, err = url.Parse(respd.Data.TokenURL); err != nil {
		return nil, fmt.Errorf("failed to parse token URL. %v", respd.Data.TokenURL)
	}
	if ssoURL, err = url.Parse(respd.Data.TokenURL); err != nil {
		return nil, fmt.Errorf("failed to parse ssoURL URL. %v", respd.Data.SSOURL)
	}
	if !isPrefixEqual(oktaURL, ssoURL) || !isPrefixEqual(oktaURL, tokenURL) {
		return nil, &SnowflakeError{
			Number:      ErrCodeIdpConnectionError,
			SQLState:    SQLStateConnectionRejected,
			Message:     errMsgIdpConnectionError,
			MessageArgs: []interface{}{oktaURL, respd.Data.TokenURL, respd.Data.SSOURL},
		}
	}
	glog.V(2).Info("step 3: query IDP token url to authenticate and retrieve access token")
	jsonBody, err = json.Marshal(authOKTARequest{
		Username: user,
		Password: password,
	})
	if err != nil {
		return nil, err
	}
	respa, err := sr.FuncPostAuthOKTA(sr, headers, jsonBody, respd.Data.TokenURL, sr.LoginTimeout)
	if err != nil {
		return nil, err
	}

	glog.V(2).Info("step 4: query IDP URL snowflake app to get SAML response")
	params = &url.Values{}
	params.Add("RelayState", "/some/deep/link")
	params.Add("onetimetoken", respa.CookieToken)

	headers = make(map[string]string)
	headers["accept"] = "*/*"
	bd, err := sr.FuncGetSSO(sr, params, headers, respd.Data.SSOURL, sr.LoginTimeout)
	if err != nil {
		return nil, err
	}
	glog.V(2).Info("step 5: validate post_back_url matches Snowflake URL")
	tgtURL, err := postBackURL(bd)
	if err != nil {
		return nil, err
	}

	fullURL := sr.getURL()
	glog.V(2).Infof("tgtURL: %v, origURL: %v", tgtURL, fullURL)
	if !isPrefixEqual(tgtURL, fullURL) {
		return nil, &SnowflakeError{
			Number:      ErrCodeSSOURLNotMatch,
			SQLState:    SQLStateConnectionRejected,
			Message:     errMsgSSOURLNotMatch,
			MessageArgs: []interface{}{tgtURL, fullURL},
		}
	}
	return bd, nil
}

func postBackURL(htmlData []byte) (url *url.URL, err error) {
	idx0 := bytes.Index(htmlData, []byte("<form"))
	if idx0 < 0 {
		return nil, fmt.Errorf("failed to find a form tag in HTML response: %v", htmlData)
	}
	idx := bytes.Index(htmlData[idx0:], []byte("action=\""))
	if idx < 0 {
		return nil, fmt.Errorf("failed to find action field in HTML response: %v", htmlData[idx0:])
	}
	idx += idx0
	endIdx := bytes.Index(htmlData[idx+8:], []byte("\""))
	if endIdx < 0 {
		return nil, fmt.Errorf("failed to find the end of action field: %v", htmlData[idx+8:])
	}
	r := html.UnescapeString(string(htmlData[idx+8 : idx+8+endIdx]))
	return url.Parse(r)
}

func isPrefixEqual(u1 *url.URL, u2 *url.URL) bool {
	p1 := u1.Port()
	if p1 == "" && u1.Scheme == "https" {
		p1 = "443"
	}
	p2 := u1.Port()
	if p2 == "" && u1.Scheme == "https" {
		p2 = "443"
	}
	return u1.Hostname() == u2.Hostname() && p1 == p2 && u1.Scheme == u2.Scheme
}

// Makes a request to /session/authenticator-request to get SAML Information,
// such as the IDP Url and Proof Key, depending on the authenticator
func postAuthSAML(
	sr *snowflakeRestful,
	headers map[string]string,
	body []byte,
	timeout time.Duration) (
	data *authResponse, err error) {

	params := &url.Values{}
	params.Add(requestIDKey, uuid.New().String())
	fullURL := sr.getFullURL(authenticatorRequestPath, params)

	glog.V(2).Infof("fullURL: %v", fullURL)
	resp, err := sr.FuncPost(context.TODO(), sr, fullURL, headers, body, timeout, true)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		glog.V(2).Infof("postAuthSAML: resp: %v", resp)
		var respd authResponse
		err = json.NewDecoder(resp.Body).Decode(&respd)
		if err != nil {
			glog.V(1).Infof("failed to decode JSON. err: %v", err)
			glog.Flush()
			return nil, err
		}
		return &respd, nil
	}
	switch resp.StatusCode {
	case http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		// service availability or connectivity issue. Most likely server side issue.
		return nil, &SnowflakeError{
			Number:      ErrCodeServiceUnavailable,
			SQLState:    SQLStateConnectionWasNotEstablished,
			Message:     errMsgServiceUnavailable,
			MessageArgs: []interface{}{resp.StatusCode, fullURL},
		}
	case http.StatusUnauthorized, http.StatusForbidden:
		// failed to connect to db. account name may be wrong
		return nil, &SnowflakeError{
			Number:      ErrCodeFailedToConnect,
			SQLState:    SQLStateConnectionRejected,
			Message:     errMsgFailedToConnect,
			MessageArgs: []interface{}{resp.StatusCode, fullURL},
		}
	}
	_, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		glog.V(1).Infof("failed to extract HTTP response body. err: %v", err)
		glog.Flush()
		return nil, err
	}
	glog.Flush()
	return nil, &SnowflakeError{
		Number:      ErrFailedToAuthSAML,
		SQLState:    SQLStateConnectionRejected,
		Message:     errMsgFailedToAuthSAML,
		MessageArgs: []interface{}{resp.StatusCode, fullURL},
	}
}

func postAuthOKTA(
	sr *snowflakeRestful,
	headers map[string]string,
	body []byte,
	fullURL string,
	timeout time.Duration) (
	data *authOKTAResponse, err error) {
	glog.V(2).Infof("fullURL: %v", fullURL)
	targetURL, err := url.Parse(fullURL)
	if err != nil {
		return nil, err
	}
	resp, err := sr.FuncPost(context.TODO(), sr, targetURL, headers, body, timeout, false)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		glog.V(2).Infof("postAuthOKTA: resp: %v", resp)
		var respd authOKTAResponse
		err = json.NewDecoder(resp.Body).Decode(&respd)
		if err != nil {
			glog.V(1).Infof("failed to decode JSON. err: %v", err)
			glog.Flush()
			return nil, err
		}
		return &respd, nil
	}
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		glog.V(1).Infof("failed to extract HTTP response body. err: %v", err)
		glog.Flush()
		return nil, err
	}
	glog.V(1).Infof("HTTP: %v, URL: %v, Body: %v", resp.StatusCode, fullURL, b)
	glog.V(1).Infof("Header: %v", resp.Header)
	glog.Flush()
	return nil, &SnowflakeError{
		Number:      ErrFailedToAuthOKTA,
		SQLState:    SQLStateConnectionRejected,
		Message:     errMsgFailedToAuthOKTA,
		MessageArgs: []interface{}{resp.StatusCode, fullURL},
	}
}

func getSSO(
	sr *snowflakeRestful,
	params *url.Values,
	headers map[string]string,
	ssoURL string,
	timeout time.Duration) (
	bd []byte, err error) {
	fullURL, err := url.Parse(ssoURL)
	if err != nil {
		return nil, err
	}
	fullURL.RawQuery = params.Encode()
	glog.V(2).Infof("fullURL: %v", fullURL)
	resp, err := sr.FuncGet(context.TODO(), sr, fullURL, headers, timeout)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		glog.V(1).Infof("failed to extract HTTP response body. err: %v", err)
		glog.Flush()
		return nil, err
	}
	if resp.StatusCode == http.StatusOK {
		glog.V(2).Infof("getSSO: resp: %v", resp)
		return b, nil
	}
	glog.V(1).Infof("HTTP: %v, URL: %v, Body: %v", resp.StatusCode, fullURL, b)
	glog.V(1).Infof("Header: %v", resp.Header)
	glog.Flush()
	return nil, &SnowflakeError{
		Number:      ErrFailedToGetSSO,
		SQLState:    SQLStateConnectionRejected,
		Message:     errMsgFailedToGetSSO,
		MessageArgs: []interface{}{resp.StatusCode, fullURL},
	}
}
