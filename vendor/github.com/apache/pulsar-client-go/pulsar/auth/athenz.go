// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package auth

import (
	"crypto/tls"
	"encoding/base64"
	"errors"
	"net/http"
	"os"
	"regexp"
	"strings"
	"time"

	zms "github.com/AthenZ/athenz/libs/go/zmssvctoken"
	zts "github.com/AthenZ/athenz/libs/go/ztsroletoken"
)

const (
	minExpire         = 2 * time.Hour
	maxExpire         = 24 * time.Hour
	defaultKeyID      = "0"
	defaultRoleHeader = "Athenz-Role-Auth"
)

type athenzAuthProvider struct {
	providerDomain          string
	tenantDomain            string
	tenantService           string
	privateKey              string
	keyID                   string
	x509CertChain           string
	caCert                  string
	principalHeader         string
	roleHeader              string
	ztsURL                  string
	tokenBuilder            zms.TokenBuilder
	roleToken               zts.RoleToken
	zmsNewTokenBuilder      func(domain, name string, privateKeyPEM []byte, keyVersion string) (zms.TokenBuilder, error)
	ztsNewRoleToken         func(tok zms.Token, domain string, opts zts.RoleTokenOptions) zts.RoleToken
	ztsNewRoleTokenFromCert func(certFile, keyFile, domain string, opts zts.RoleTokenOptions) zts.RoleToken

	T http.RoundTripper
}

type parsedURI struct {
	Scheme                   string
	MediaTypeAndEncodingType string
	Data                     string
	Path                     string
}

func NewAuthenticationAthenzWithParams(params map[string]string) (Provider, error) {
	return NewAuthenticationAthenz(
		params["providerDomain"],
		params["tenantDomain"],
		params["tenantService"],
		params["privateKey"],
		params["keyId"],
		params["x509CertChain"],
		params["caCert"],
		params["principalHeader"],
		params["roleHeader"],
		params["ztsUrl"],
	), nil
}

func NewAuthenticationAthenz(
	providerDomain string,
	tenantDomain string,
	tenantService string,
	privateKey string,
	keyID string,
	x509CertChain string,
	caCert string,
	principalHeader string,
	roleHeader string,
	ztsURL string) Provider {
	fixedKeyID := defaultKeyID
	if keyID != "" {
		fixedKeyID = keyID
	}

	fixedRoleHeader := defaultRoleHeader
	if roleHeader != "" {
		fixedRoleHeader = roleHeader
	}

	ztsNewRoleToken := func(tok zms.Token, domain string, opts zts.RoleTokenOptions) zts.RoleToken {
		return zts.RoleToken(zts.NewRoleToken(tok, domain, opts))
	}

	ztsNewRoleTokenFromCert := func(certFile, keyFile, domain string, opts zts.RoleTokenOptions) zts.RoleToken {
		return zts.RoleToken(zts.NewRoleTokenFromCert(certFile, keyFile, domain, opts))
	}

	return &athenzAuthProvider{
		providerDomain:          providerDomain,
		tenantDomain:            tenantDomain,
		tenantService:           tenantService,
		privateKey:              privateKey,
		keyID:                   fixedKeyID,
		x509CertChain:           x509CertChain,
		caCert:                  caCert,
		principalHeader:         principalHeader,
		roleHeader:              fixedRoleHeader,
		ztsURL:                  strings.TrimSuffix(ztsURL, "/"),
		zmsNewTokenBuilder:      zms.NewTokenBuilder,
		ztsNewRoleToken:         ztsNewRoleToken,
		ztsNewRoleTokenFromCert: ztsNewRoleTokenFromCert,
	}
}

func (p *athenzAuthProvider) Init() error {
	if p.providerDomain == "" || p.privateKey == "" || p.ztsURL == "" {
		return errors.New("missing required parameters")
	}

	var roleToken zts.RoleToken
	opts := zts.RoleTokenOptions{
		BaseZTSURL: p.ztsURL + "/zts/v1",
		MinExpire:  minExpire,
		MaxExpire:  maxExpire,
		AuthHeader: p.principalHeader,
	}

	if p.x509CertChain != "" {
		// use Copper Argos
		certURISt := parseURI(p.x509CertChain)
		keyURISt := parseURI(p.privateKey)

		if certURISt.Scheme != "file" || keyURISt.Scheme != "file" {
			return errors.New("x509CertChain and privateKey must be specified as file paths")
		}

		if p.caCert != "" {
			caCertData, err := loadPEM(p.caCert)
			if err != nil {
				return err
			}
			opts.CACert = caCertData
		}

		roleToken = p.ztsNewRoleTokenFromCert(certURISt.Path, keyURISt.Path, p.providerDomain, opts)
	} else {
		if p.tenantDomain == "" || p.tenantService == "" {
			return errors.New("missing required parameters")
		}

		keyData, err := loadPEM(p.privateKey)
		if err != nil {
			return err
		}

		tb, err := p.zmsNewTokenBuilder(p.tenantDomain, p.tenantService, keyData, p.keyID)
		if err != nil {
			return err
		}
		p.tokenBuilder = tb

		roleToken = p.ztsNewRoleToken(p.tokenBuilder.Token(), p.providerDomain, opts)
	}

	p.roleToken = roleToken
	return nil
}

func (p *athenzAuthProvider) Name() string {
	return "athenz"
}

func (p *athenzAuthProvider) GetTLSCertificate() (*tls.Certificate, error) {
	return nil, nil
}

func (p *athenzAuthProvider) GetData() ([]byte, error) {
	tok, err := p.roleToken.RoleTokenValue()
	if err != nil {
		return nil, err
	}

	return []byte(tok), nil
}

func (p *athenzAuthProvider) Close() error {
	return nil
}

func parseURI(uri string) parsedURI {
	var uriSt parsedURI
	// scheme mediatype[;base64] path file
	const expression = `^(?:([^:/?#]+):)(?:([;/\\\-\w]*),)?(?:/{0,2}((?:[^?#/]*/)*))?([^?#]*)`

	// when expression cannot be parsed, then panics
	re := regexp.MustCompile(expression)
	if re.MatchString(uri) {
		groups := re.FindStringSubmatch(uri)
		uriSt.Scheme = groups[1]
		uriSt.MediaTypeAndEncodingType = groups[2]
		uriSt.Data = groups[4]
		uriSt.Path = groups[3] + groups[4]
	} else {
		// consider a file path specified instead of a URI
		uriSt.Scheme = "file"
		uriSt.Path = uri
	}

	return uriSt
}

func loadPEM(uri string) ([]byte, error) {
	uriSt := parseURI(uri)
	var pemData []byte

	if uriSt.Scheme == "data" {
		if uriSt.MediaTypeAndEncodingType != "application/x-pem-file;base64" {
			return nil, errors.New("Unsupported mediaType or encodingType: " + uriSt.MediaTypeAndEncodingType)
		}
		pem, err := base64.StdEncoding.DecodeString(uriSt.Data)
		if err != nil {
			return nil, err
		}
		pemData = pem
	} else if uriSt.Scheme == "file" {
		pem, err := os.ReadFile(uriSt.Path)
		if err != nil {
			return nil, err
		}
		pemData = pem
	} else {
		return nil, errors.New("Unsupported URI Scheme: " + uriSt.Scheme)
	}

	return pemData, nil
}

func (p *athenzAuthProvider) RoundTrip(req *http.Request) (*http.Response, error) {
	tok, err := p.roleToken.RoleTokenValue()
	if err != nil {
		return nil, err
	}
	req.Header.Add(p.roleHeader, tok)
	return p.T.RoundTrip(req)
}

func (p *athenzAuthProvider) Transport() http.RoundTripper {
	return p.T
}

func (p *athenzAuthProvider) WithTransport(tripper http.RoundTripper) error {
	p.T = tripper
	return nil
}
