// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache version 2.0 license. See LICENSE file for terms.

// Package zmssvctoken produces and validates ntokens given appropriate keys.
// It can only produce service tokens but can validate any principal token.
package zmssvctoken

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	defaultTokenVersion = "S1"
)

var expirationDrift = 10 * time.Minute

const (
	tagVersion        = "v"
	tagDomain         = "d"
	tagName           = "n"
	tagKeyVersion     = "k"
	tagHostname       = "h"
	tagIP             = "i"
	tagGenerationTime = "t"
	tagExpireTime     = "e"
	tagSalt           = "a"
	tagSignature      = "s"
	tagKeyService     = "z"
)

// NToken provides access to useful fields in an ntoken.
type NToken struct {
	Version        string    // the token version e.g. S1, U1
	Domain         string    // domain for which token is valid
	Name           string    // local principal name
	KeyVersion     string    // key version as registered in Athenz
	KeyService     string    // optional key service
	Hostname       string    // optional hostname
	IPAddress      string    // optional IP address
	GenerationTime time.Time // time token was generated
	ExpiryTime     time.Time // time token expires
}

// PrincipalName returns the fully qualified principal name for the token.
func (n *NToken) PrincipalName() string {
	return n.Domain + "." + n.Name
}

// IsExpired is a convenience function to check token expiry.
func (n *NToken) IsExpired() bool {
	return n.ExpiryTime.Before(time.Now())
}

func (n *NToken) almostExpired() bool {
	return time.Now().After(n.ExpiryTime.Add(-1 * expirationDrift))
}

func (n *NToken) toSignedToken(s Signer, expiration time.Duration) (string, error) {

	n.GenerationTime = time.Now()
	n.ExpiryTime = n.GenerationTime.Add(expiration)

	var parts []string
	add := func(k, v string) {
		parts = append(parts, fmt.Sprintf("%s=%s", k, v))
	}
	add(tagVersion, n.Version)
	add(tagDomain, n.Domain)
	add(tagName, n.Name)
	add(tagKeyVersion, n.KeyVersion)
	if n.KeyService != "" {
		add(tagKeyService, n.KeyService)
	}
	if n.Hostname != "" {
		add(tagHostname, n.Hostname)
	}
	if n.IPAddress != "" {
		add(tagIP, n.IPAddress)
	}
	salt, err := getSalt()
	if err != nil {
		return "", fmt.Errorf("Salt error: %v", err)
	}
	add(tagSalt, salt)

	add(tagGenerationTime, fmt.Sprintf("%d", n.GenerationTime.Unix()))
	add(tagExpireTime, fmt.Sprintf("%d", n.ExpiryTime.Unix()))

	unsignedToken := strings.Join(parts, ";")
	signature, err := s.Sign(unsignedToken)
	if err != nil {
		return "", fmt.Errorf("Signature error: %v", err)
	}
	return unsignedToken + fmt.Sprintf(";%s=", tagSignature) + signature, nil
}

var zeroTime = time.Time{}

func asTime(s, name string) (time.Time, error) {
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return zeroTime, fmt.Errorf("Invalid field value '%s' for field '%s'", s, name)
	}
	return time.Unix(n, 0), nil
}

func (n *NToken) assertValid() error {
	if n.Version == "" {
		return fmt.Errorf("Invalid token: missing version")
	}
	if n.Domain == "" {
		return fmt.Errorf("Invalid token: missing domain")
	}
	if n.Name == "" {
		return fmt.Errorf("Invalid token: missing name")
	}
	if n.KeyVersion == "" {
		return fmt.Errorf("Invalid token: missing key version")
	}
	if n.GenerationTime.IsZero() {
		return fmt.Errorf("Invalid token: missing generation time")
	}
	if n.ExpiryTime.IsZero() {
		return fmt.Errorf("Invalid token: missing expiry time")
	}
	return nil
}

func (n *NToken) load(tok string, verifier Verifier) error {

	delim := fmt.Sprintf(";%s=", tagSignature)
	usig := strings.SplitN(tok, delim, 2)
	if len(usig) != 2 {
		return fmt.Errorf("Token does not have a signature")
	}

	unsignedToken := usig[0]
	signature := usig[1]

	err := verifier.Verify(unsignedToken, signature)
	if err != nil {
		return fmt.Errorf("Invalid token signature")
	}

	parts := strings.Split(unsignedToken, ";")
	for _, part := range parts {
		inner := strings.SplitN(part, "=", 2)
		if len(inner) != 2 {
			return fmt.Errorf("Malformed token field %s", part)
		}
		v := inner[1]
		switch inner[0] {
		case tagVersion:
			n.Version = v
		case tagDomain:
			n.Domain = v
		case tagName:
			n.Name = v
		case tagKeyVersion:
			n.KeyVersion = v
		case tagHostname:
			n.Hostname = v
		case tagIP:
			n.IPAddress = v
		case tagKeyService:
			n.KeyService = v
		case tagGenerationTime:
			if n.GenerationTime, err = asTime(v, tagGenerationTime); err != nil {
				return err
			}
		case tagExpireTime:
			if n.ExpiryTime, err = asTime(v, tagExpireTime); err != nil {
				return err
			}
		case tagSalt, tagSignature:
			// ignore
		default:
			log.Println("Unknown ntoken field: ", v)
		}
	}

	return n.assertValid()
}

func (n *NToken) String() string {
	parts := []string{
		fmt.Sprintf("Version: %v", n.Version),
		fmt.Sprintf("Domain: %v", n.Domain),
		fmt.Sprintf("Name: %v", n.Name),
		fmt.Sprintf("Key version: %v", n.KeyVersion),
		fmt.Sprintf("Key service: %v", n.KeyService),
		fmt.Sprintf("Hostname: %v", n.Hostname),
		fmt.Sprintf("I/P: %v", n.IPAddress),
		fmt.Sprintf("Created on: %v", n.GenerationTime),
		fmt.Sprintf("Expires: %v", n.ExpiryTime),
	}
	return strings.Join(parts, "\n")
}

// Token is a mechanism to get an ntoken as a string.
// It guarantees that the returned token has not expired.
type Token interface {
	// Value returns the value of the current token or
	// an error if it couldn't be generated for any reason.
	Value() (string, error)
}

// TokenBuilder provides a mechanism to set optional ntoken attributes and
// a means to get the token value with efficient auto-refresh.
type TokenBuilder interface {
	// SetExpiration sets the duration for which the token is valid (default=1h).
	SetExpiration(t time.Duration)
	// SetHostname sets the hostname for the token (default=current hostname).
	SetHostname(h string)
	// SetIPAddress sets the IP address for the token (default=host IP address).
	SetIPAddress(ip string)
	// SetKeyService sets the key service for the token
	SetKeyService(keyService string)
	// Token returns a Token instance with the fields correctly set for
	// the current token. Multiple calls to Token will return the same implementation.
	// If you change optional attributes between calls to Token, these will have no effect.
	Token() Token
}

// tokenBuilder implements TokenBuilder
type tokenBuilder struct {
	signer     Signer
	ntok       *NToken
	expiration time.Duration
	l          sync.Mutex
	tok        *token
}

// NewTokenBuilder returns a TokenBuilder implementation for the specified
// domain/name, with a private key (PEM format) and its key-version. The key-version
// should be the same string that was used to register the key with Athenz.
func NewTokenBuilder(domain, name string, privateKeyPEM []byte, keyVersion string) (TokenBuilder, error) {
	s, err := NewSigner(privateKeyPEM)
	if err != nil {
		return nil, fmt.Errorf("Unable to create signer: %v", err)
	}

	exp := time.Hour
	ntok := &NToken{
		Domain:         domain,
		Name:           name,
		KeyVersion:     keyVersion,
		Version:        defaultTokenVersion,
		Hostname:       defaultHostname(),
		IPAddress:      defaultIP(),
		GenerationTime: time.Now().Add(-2 * exp),
		ExpiryTime:     time.Now().Add(-1 * exp), // start out as expired
	}

	err = ntok.assertValid()
	if err != nil {
		return nil, err
	}

	return &tokenBuilder{
		signer:     s,
		ntok:       ntok,
		expiration: exp,
	}, nil
}

func (t *tokenBuilder) SetExpiration(e time.Duration) {
	t.expiration = e
}

func (t *tokenBuilder) SetHostname(h string) {
	t.ntok.Hostname = h
}

func (t *tokenBuilder) SetIPAddress(v string) {
	t.ntok.IPAddress = v
}

func (t *tokenBuilder) SetKeyService(keyService string) {
	t.ntok.KeyService = keyService
}

func (t *tokenBuilder) Token() Token {
	t.l.Lock()
	defer t.l.Unlock()
	if t.tok == nil {
		t.tok = &token{
			signer:     t.signer,
			ntok:       t.ntok,
			expiration: t.expiration,
		}
	}
	return t.tok
}

type token struct {
	sync.Mutex
	signer      Signer
	ntok        *NToken
	expiration  time.Duration
	cachedToken string
}

func (t *token) Value() (string, error) {
	t.Lock()
	defer t.Unlock()

	if t.ntok.almostExpired() {
		v, err := t.ntok.toSignedToken(t.signer, t.expiration)
		if err != nil {
			return "", err
		}
		t.cachedToken = v
	}

	return t.cachedToken, nil
}

// TokenValidator provides a mechanism to validate tokens.
type TokenValidator interface {
	// Validate returns an unexpired NToken object from its
	// string representation.
	Validate(token string) (*NToken, error)
}

type tokenValidator struct {
	verifier Verifier
}

func (tv *tokenValidator) Validate(token string) (*NToken, error) {
	ntok := &NToken{}
	err := ntok.load(token, tv.verifier)
	if err != nil {
		return nil, err
	}
	if ntok.IsExpired() {
		return nil, fmt.Errorf("Token has expired")
	}
	return ntok, nil
}

// NewPubKeyTokenValidator returns NToken objects from signed token strings
// given a public key to verify signatures.
func NewPubKeyTokenValidator(publicKeyPEM []byte) (TokenValidator, error) {
	v, err := NewVerifier(publicKeyPEM)
	if err != nil {
		return nil, fmt.Errorf("Unable to create verifier: %v", err)
	}
	return &tokenValidator{
		verifier: v,
	}, nil
}

// ValidationConfig contains data to change runtime parameters from the default values.
type ValidationConfig struct {
	ZTSBaseUrl            string        // the ZTS base url including the /zts/v1 version path, default
	PublicKeyFetchTimeout time.Duration // timeout for fetching the public key from ZTS, default: 5s
	CacheTTL              time.Duration // TTL for cached public keys, default: 10 minutes
	sysAuthDomain         string        // domain for the ZMS / ZTS service itself
	zmsService            string        // service name for the ZMS service
	ztsService            string        // service name for the ZTS service
}

// NewTokenValidator returns NToken objects from signed token strings.
// It automatically fetches the required public key for validation from ZTS
// based on the token contents. You can optionally pass in a validation config
// object to change runtime parameters from the default values.
func NewTokenValidator(config ...ValidationConfig) TokenValidator {
	cfg := &ValidationConfig{
		ZTSBaseUrl:            "https://localhost:4443/zts/v1",
		PublicKeyFetchTimeout: 5 * time.Second,
		CacheTTL:              10 * time.Minute,
		sysAuthDomain:         "sys.auth",
		zmsService:            "zms",
		ztsService:            "zts",
	}
	for _, c := range config {
		if c.ZTSBaseUrl != "" {
			cfg.ZTSBaseUrl = c.ZTSBaseUrl
		}
		if c.PublicKeyFetchTimeout != 0 {
			cfg.PublicKeyFetchTimeout = c.PublicKeyFetchTimeout
		}
		if c.CacheTTL != 0 {
			cfg.CacheTTL = c.CacheTTL
		}
	}
	return newAutoTokenValidator(cfg)
}

type nopVerifier struct {
}

func (nv *nopVerifier) Verify(input, signature string) error {
	return nil
}

type autoTokenValidator struct {
	config          *ValidationConfig
	store           *keyStore
	initialVerifier Verifier
}

func newAutoTokenValidator(cfg *ValidationConfig) *autoTokenValidator {
	return &autoTokenValidator{
		config:          cfg,
		store:           newKeyStore(cfg),
		initialVerifier: &nopVerifier{},
	}
}

func (a *autoTokenValidator) Validate(token string) (*NToken, error) {

	t := &NToken{}
	err := t.load(token, a.initialVerifier)
	if err != nil {
		return nil, err
	}

	src := keySource{keyVersion: t.KeyVersion}
	if t.KeyService != "" && t.KeyService == a.config.zmsService {
		src.domain = a.config.sysAuthDomain
		src.name = a.config.zmsService
	} else if t.KeyService != "" && t.KeyService == a.config.ztsService {
		src.domain = a.config.sysAuthDomain
		src.name = a.config.ztsService
	} else if len(t.Version) > 0 && t.Version[0] == 'U' { // user principal, use ZMS's public key
		src.domain = a.config.sysAuthDomain
		src.name = a.config.zmsService
	} else {
		src.domain = t.Domain
		src.name = t.Name
	}

	validator, err := a.store.getValidator(src)
	if err != nil {
		return nil, err
	}

	return validator.Validate(token)
}
