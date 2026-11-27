package restapi

import (
	"context"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/youmark/pkcs8"

	"github.com/rudderlabs/rudder-server/utils/httputil"
)

const (
	// jwtLifetime is how long a key-pair JWT is valid for. Snowflake accepts at most one hour.
	jwtLifetime = 59 * time.Minute
	// defaultTokenLifetime is used when the scoped token's expiry can't be read from it.
	// Scoped tokens are one-hour JWTs (verified against the test account).
	defaultTokenLifetime = time.Hour
	// tokenRefreshMargin is how long before expiry a scoped token is refreshed.
	tokenRefreshMargin = 5 * time.Minute
)

// ParsePrivateKey parses a PEM encoded PKCS#8 RSA private key, encrypted (PBES2 with AES or Triple-DES, which the
// Snowflake HP SDK can't decrypt) or not. It accepts the same keys as the SQL connection (sqlconnect-go), with the same library.
func ParsePrivateKey(pemContent, passphrase string) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode([]byte(pemContent))
	if block == nil {
		return nil, errors.New("decoding private key: no PEM block found")
	}
	var opts [][]byte
	if passphrase != "" {
		opts = append(opts, []byte(passphrase))
	}
	key, err := pkcs8.ParsePKCS8PrivateKeyRSA(block.Bytes, opts...)
	if err != nil {
		return nil, fmt.Errorf("parsing private key: %w", err)
	}
	return key, nil
}

// accountIdentifier returns the account part used in key-pair JWT claims:
// the account locator without region or cloud, upper-cased.
func accountIdentifier(account string) string {
	before, _, _ := strings.Cut(account, ".")
	return strings.ToUpper(before)
}

// publicKeyFingerprint returns the SHA256 fingerprint of the public key, as Snowflake expects it in the JWT issuer.
func publicKeyFingerprint(key *rsa.PrivateKey) (string, error) {
	der, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		return "", fmt.Errorf("marshalling public key: %w", err)
	}
	sum := sha256.Sum256(der)
	return "SHA256:" + base64.StdEncoding.EncodeToString(sum[:]), nil
}

// tokenSource issues and caches the scoped OAuth token used for every ingest call.
// It is safe for concurrent use.
type tokenSource struct {
	account     string // account identifier for the account URL, e.g. xy12345.us-east-1
	user        string
	role        string
	key         *rsa.PrivateKey
	fingerprint string
	accountURL  string
	doer        requestDoer
	now         func() time.Time

	mu        sync.Mutex
	token     string
	expiresAt time.Time
}

// keyPairJWT returns a freshly signed key-pair JWT.
func (ts *tokenSource) keyPairJWT() (string, error) {
	qualifiedUser := accountIdentifier(ts.account) + "." + strings.ToUpper(ts.user)
	now := ts.now()
	claims := jwt.MapClaims{
		"iss": qualifiedUser + "." + ts.fingerprint,
		"sub": qualifiedUser,
		"iat": now.Unix(),
		"exp": now.Add(jwtLifetime).Unix(),
	}
	signed, err := jwt.NewWithClaims(jwt.SigningMethodRS256, claims).SignedString(ts.key)
	if err != nil {
		return "", fmt.Errorf("signing jwt: %w", err)
	}
	return signed, nil
}

// Token returns a scoped OAuth token for the given ingest host, exchanging a new one if the cached one is about to expire.
func (ts *tokenSource) Token(ctx context.Context, ingestHost string) (string, error) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	if ts.token != "" && ts.now().Before(ts.expiresAt.Add(-tokenRefreshMargin)) {
		return ts.token, nil
	}
	token, expiresAt, err := ts.exchange(ctx, ingestHost)
	if err != nil {
		return "", err
	}
	ts.token, ts.expiresAt = token, expiresAt
	return token, nil
}

// Invalidate drops the cached token, so that the next call to [tokenSource.Token] exchanges a new one.
func (ts *tokenSource) Invalidate() {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	ts.token = ""
}

// exchange trades a key-pair JWT for a token scoped to the ingest host and the configured role.
func (ts *tokenSource) exchange(ctx context.Context, ingestHost string) (string, time.Time, error) {
	signed, err := ts.keyPairJWT()
	if err != nil {
		return "", time.Time{}, err
	}
	scope := ingestHost
	if ts.role != "" {
		scope += " session:role:" + ts.role
	}
	form := url.Values{
		"grant_type": {"urn:ietf:params:oauth:grant-type:jwt-bearer"},
		"scope":      {scope},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, ts.accountURL+"/oauth/token", strings.NewReader(form.Encode()))
	if err != nil {
		return "", time.Time{}, fmt.Errorf("creating token request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+signed)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")

	resp, err := ts.doer.Do(req)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("sending token request: %w", err)
	}
	defer func() { httputil.CloseResponse(resp) }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("reading token response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return "", time.Time{}, newAPIError(resp.StatusCode, body)
	}
	// The token comes back as the raw JWT, even though the content type says JSON.
	token := strings.Trim(strings.TrimSpace(string(body)), `"`)
	if token == "" {
		return "", time.Time{}, errors.New("exchanging token: empty token in response")
	}
	return token, ts.expiry(token), nil
}

// expiry reads the exp claim of the scoped token without verifying it, falling back to the documented lifetime.
func (ts *tokenSource) expiry(token string) time.Time {
	var claims jwt.RegisteredClaims
	if _, _, err := jwt.NewParser().ParseUnverified(token, &claims); err == nil && claims.ExpiresAt != nil {
		return claims.ExpiresAt.Time
	}
	return ts.now().Add(defaultTokenLifetime)
}
