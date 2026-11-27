package restapi

import (
	"context"
	"crypto"
	cryptorand "crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
	"github.com/youmark/pkcs8"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/testhelper/rand"
)

// fakeSnowflake is an in-memory Snowflake account and ingest host, faithful to the behaviour verified against
// a real account: a hostname endpoint, a token exchange, and channels with continuation tokens.
type fakeSnowflake struct {
	t      *testing.T
	server *httptest.Server
	key    *rsa.PublicKey

	mu             sync.Mutex
	tokensIssued   int
	tokenExpiresIn time.Duration
	failNext       []int // status codes to answer before handling the next ingest call
	rejectToken    bool  // answer the next ingest call with 401
	requestIDs     []string
	appendBodies   [][]byte
	channels       map[string]*fakeChannel
	lastScope      string
}

type fakeChannel struct {
	seq       int
	rowSeq    int
	committed *string
}

func newFakeSnowflake(t *testing.T, key *rsa.PublicKey) *fakeSnowflake {
	f := &fakeSnowflake{t: t, key: key, channels: map[string]*fakeChannel{}, tokenExpiresIn: time.Hour}
	f.server = httptest.NewServer(http.HandlerFunc(f.handle))
	t.Cleanup(f.server.Close)
	return f
}

func (f *fakeSnowflake) host() string { return strings.TrimPrefix(f.server.URL, "http://") }

func (f *fakeSnowflake) handle(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()

	switch r.URL.Path {
	case "/v2/streaming/hostname":
		require.Equal(f.t, "KEYPAIR_JWT", r.Header.Get(headerSnowflakeAuthorizationToken))
		f.verifyKeyPairJWT(r)
		_, _ = io.WriteString(w, f.host())
		return
	case "/oauth/token":
		f.verifyKeyPairJWT(r)
		require.NoError(f.t, r.ParseForm())
		require.Equal(f.t, "urn:ietf:params:oauth:grant-type:jwt-bearer", r.Form.Get("grant_type"))
		f.lastScope = r.Form.Get("scope")
		f.tokensIssued++
		scoped, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
			"n":   f.tokensIssued,
			"exp": time.Now().Add(f.tokenExpiresIn).Unix(),
		}).SignedString([]byte("secret"))
		require.NoError(f.t, err)
		w.Header().Set("Content-Type", contentTypeJSON)
		_, _ = io.WriteString(w, scoped)
		return
	}

	// ingest calls
	require.Equal(f.t, contentTypeJSON, r.Header.Get("Accept"))
	require.Equal(f.t, "OAUTH", r.Header.Get(headerSnowflakeAuthorizationToken))
	requestID := r.URL.Query().Get("requestId")
	_, err := uuid.Parse(requestID)
	require.NoError(f.t, err, "requestId must be a UUID")
	f.requestIDs = append(f.requestIDs, requestID)

	if f.rejectToken {
		f.rejectToken = false
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	if len(f.failNext) > 0 {
		code := f.failNext[0]
		f.failNext = f.failNext[1:]
		w.WriteHeader(code)
		_, _ = io.WriteString(w, `{"code":"ERR_INTERNAL","message":"try again"}`)
		return
	}

	body, err := io.ReadAll(r.Body)
	require.NoError(f.t, err)
	path := r.URL.Path
	switch {
	case r.Method == http.MethodPut && strings.Contains(path, "/channels/"):
		name := path[strings.LastIndex(path, "/")+1:]
		if strings.Contains(path, "/pipes/MISSING-STREAMING/") {
			w.WriteHeader(http.StatusNotFound)
			_, _ = io.WriteString(w, `{"code":"ERR_TABLE_DOES_NOT_EXIST_NOT_AUTHORIZED","error_code":"ERR_TABLE_DOES_NOT_EXIST_NOT_AUTHORIZED","message":"Failed to retrieve table"}`)
			return
		}
		ch, ok := f.channels[name]
		if !ok {
			ch = &fakeChannel{}
			f.channels[name] = ch
		} else {
			ch.seq++
		}
		ch.rowSeq = 1
		f.writeJSON(w, map[string]any{
			"next_continuation_token": f.token(ch),
			"channel_status":          f.status(name, ch),
		})
	case r.Method == http.MethodPost && strings.HasSuffix(path, "/rows"):
		require.Equal(f.t, contentTypeNDJSON, r.Header.Get("Content-Type"))
		require.Equal(f.t, "zstd", r.Header.Get("Content-Encoding"))
		parts := strings.Split(path, "/")
		ch := f.channels[parts[len(parts)-2]]
		require.NotNil(f.t, ch)
		require.Equal(f.t, f.token(ch), r.URL.Query().Get("continuationToken"))
		f.appendBodies = append(f.appendBodies, body)
		ch.rowSeq++
		end := r.URL.Query().Get("endOffsetToken")
		ch.committed = &end
		f.writeJSON(w, map[string]any{"next_continuation_token": f.token(ch), "status_code": 0})
	case r.Method == http.MethodPost && strings.HasSuffix(path, ":bulk-channel-status"):
		var req bulkChannelStatusRequest
		require.NoError(f.t, jsonrs.Unmarshal(body, &req))
		statuses := map[string]any{}
		for _, name := range req.ChannelNames {
			if ch, ok := f.channels[name]; ok {
				statuses[name] = f.status(name, ch)
			}
		}
		f.writeJSON(w, map[string]any{"channel_statuses": statuses})
	default:
		f.t.Fatalf("unexpected request %s %s", r.Method, path)
	}
}

func (f *fakeSnowflake) token(ch *fakeChannel) string {
	return strconv.Itoa(ch.seq) + "_" + strconv.Itoa(ch.rowSeq)
}

func (f *fakeSnowflake) status(name string, ch *fakeChannel) map[string]any {
	return map[string]any{
		"channel_name":                name,
		"channel_status_code":         ChannelStatusSuccess,
		"last_committed_offset_token": ch.committed,
		"rows_error_count":            0,
	}
}

func (f *fakeSnowflake) writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", contentTypeJSON)
	require.NoError(f.t, jsonrs.NewEncoder(w).Encode(v))
}

func (f *fakeSnowflake) verifyKeyPairJWT(r *http.Request) {
	signed := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	claims := jwt.MapClaims{}
	_, err := jwt.ParseWithClaims(signed, claims, func(*jwt.Token) (any, error) { return f.key, nil })
	require.NoError(f.t, err)
	require.Equal(f.t, "XY12345.LOADER", claims["sub"])
	require.True(f.t, strings.HasPrefix(claims["iss"].(string), "XY12345.LOADER.SHA256:"))
}

func newTestKey(t *testing.T) *rsa.PrivateKey {
	t.Helper()
	key, err := rsa.GenerateKey(cryptorand.Reader, 2048)
	require.NoError(t, err)
	return key
}

func newTestClient(t *testing.T, f *fakeSnowflake, key *rsa.PrivateKey) *Client {
	t.Helper()
	c, err := New(
		Config{Account: "xy12345.us-east-1", User: "loader", Role: "LOADER_ROLE", PrivateKey: key},
		WithAccountURL(f.server.URL),
		WithIngestScheme("http"),
		WithRetries(3, time.Millisecond, 5*time.Millisecond),
	)
	require.NoError(t, err)
	return c
}

var testChannel = ChannelRef{PipeRef: DefaultPipe("DB", "SCHEMA", "TRACKS"), Channel: "dest-1-rudder-0"}

func TestClient(t *testing.T) {
	ctx := context.Background()

	t.Run("open, append and status", func(t *testing.T) {
		key := newTestKey(t)
		f := newFakeSnowflake(t, &key.PublicKey)
		c := newTestClient(t, f, key)

		open, err := c.OpenChannel(ctx, testChannel)
		require.NoError(t, err)
		require.Equal(t, "0_1", open.NextContinuationToken)
		require.True(t, open.ChannelStatus.Healthy())
		require.Empty(t, open.ChannelStatus.CommittedOffset())
		require.Equal(t, f.host()+" session:role:LOADER_ROLE", f.lastScope, "the role is part of the token scope")

		chunks, oversized := NewChunker(1024).Chunk([][]byte{[]byte(`{"ID":"1"}`), []byte(`{"ID":"2"}`)})
		require.Empty(t, oversized)
		require.Len(t, chunks, 1)
		res, err := c.AppendRows(ctx, testChannel, AppendRowsRequest{
			ContinuationToken: open.NextContinuationToken,
			StartOffsetToken:  "1",
			EndOffsetToken:    "2",
			Body:              chunks[0].Body,
		})
		require.NoError(t, err)
		require.Equal(t, "0_2", res.NextContinuationToken)
		require.Equal(t, "{\"ID\":\"1\"}\n{\"ID\":\"2\"}\n", string(decompress(t, f.appendBodies[0])))

		statuses, err := c.BulkChannelStatus(ctx, testChannel.PipeRef, []string{testChannel.Channel, "unknown"})
		require.NoError(t, err)
		require.Len(t, statuses, 1, "unknown channels are absent")
		st := statuses[testChannel.Channel]
		require.Equal(t, "2", st.CommittedOffset())
		require.Equal(t, 1, f.tokensIssued, "the scoped token is cached")
	})

	t.Run("missing table is a typed error and not retried", func(t *testing.T) {
		key := newTestKey(t)
		f := newFakeSnowflake(t, &key.PublicKey)
		c := newTestClient(t, f, key)

		_, err := c.OpenChannel(ctx, ChannelRef{PipeRef: DefaultPipe("DB", "SCHEMA", "MISSING"), Channel: "c"})
		require.Error(t, err)
		require.True(t, HasCode(err, ErrCodeTableDoesNotExist))
		require.Len(t, f.requestIDs, 1)
	})

	t.Run("retryable errors are retried with the same request id", func(t *testing.T) {
		key := newTestKey(t)
		f := newFakeSnowflake(t, &key.PublicKey)
		c := newTestClient(t, f, key)
		f.failNext = []int{http.StatusServiceUnavailable, http.StatusTooManyRequests}

		_, err := c.OpenChannel(ctx, testChannel)
		require.NoError(t, err)
		require.Len(t, f.requestIDs, 3)
		require.Equal(t, f.requestIDs[0], f.requestIDs[2])
	})

	t.Run("retries are bounded", func(t *testing.T) {
		key := newTestKey(t)
		f := newFakeSnowflake(t, &key.PublicKey)
		c := newTestClient(t, f, key)
		f.failNext = []int{500, 500, 500, 500, 500}

		_, err := c.OpenChannel(ctx, testChannel)
		apiErr, ok := errors.AsType[*APIError](err)
		require.True(t, ok, err)
		require.Equal(t, http.StatusInternalServerError, apiErr.StatusCode)
		require.Len(t, f.requestIDs, 4, "one attempt and three retries")
	})

	t.Run("a 401 refreshes the token once", func(t *testing.T) {
		key := newTestKey(t)
		f := newFakeSnowflake(t, &key.PublicKey)
		c := newTestClient(t, f, key)
		_, err := c.OpenChannel(ctx, testChannel)
		require.NoError(t, err)

		f.rejectToken = true
		_, err = c.BulkChannelStatus(ctx, testChannel.PipeRef, []string{testChannel.Channel})
		require.NoError(t, err)
		require.Equal(t, 2, f.tokensIssued)
	})

	t.Run("a token about to expire is refreshed", func(t *testing.T) {
		key := newTestKey(t)
		f := newFakeSnowflake(t, &key.PublicKey)
		f.tokenExpiresIn = 2 * time.Minute // within the refresh margin
		c := newTestClient(t, f, key)
		_, err := c.OpenChannel(ctx, testChannel)
		require.NoError(t, err)
		_, err = c.OpenChannel(ctx, testChannel)
		require.NoError(t, err)
		require.Equal(t, 2, f.tokensIssued)
	})

	t.Run("bodies above 4 MiB are refused before sending", func(t *testing.T) {
		key := newTestKey(t)
		f := newFakeSnowflake(t, &key.PublicKey)
		c := newTestClient(t, f, key)
		_, err := c.AppendRows(ctx, testChannel, AppendRowsRequest{ContinuationToken: "0_1", Body: make([]byte, maxRequestBodyBytes+1)})
		require.True(t, HasCode(err, ErrCodeRequestBodyTooLarge))
		require.Empty(t, f.requestIDs)
	})

	t.Run("names are escaped in paths", func(t *testing.T) {
		ref := ChannelRef{PipeRef: DefaultPipe("DB", "MY SCHEMA", "T/1"), Channel: "a b"}
		require.Equal(t, "T%2F1-STREAMING", url.PathEscape(ref.Pipe))
	})
}

func TestParsePrivateKey(t *testing.T) {
	key := newTestKey(t)
	pkcs8DER, err := x509.MarshalPKCS8PrivateKey(key)
	require.NoError(t, err)
	encode := func(blockType string, der []byte) string {
		return string(pem.EncodeToMemory(&pem.Block{Type: blockType, Bytes: der}))
	}
	encrypt := func(cipher pkcs8.Cipher, hash crypto.Hash) string {
		der, err := pkcs8.MarshalPrivateKey(key, []byte("secret"), &pkcs8.Opts{
			Cipher:  cipher,
			KDFOpts: pkcs8.PBKDF2Opts{SaltSize: 8, IterationCount: 2048, HMACHash: hash},
		})
		require.NoError(t, err)
		return encode("ENCRYPTED PRIVATE KEY", der)
	}

	for name, pemContent := range map[string]string{
		"pkcs8":                     encode("PRIVATE KEY", pkcs8DER),
		"triple-des (HP SDK fails)": encrypt(pkcs8.TripleDESCBC, crypto.SHA256),
		"aes-256 sha-256":           encrypt(pkcs8.AES256CBC, crypto.SHA256),
		"aes-256 sha-1":             encrypt(pkcs8.AES256CBC, crypto.SHA1),
	} {
		t.Run(name, func(t *testing.T) {
			passphrase := ""
			if strings.Contains(pemContent, "ENCRYPTED") {
				passphrase = "secret"
			}
			parsed, err := ParsePrivateKey(pemContent, passphrase)
			require.NoError(t, err)
			require.True(t, key.Equal(parsed))
		})
	}

	t.Run("wrong passphrase", func(t *testing.T) {
		_, err := ParsePrivateKey(encrypt(pkcs8.AES256CBC, crypto.SHA256), "wrong")
		require.Error(t, err)
	})
	t.Run("missing passphrase", func(t *testing.T) {
		_, err := ParsePrivateKey(encrypt(pkcs8.AES256CBC, crypto.SHA256), "")
		require.Error(t, err)
	})
	t.Run("PKCS#1, which the SQL connection rejects too", func(t *testing.T) {
		_, err := ParsePrivateKey(encode("RSA PRIVATE KEY", x509.MarshalPKCS1PrivateKey(key)), "")
		require.Error(t, err)
	})
	t.Run("not a PEM", func(t *testing.T) {
		_, err := ParsePrivateKey("garbage", "")
		require.Error(t, err)
	})
}

func TestChunker(t *testing.T) {
	t.Run("keeps order and splits at row boundaries", func(t *testing.T) {
		rows := make([][]byte, 100)
		for i := range rows {
			rows[i] = []byte(`{"ID":"` + strconv.Itoa(i) + `"}`)
		}
		chunks, oversized := NewChunker(100).Chunk(rows)
		require.Empty(t, oversized)
		require.Greater(t, len(chunks), 1)
		next := 0
		for _, c := range chunks {
			require.Equal(t, next, c.First)
			require.GreaterOrEqual(t, c.Last, c.First)
			lines := strings.Split(strings.TrimSuffix(string(decompress(t, c.Body)), "\n"), "\n")
			require.Len(t, lines, c.Last-c.First+1)
			require.Equal(t, string(rows[c.First]), lines[0])
			next = c.Last + 1
		}
		require.Equal(t, len(rows), next)
	})

	t.Run("halves chunks that compress above the limit and skips rows too large on their own", func(t *testing.T) {
		c := &Chunker{maxRawBytes: 1 << 20, maxBodyBytes: 200}
		big := []byte(`{"S":"` + rand.String(500) + `"}`)
		rows := [][]byte{[]byte(`{"ID":"1"}`), []byte(`{"S":"` + rand.String(120) + `"}`), big, []byte(`{"S":"` + rand.String(120) + `"}`), []byte(`{"ID":"5"}`)}
		chunks, oversized := c.Chunk(rows)
		require.Equal(t, []int{2}, oversized)
		var covered []int
		for _, ch := range chunks {
			require.LessOrEqual(t, len(ch.Body), 200)
			for i := ch.First; i <= ch.Last; i++ {
				covered = append(covered, i)
			}
		}
		require.Equal(t, []int{0, 1, 3, 4}, covered)
	})
}

func decompress(t *testing.T, body []byte) []byte {
	t.Helper()
	dec, err := zstd.NewReader(nil)
	require.NoError(t, err)
	defer dec.Close()
	out, err := dec.DecodeAll(body, nil)
	require.NoError(t, err)
	return out
}
