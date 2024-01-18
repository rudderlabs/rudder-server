package v2

import (
	"encoding/json"
	"net/http"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/rudderlabs/rudder-go-kit/cachettl"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/sync"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	oauth_exts "github.com/rudderlabs/rudder-server/services/oauth/v2/extensions"
)

type DestinationJobT struct {
	Message           json.RawMessage            `json:"batchedRequest"`
	JobMetadataArray  []JobMetadataT             `json:"metadata"` // multiple jobs may be batched in a single message
	Destination       backendconfig.DestinationT `json:"destination"`
	Batched           bool                       `json:"batched"`
	StatusCode        int                        `json:"statusCode"`
	Error             string                     `json:"error"`
	AuthErrorCategory string                     `json:"authErrorCategory"`
}

// type DestinationJobs []DestinationJobT

var jsonfast = jsoniter.ConfigCompatibleWithStandardLibrary

type JobMetadataT struct {
	UserID             string          `json:"userId"`
	JobID              int64           `json:"jobId"`
	SourceID           string          `json:"sourceId"`
	DestinationID      string          `json:"destinationId"`
	AttemptNum         int             `json:"attemptNum"`
	ReceivedAt         string          `json:"receivedAt"`
	CreatedAt          string          `json:"createdAt"`
	FirstAttemptedAt   string          `json:"firstAttemptedAt"`
	TransformAt        string          `json:"transformAt"`
	WorkspaceID        string          `json:"workspaceId"`
	Secret             json.RawMessage `json:"secret"`
	JobT               *jobsdb.JobT    `json:"jobsT,omitempty"`
	WorkerAssignedTime time.Time       `json:"workerAssignedTime"`
	DestInfo           json.RawMessage `json:"destInfo,omitempty"`
}

// Oauth2Transport is an http.RoundTripper that adds the appropriate authorization information to oauth requests.
type Oauth2Transport struct {
	oauthHandler OAuthHandler
	oauth_exts.Augmenter
	Handler
	Transport  http.RoundTripper
	baseURL    string
	keyLocker  *sync.PartitionLocker
	tokenCache *cachettl.Cache[CacheKey, *AccessToken]
	log        logger.Logger
	flow       RudderFlow
}

// OAuthHttpClient returns an http client that will add the appropriate authorization information to oauth requests.
func OAuthHttpClient(client *http.Client, baseURL string, augmenter oauth_exts.Augmenter, handler Handler, flowType RudderFlow) *http.Client {
	client.Transport = &Oauth2Transport{
		oauthHandler: *NewOAuthHandler(backendconfig.DefaultBackendConfig),
		Augmenter:    augmenter,
		Handler:      handler,
		Transport:    client.Transport,
		baseURL:      baseURL,
		tokenCache:   cachettl.New[CacheKey, *AccessToken](),
		keyLocker:    sync.NewPartitionLocker(),
		log:          logger.NewLogger().Child("OAuthHttpClient"),
		flow:         flowType,
	}
	return client
}

func (t *Oauth2Transport) RoundTrip(r *http.Request) (*http.Response, error) {
	return t.Handler.Handler(r, t.Transport, t.oauthHandler, t.Augmenter)
}
