package snowpipestreaming

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/snowpipestreaming/internal/restapi"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/snowflake"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

// versionHP is the [importInfo.Version] of imports created by [ManagerV2].
const versionHP = 2

type (
	// hpAPI is the part of the Snowpipe Streaming HP REST API that [ManagerV2] uses. See [restapi.Client].
	hpAPI interface {
		OpenChannel(ctx context.Context, ref restapi.ChannelRef) (*restapi.OpenChannelResponse, error)
		AppendRows(ctx context.Context, ref restapi.ChannelRef, req restapi.AppendRowsRequest) (*restapi.AppendRowsResponse, error)
		BulkChannelStatus(ctx context.Context, pipe restapi.PipeRef, channelNames []string) (map[string]restapi.ChannelStatus, error)
	}

	// hpChannel is an open channel, one per table. There is at most one append in flight per channel.
	hpChannel struct {
		ref restapi.ChannelRef
		// continuationToken is the latest token Snowflake returned for the channel. Appending with an older one
		// makes Snowflake drop the rows silently, so it is replaced after every successful call.
		continuationToken string
		// appendedOffset is the end offset of the last chunk Snowflake accepted since the channel was opened.
		// It is empty after an open, since opening a channel discards its uncommitted rows.
		appendedOffset string
		// committedOffset is the committed offset Snowflake last reported for the channel.
		committedOffset string
		// rowsErrorBaseline is the channel's cumulative rows_error_count when it was opened or last polled.
		rowsErrorBaseline int64
		// isBroken is set when an append failed: the channel is reopened before its next append.
		isBroken bool
	}

	// hpCredentials identifies the Snowflake user an API client was built for.
	hpCredentials struct {
		account, user, role, privateKey, passphrase string
	}

	// ManagerV2 uploads to Snowpipe Streaming through Snowflake's high-performance (HP) REST API, without snowpipe-clients.
	//
	// Compared to the classic [Manager], it owns the table schema (HP doesn't return it), validates and creates columns
	// before sending (HP silently drops unknown fields), sends JSON values natively, and treats an empty committed offset
	// as "nothing committed yet". Offsets are job IDs: rows are sent in job ID order, split in chunks of at most 4 MiB
	// compressed, each chunk's end offset being its last job ID.
	ManagerV2 struct {
		base         *Manager // shared helpers: reading the batch file, discards, abort and fail outputs
		logger       logger.Logger
		statsFactory stats.Stats
		destination  *backendconfig.DestinationT
		now          func() time.Time
		channelName  string // <destinationID>-<instanceID>: each pod writes only its own channels

		// newAPI builds the REST client for the destination's credentials.
		newAPI func(creds hpCredentials) (hpAPI, error)
		// newSession opens a session for running SQL in the given namespace.
		newSession func(destConf *destConfig) warehouseSession

		// state holds the destination's open channels. It outlives the manager, which batchrouter rebuilds whenever
		// the destination's config changes, so that imports in flight keep their channel's state across a rebuild.
		// Its mutex serialises Upload and Poll, including across an old and a new manager of the same destination.
		state            *hpState
		api              hpAPI
		apiCreds         hpCredentials
		schemas          hpSchemas
		polled           map[string]*importInfo // imports that reached a terminal state, by table
		pollingStartTime time.Time

		config struct {
			schemaCacheTTL    config.ValueLoader[time.Duration] // cached table schemas are read again after this long
			maxChunkRawBytes  config.ValueLoader[int]           // uncompressed bytes compressed per chunk attempt
			pollConcurrency   config.ValueLoader[int]           // concurrent bulk channel status calls per poll
			maxRetries        int                               // retries of a retryable REST call
			retryWaitMin      time.Duration                     // minimum backoff between retries
			retryWaitMax      time.Duration                     // maximum backoff between retries
			timeout           time.Duration                     // timeout of a single REST call
			maxConnsPerHost   int                               // HTTP connections per Snowflake host
			stuckPipelineTime config.ValueLoader[time.Duration] // polling longer than this fails the in-progress imports
		}

		stats hpStats
	}
)

// hpOpt configures a [ManagerV2].
type hpOpt func(*ManagerV2)

// withAPIFactory sets how the REST client is built for the destination's credentials.
func withAPIFactory(newAPI func(creds hpCredentials) (hpAPI, error)) hpOpt {
	return func(m *ManagerV2) {
		m.newAPI = newAPI
	}
}

// withWarehouseSession sets how a session for running SQL is opened.
func withWarehouseSession(newSession func(destConf *destConfig) warehouseSession) hpOpt {
	return func(m *ManagerV2) {
		m.newSession = newSession
	}
}

// withState sets the destination's channel state, which defaults to this process's state for the destination.
func withState(state *hpState) hpOpt {
	return func(m *ManagerV2) {
		m.state = state
	}
}

// withClock sets the clock.
func withClock(now func() time.Time) hpOpt {
	return func(m *ManagerV2) {
		m.now = now
	}
}

// withMaxChunkRawBytes sets the uncompressed size compressed per chunk attempt, normally set in MB by config.
//
//nolint:unparam // tests only need one row per chunk so far
func withMaxChunkRawBytes(n int) hpOpt {
	return func(m *ManagerV2) {
		m.config.maxChunkRawBytes = config.SingleValueLoader(n)
	}
}

// NewV2 creates a [ManagerV2] for the destination. base provides the helpers shared with the classic path.
func NewV2(conf *config.Config, statsFactory stats.Stats, destination *backendconfig.DestinationT, base *Manager, opts ...hpOpt) *ManagerV2 {
	m := &ManagerV2{
		base:         base,
		logger:       base.logger.Withn(logger.NewIntField("version", versionHP)),
		statsFactory: statsFactory,
		destination:  destination,
		now:          timeutil.Now,
		channelName:  destination.ID + "-" + base.config.instanceID,
		state:        processHPStates.get(destination.ID),
		schemas:      hpSchemas{cache: make(map[string]cachedSchema)},
		polled:       make(map[string]*importInfo),
	}
	m.config.schemaCacheTTL = conf.GetReloadableDurationVar(30, time.Minute, "SnowpipeStreaming.HP.schemaCacheTTL")
	m.config.maxChunkRawBytes = conf.GetReloadableIntVar(16, int(bytesize.MB), "SnowpipeStreaming.HP.maxChunkRawBytesInMB")
	m.config.pollConcurrency = conf.GetReloadableIntVar(16, 1, "SnowpipeStreaming.HP.pollConcurrency")
	m.config.maxRetries = conf.GetIntVar(5, 1, "SnowpipeStreaming.HP.retryMax")
	m.config.retryWaitMin = conf.GetDurationVar(100, time.Millisecond, "SnowpipeStreaming.HP.retryWaitMin")
	m.config.retryWaitMax = conf.GetDurationVar(10, time.Second, "SnowpipeStreaming.HP.retryWaitMax")
	m.config.timeout = conf.GetDurationVar(60, time.Second, "SnowpipeStreaming.HP.timeout")
	m.config.maxConnsPerHost = conf.GetIntVar(10, 1, "SnowpipeStreaming.HP.maxHTTPConnections")
	m.config.stuckPipelineTime = base.config.stuckPipelineThreshold
	m.stats = newHPStats(statsFactory, destination)
	m.newAPI = m.newRESTClient
	m.newSession = m.newSQLSession
	for _, opt := range opts {
		opt(m)
	}
	return m
}

func (m *ManagerV2) newSQLSession(destConf *destConfig) warehouseSession {
	return &sqlWarehouseSession{
		database:  destConf.Database,
		namespace: destConf.Namespace,
		connect: func(ctx context.Context) (*snowflake.Snowflake, error) {
			return m.connectSnowflake(ctx, destConf.Namespace)
		},
	}
}

func (m *ManagerV2) newRESTClient(creds hpCredentials) (hpAPI, error) {
	key, err := restapi.ParsePrivateKey(whutils.FormatPemContent(creds.privateKey), creds.passphrase)
	if err != nil {
		return nil, err
	}
	return restapi.New(
		restapi.Config{Account: creds.account, User: creds.user, Role: creds.role, PrivateKey: key},
		restapi.WithHTTPClient(&http.Client{
			Transport: &http.Transport{
				Proxy:               http.ProxyFromEnvironment,
				MaxConnsPerHost:     m.config.maxConnsPerHost,
				MaxIdleConnsPerHost: m.config.maxConnsPerHost,
				IdleConnTimeout:     90 * time.Second,
			},
			Timeout: m.config.timeout,
		}),
		restapi.WithRetries(m.config.maxRetries, m.config.retryWaitMin, m.config.retryWaitMax),
	)
}

// ensureAPI makes m.api the REST client for the destination's credentials, building a new one when they change.
func (m *ManagerV2) ensureAPI(destConf *destConfig) error {
	creds := hpCredentials{
		account:    destConf.Account,
		user:       destConf.User,
		role:       destConf.Role,
		privateKey: destConf.PrivateKey,
		passphrase: destConf.PrivateKeyPassphrase,
	}
	if m.api != nil && m.apiCreds == creds {
		return nil
	}
	api, err := m.newAPI(creds)
	if err != nil {
		return fmt.Errorf("creating snowflake client: %w", err)
	}
	m.api, m.apiCreds = api, creds
	// Channels belong to the account: a new key, user or role keeps writing to the same channels, but another
	// account has none of them.
	if m.state.account != creds.account {
		clear(m.state.channels)
		m.state.account = creds.account
	}
	return nil
}

func (m *ManagerV2) Transform(job *jobsdb.JobT) (string, error) {
	return m.base.Transform(job)
}

// connectSnowflake connects to Snowflake in the given namespace. It builds the Snowflake integration directly rather
// than through the warehouse manager factory, whose stats decorator hides the connection that SHOW COLUMNS needs.
func (m *ManagerV2) connectSnowflake(ctx context.Context, namespace string) (*snowflake.Snowflake, error) {
	sf := snowflake.New(m.base.appConfig, m.base.logger, m.statsFactory)
	sf.SetConnectionTimeout(whutils.GetConnectionTimeout(m.destination.DestinationDefinition.Name, m.destination.ID))
	modelWarehouse := whutils.ModelWarehouse{
		WorkspaceID: m.destination.WorkspaceID,
		Destination: *m.destination,
		Namespace:   namespace,
		Type:        m.destination.DestinationDefinition.Name,
		Identifier:  m.destination.WorkspaceID + ":" + m.destination.ID,
	}
	if err := sf.Setup(ctx, modelWarehouse, whutils.NewNoOpUploader()); err != nil {
		return nil, fmt.Errorf("setting up snowflake: %w", err)
	}
	return sf, nil
}
