package bqstreamv2

import (
	"context"
	"sync"
	"time"

	managedwriter "cloud.google.com/go/bigquery/storage/managedwriter"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type (
	// Manager streams events into BigQuery through the Storage Write API,
	// caching table schemas (TTL) and stream writers across uploads.
	Manager struct {
		appConfig                 *config.Config
		logger                    logger.Logger
		statsFactory              stats.Stats
		destination               *backendconfig.DestinationT
		integrationManagerCreator IntegrationManagerCreator

		streamWriterFactory StreamWriterFactory
		streamWritersMu     sync.Mutex // guards streamWriters
		// streamWriters caches open Storage Write API streams (and their
		// BigQuery clients) which, by design, outlive a single upload
		// (writerForTable dials with context.WithoutCancel). They are only
		// released via invalidateTableCacheAndStreamWriter (schema change /
		// append error).
		//
		// NOTE: Manager has no Close/Cleanup, so the writers cached here are never torn down when the
		// Manager itself is discarded — handle_lifecycle.go's refreshDestination
		// builds a new Manager on every destination RevisionID change (common in
		// multi-tenant) and Shutdown() simply drops it, in both cases leaking the
		// open streams/connections. Fixing it needs a Close() that drains and closes every cached writer, wired into the refresh/teardown path; the interface gap in common.AsyncDestinationManager is broader than this PR.
		streamWriters map[string]*tableStreamWriter

		now func() time.Time

		config struct {
			maxBufferCapacity config.ValueLoader[int64]
			tableWorkers      config.ValueLoader[int]
			maxChunkBytes     config.ValueLoader[int64]
			schemaCacheTTL    config.ValueLoader[time.Duration]
		}

		stats struct {
			jobs struct {
				succeeded stats.Counter
				failed    stats.Counter
				aborted   stats.Counter
			}
			discards               stats.Counter
			duplicateEventsInBatch stats.Counter
		}

		schemaCache TableSchemaCache
	}

	destConfig struct {
		ProjectID   string `mapstructure:"project"`
		Namespace   string `mapstructure:"namespace"`
		Credentials string `mapstructure:"credentials"`
	}

	IntegrationManager interface {
		FetchSchema(ctx context.Context) (whutils.ModelSchema, error)
		CreateSchema(ctx context.Context) error
		CreateTable(ctx context.Context, tableName string, columnMap whutils.ModelTableSchema) error
		AddColumns(ctx context.Context, tableName string, columnsInfo []whutils.ColumnInfo) error
		Cleanup(ctx context.Context)
	}

	IntegrationManagerCreator func(ctx context.Context, cfg destConfig) (IntegrationManager, error)
	StreamWriterFactory       interface {
		NewStreamWriter(ctx context.Context, destConf destConfig, tableName string, tableSchema whutils.ModelTableSchema) (StreamWriter, error)
	}

	streamWriterFactoryImpl struct {
		maxInflightRequests int
		maxInflightBytes    int64
	}

	StreamWriter interface {
		AppendRows(ctx context.Context, data [][]byte) (AppendResult, error)
		Close() error
	}

	tableStreamWriter struct {
		writer     StreamWriter
		descriptor protoreflect.MessageDescriptor
		mu         sync.RWMutex
		closed     bool // true once the writer is closed, preventing new appends
	}
	AppendResult interface {
		GetResult(ctx context.Context) (int64, error)
	}

	TableSchemaCache interface {
		Len() int
		Get(tableName string, now time.Time) (whutils.ModelTableSchema, bool)
		Has(tableName string, now time.Time) bool
		Peek(tableName string) (whutils.ModelTableSchema, bool)
		Set(tableName string, schema whutils.ModelTableSchema, now time.Time)
		Invalidate(tableName string)
	}

	streamWriterImpl struct {
		stream *managedwriter.ManagedStream
		client *managedwriter.Client
	}

	tableSchemaCacheImpl struct {
		mu    sync.RWMutex
		ttl   time.Duration
		items map[string]tableSchemaCacheItem
	}

	tableSchemaCacheItem struct {
		schema    whutils.ModelTableSchema
		expiresAt time.Time
	}

	event struct {
		Message struct {
			Metadata struct {
				Table   string            `json:"table"`
				Columns map[string]string `json:"columns"`
			} `json:"metadata"`
			Data map[string]any `json:"data"`
		} `json:"message"`
		Metadata struct {
			JobID int64 `json:"job_id"`
		} `json:"metadata"`
		MessageDataByteSize int64 `json:"-"` // Approximate row size for chunking: full line length as a conservative upper bound
	}

	tableEvents struct {
		tableName    string
		jobIDs       []int64
		events       []*event
		eventsSchema whutils.ModelTableSchema
	}
	tableProcessResult struct {
		succeededJobIDs []int64
		failedJobResult failedJobResult
	}

	failedJobResult struct {
		failedJobIDs   []int64
		failedJobError error
	}

	streamEventBatchesResult struct {
		succeededIDs []int64
		failedIDs    []int64
		failedError  error
	}

	discardEvent struct {
		tableName   string
		columnName  string
		columnValue any
		reason      string
		uuidTS      string
		rowID       any
		receivedAt  any
	}

	Row map[string]any
)
