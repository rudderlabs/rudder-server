package bqstreamv2

import (
	"context"
	"sync"
	"time"

	managedwriter "cloud.google.com/go/bigquery/storage/managedwriter"

	"github.com/rudderlabs/rudder-go-kit/config"

	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type (
	// Manager streams events into BigQuery through the Storage Write API,
	// caching table schemas (TTL) and stream writers across uploads.
	Manager struct {
		now func() time.Time

		config struct {
			maxBufferCapacity config.ValueLoader[int64]
			tableWorkers      config.ValueLoader[int]
			maxChunkBytes     config.ValueLoader[int64]
			schemaCacheTTL    config.ValueLoader[time.Duration]
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

	StreamWriterFactory interface {
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
