package bqstreaming

import (
	"context"
	"sync"
	"time"

	managedwriter "cloud.google.com/go/bigquery/storage/managedwriter"
	"github.com/mitchellh/mapstructure"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type (
	Manager struct {
		appConfig                 *config.Config
		logger                    logger.Logger
		statsFactory              stats.Stats
		destination               *backendconfig.DestinationT
		integrationManagerCreator func(ctx context.Context, cfg destConfig) (IntegrationManager, error)

		streamWriterFactory StreamWriterFactory
		streamWritersMu     sync.Mutex
		streamWriters       map[string]tableStreamWriter

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

	StreamWriterFactory interface {
		NewStreamWriter(ctx context.Context, destConf destConfig, tableName string, tableSchema whutils.ModelTableSchema) (StreamWriter, error)
	}

	streamWriterFactoryImpl struct{}

	StreamWriter interface {
		AppendRows(ctx context.Context, data [][]byte) (AppendResult, error)
		Close() error
	}

	tableStreamWriter struct {
		writer     StreamWriter
		descriptor protoreflect.MessageDescriptor
	}

	AppendResult interface {
		GetResult(ctx context.Context) (int64, error)
	}

	TableSchemaCache interface {
		Len() int
		Get(key string, now time.Time) (whutils.ModelTableSchema, bool)
		Peek(key string) (whutils.ModelTableSchema, bool)
		Set(key string, schema whutils.ModelTableSchema, now time.Time)
		Invalidate(key string)
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

// Decode decodes the destination configuration from the given map. Also, converts the namespace to the provider case.
func (d *destConfig) Decode(m map[string]any) error {
	if err := mapstructure.Decode(m, d); err != nil {
		return err
	}
	d.Namespace = whutils.ToProviderCase(
		whutils.BQStreaming,
		whutils.ToSafeNamespace(whutils.BQStreaming, d.Namespace),
	)
	return nil
}

// setUUIDTimestamp sets the uuid_ts timestamp in the event message data and
// reports whether it was actually set.
func (m *Manager) setUUIDTimestamp(e *event, formattedTS string) bool {
	if e.Message.Metadata.Columns == nil {
		return false
	}
	if _, columnExists := e.Message.Metadata.Columns[uuidTSColumnName]; columnExists {
		e.Message.Data[uuidTSColumnName] = formattedTS
		return true
	}
	return false
}

// setLoadedAtTimestamp sets the loaded_at timestamp in the event message data
// and reports whether it was actually set.
func (m *Manager) setLoadedAtTimestamp(e *event, formattedTS string) bool {
	if e.Message.Metadata.Columns == nil {
		return false
	}
	if _, columnExists := e.Message.Metadata.Columns[loadedAtColumnName]; columnExists {
		e.Message.Data[loadedAtColumnName] = formattedTS
		return true
	}
	return false
}
