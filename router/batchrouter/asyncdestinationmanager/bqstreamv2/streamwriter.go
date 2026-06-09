package bqstreamv2

import (
	"context"
	"errors"
	"fmt"

	managedwriter "cloud.google.com/go/bigquery/storage/managedwriter"
	"cloud.google.com/go/bigquery/storage/managedwriter/adapt"
	"google.golang.org/api/option"

	"github.com/rudderlabs/rudder-go-kit/googleutil"

	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func NewStreamWriterFactory(maxInflightRequests int, maxInflightBytes int64) StreamWriterFactory {
	return &streamWriterFactoryImpl{maxInflightRequests: maxInflightRequests, maxInflightBytes: maxInflightBytes}
}

// NewStreamWriter creates a managed stream for the table, with bounded
// in-flight appends for backpressure.
//
// Each writer dials its own client so that evicting a writer (e.g. after a
// table is deleted and re-created) also discards its gRPC connection: the
// Storage Write API resolves the default stream through connection-cached
// table metadata, and appends over an existing connection can keep failing
// with NotFound long after the table exists again. A fresh connection
// resolves the new table generation immediately.
func (s *streamWriterFactoryImpl) NewStreamWriter(ctx context.Context, destConf destConfig, tableName string, tableSchema whutils.ModelTableSchema) (StreamWriter, error) {
	credBytes := []byte(destConf.Credentials)
	if err := googleutil.CompatibleServiceAccountJSON(credBytes); err != nil {
		return nil, fmt.Errorf("incompatible credentials: %w", err)
	}

	client, err := managedwriter.NewClient(ctx, destConf.ProjectID, option.WithAuthCredentialsJSON(option.ServiceAccount, credBytes))
	if err != nil {
		return nil, fmt.Errorf("creating managedwriter client: %w", err)
	}
	defer func() {
		if err != nil {
			_ = client.Close()
		}
	}()

	md, err := descriptorForSchema(tableSchema)
	if err != nil {
		return nil, fmt.Errorf("converting schema: %w", err)
	}

	descProto, err := adapt.NormalizeDescriptor(md)
	if err != nil {
		return nil, fmt.Errorf("normalizing descriptor: %w", err)
	}

	destinationTable := fmt.Sprintf("projects/%s/datasets/%s/tables/%s", destConf.ProjectID, destConf.Namespace, tableName)
	stream, err := client.NewManagedStream(ctx,
		managedwriter.WithType(managedwriter.DefaultStream),
		managedwriter.WithDestinationTable(destinationTable),
		managedwriter.WithSchemaDescriptor(descProto),
		managedwriter.WithMaxInflightRequests(s.maxInflightRequests),
		managedwriter.WithMaxInflightBytes(int(s.maxInflightBytes)),
	)
	if err != nil {
		return nil, fmt.Errorf("creating managed stream: %w", err)
	}
	return &streamWriterImpl{stream: stream, client: client}, nil
}

// AppendRows appends the given data to the stream.
func (s *streamWriterImpl) AppendRows(ctx context.Context, data [][]byte) (AppendResult, error) {
	return s.stream.AppendRows(ctx, data)
}

// Close closes the stream and its client; they share one lifecycle.
func (s *streamWriterImpl) Close() error {
	return errors.Join(s.stream.Close(), s.client.Close())
}
