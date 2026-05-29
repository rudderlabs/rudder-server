package bqstreaming

import (
	"context"
	"errors"
	"fmt"

	managedwriter "cloud.google.com/go/bigquery/storage/managedwriter"
	"cloud.google.com/go/bigquery/storage/managedwriter/adapt"
	"google.golang.org/api/option"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/rudderlabs/rudder-go-kit/googleutil"

	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

// NewStreamWriter creates a new stream writer for the given destination configuration, table name, and table schema.
func (s *streamWriterFactoryImpl) NewStreamWriter(ctx context.Context, destConf destConfig, tableName string, tableSchema whutils.ModelTableSchema) (StreamWriter, error) {
	credBytes := []byte(destConf.Credentials)
	destinationTable := fmt.Sprintf("projects/%s/datasets/%s/tables/%s", destConf.ProjectID, destConf.Namespace, tableName)

	if err := googleutil.CompatibleServiceAccountJSON(credBytes); err != nil {
		return nil, fmt.Errorf("incompatible credentials: %w", err)
	}

	client, err := managedwriter.NewClient(ctx, destConf.ProjectID, option.WithAuthCredentialsJSON(option.ServiceAccount, credBytes))
	if err != nil {
		return nil, fmt.Errorf("creating managedwriter client: %w", err)
	}

	storageTableSchema, err := adapt.BQSchemaToStorageTableSchema(toBigQuerySchema(tableSchema))
	if err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("converting schema: %w", err)
	}

	desc, err := adapt.StorageSchemaToProto2Descriptor(storageTableSchema, "root")
	if err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("building descriptor: %w", err)
	}

	md, ok := desc.(protoreflect.MessageDescriptor)
	if !ok {
		_ = client.Close()
		return nil, fmt.Errorf("unexpected descriptor type: %T", desc)
	}

	descProto, err := adapt.NormalizeDescriptor(md)
	if err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("normalizing descriptor: %w", err)
	}

	options := []managedwriter.WriterOption{
		managedwriter.WithType(managedwriter.DefaultStream),
		managedwriter.WithDestinationTable(destinationTable),
		managedwriter.WithSchemaDescriptor(descProto),
	}

	stream, err := client.NewManagedStream(ctx, options...)
	if err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("creating managed stream: %w", err)
	}
	return &streamWriterImpl{stream: stream, client: client}, nil
}

// AppendRows appends the given data to the stream.
func (s *streamWriterImpl) AppendRows(ctx context.Context, data [][]byte) (AppendResult, error) {
	return s.stream.AppendRows(ctx, data)
}

// Close closes the stream writer and its underlying client.
func (s *streamWriterImpl) Close() error {
	streamErr := s.stream.Close()
	clientErr := s.client.Close()

	return errors.Join(streamErr, clientErr)
}
