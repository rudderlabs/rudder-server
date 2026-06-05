package bqstreamv2

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"time"

	"cloud.google.com/go/bigquery/storage/managedwriter/adapt"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func (m *Manager) descriptorForSchema(schema whutils.ModelTableSchema) (protoreflect.MessageDescriptor, error) {
	tableSchema, err := adapt.BQSchemaToStorageTableSchema(toBigQuerySchema(schema))
	if err != nil {
		return nil, fmt.Errorf("converting schema to storage table schema: %w", err)
	}
	desc, err := adapt.StorageSchemaToProto2Descriptor(tableSchema, "root")
	if err != nil {
		return nil, fmt.Errorf("converting storage schema to proto2 descriptor: %w", err)
	}
	md, ok := desc.(protoreflect.MessageDescriptor)
	if !ok {
		return nil, fmt.Errorf("unexpected descriptor type: %T", desc)
	}
	return md, nil
}

// streamWriterKey scopes cached writers to the destination's current project
// and namespace, so a config revision cannot keep streaming to the old table.
func streamWriterKey(cfg destConfig, tableName string) string {
	return cfg.ProjectID + ":" + cfg.Namespace + ":" + tableName
}

// encodeRows encodes rows for the Storage Write API by populating a reused
// dynamic message directly from the row values, avoiding a JSON round-trip
// and a per-row message allocation.
func encodeRows(rows []Row, md protoreflect.MessageDescriptor, schema whutils.ModelTableSchema) ([][]byte, error) {
	fields := md.Fields()
	fieldsByName := make(map[string]protoreflect.FieldDescriptor, fields.Len())
	for i := range fields.Len() {
		fd := fields.Get(i)
		fieldsByName[string(fd.Name())] = fd
	}

	message := dynamicpb.NewMessage(md)
	encodedRows := make([][]byte, 0, len(rows))
	for _, row := range rows {
		normalizeRow(row, schema)

		proto.Reset(message)
		for columnName, value := range row {
			if value == nil {
				continue
			}
			fd, ok := fieldsByName[columnName]
			if !ok {
				return nil, fmt.Errorf("encoding row: unknown column %q", columnName)
			}
			fieldValue, err := protoValueFor(fd, value)
			if err != nil {
				return nil, fmt.Errorf("encoding row: column %q: %w", columnName, err)
			}
			message.Set(fd, fieldValue)
		}

		encoded, err := proto.Marshal(message)
		if err != nil {
			return nil, fmt.Errorf("marshalling row: %w", err)
		}
		encodedRows = append(encodedRows, encoded)
	}
	return encodedRows, nil
}

// protoValueFor converts a row value for the given field, accepting the same
// coercions protojson did (integral floats and numeric strings for int64,
// etc.). Only the kinds reachable through dataTypesMap are handled; anything
// else fails loudly until support is added explicitly.
func protoValueFor(fd protoreflect.FieldDescriptor, value any) (protoreflect.Value, error) {
	switch fd.Kind() {
	case protoreflect.StringKind:
		if v, ok := value.(string); ok {
			return protoreflect.ValueOfString(v), nil
		}
	case protoreflect.BoolKind:
		if v, ok := value.(bool); ok {
			return protoreflect.ValueOfBool(v), nil
		}
	case protoreflect.Int64Kind:
		switch v := value.(type) {
		case int64:
			return protoreflect.ValueOfInt64(v), nil
		case int:
			return protoreflect.ValueOfInt64(int64(v)), nil
		case float64:
			if v == math.Trunc(v) {
				return protoreflect.ValueOfInt64(int64(v)), nil
			}
		case json.Number:
			if parsed, err := v.Int64(); err == nil {
				return protoreflect.ValueOfInt64(parsed), nil
			}
		case string:
			if parsed, err := strconv.ParseInt(v, 10, 64); err == nil {
				return protoreflect.ValueOfInt64(parsed), nil
			}
		}
	case protoreflect.DoubleKind:
		switch v := value.(type) {
		case float64:
			return protoreflect.ValueOfFloat64(v), nil
		case int64:
			return protoreflect.ValueOfFloat64(float64(v)), nil
		case int:
			return protoreflect.ValueOfFloat64(float64(v)), nil
		case json.Number:
			if parsed, err := v.Float64(); err == nil {
				return protoreflect.ValueOfFloat64(parsed), nil
			}
		case string:
			if parsed, err := strconv.ParseFloat(v, 64); err == nil {
				return protoreflect.ValueOfFloat64(parsed), nil
			}
		}
	}
	return protoreflect.Value{}, fmt.Errorf("invalid value of type %T for %s field", value, fd.Kind())
}

// normalizeRow converts datetime strings into the int64 epoch-micros
// representation expected by TIMESTAMP fields.
func normalizeRow(row Row, schema whutils.ModelTableSchema) {
	for col, v := range row {
		if v == nil {
			continue
		}
		switch schema[col] {
		case "datetime":
			s, ok := v.(string)
			if !ok {
				continue
			}
			if ts, err := time.Parse(time.RFC3339Nano, s); err == nil {
				row[col] = ts.UnixMicro()
			}
		}
	}
}
