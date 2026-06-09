package bqstreamv2

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"

	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestEncodeRowsMatchesProtoJSON(t *testing.T) {
	schema, md, rows := benchmarkSchemaAndRows(t, 10)

	expected, err := encodeRowsProtoJSON(rows, md, schema)
	require.NoError(t, err)

	actual, err := encodeRows(rows, md, schema)
	require.NoError(t, err)

	require.Len(t, actual, len(expected))
	for i := range expected {
		expectedMsg := dynamicpb.NewMessage(md)
		require.NoError(t, proto.Unmarshal(expected[i], expectedMsg))

		actualMsg := dynamicpb.NewMessage(md)
		require.NoError(t, proto.Unmarshal(actual[i], actualMsg))

		require.True(t, proto.Equal(expectedMsg, actualMsg))

	}
}

// encodeRowsProtoJSON is the previous implementation of encodeRows (JSON
// round-trip through protojson with a fresh dynamic message per row), kept
// here for benchmark comparison against the direct proto population.
func encodeRowsProtoJSON(rows []Row, md protoreflect.MessageDescriptor, schema whutils.ModelTableSchema) ([][]byte, error) {
	encodedRows := make([][]byte, 0, len(rows))
	for _, row := range rows {
		err := normalizeRow(row, schema)
		if err != nil {
			return nil, fmt.Errorf("normalizing row: %w", err)
		}
		message := dynamicpb.NewMessage(md)

		rowJSON, err := jsonrs.Marshal(row)
		if err != nil {
			return nil, fmt.Errorf("marshalling row: %w", err)
		}
		err = protojson.Unmarshal(rowJSON, message)
		if err != nil {
			return nil, fmt.Errorf("unmarshalling row: %w", err)
		}
		encoded, err := proto.Marshal(message)
		if err != nil {
			return nil, fmt.Errorf("marshalling row: %w", err)
		}
		encodedRows = append(encodedRows, encoded)
	}
	return encodedRows, nil
}

// BenchmarkEncodeRows
// BenchmarkEncodeRows/regular/direct-12         	      20	  52967585 ns/op	35204568 B/op	  399283 allocs/op
// BenchmarkEncodeRows/regular/protojson-12      	       9	 120727116 ns/op	55701667 B/op	  933552 allocs/op
// BenchmarkEncodeRows/wide-schema/direct-12     	      24	  47386208 ns/op	29448721 B/op	  311460 allocs/op
// BenchmarkEncodeRows/wide-schema/protojson-12  	      10	 103800333 ns/op	45458879 B/op	  753857 allocs/op
func BenchmarkEncodeRows(b *testing.B) {
	const numRows = 10000

	benchmarks := []struct {
		name   string
		setup  func(tb testing.TB, numRows int) (whutils.ModelTableSchema, protoreflect.MessageDescriptor, []Row)
		encode func(rows []Row, md protoreflect.MessageDescriptor, schema whutils.ModelTableSchema) ([][]byte, error)
	}{
		{name: "regular/direct", setup: benchmarkSchemaAndRows, encode: encodeRows},
		{name: "regular/protojson", setup: benchmarkSchemaAndRows, encode: encodeRowsProtoJSON},
		{name: "wide-schema/direct", setup: wideSchemaAndRows, encode: encodeRows},
		{name: "wide-schema/protojson", setup: wideSchemaAndRows, encode: encodeRowsProtoJSON},
	}
	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			schema, md, rows := bm.setup(b, numRows)

			b.ReportAllocs()
			for b.Loop() {
				_, err := bm.encode(rows, md, schema)
				require.NoError(b, err)
			}
		})
	}
}

func benchmarkSchemaAndRows(tb testing.TB, numRows int) (whutils.ModelTableSchema, protoreflect.MessageDescriptor, []Row) {
	tb.Helper()

	schema := whutils.ModelTableSchema{
		"id": "string", "user_id": "string", "event": "string", "event_text": "string",
		"context_destination_id": "string", "context_destination_type": "string",
		"context_source_id": "string", "context_source_type": "string",
		"context_ip": "string", "context_request_ip": "string",
		"product_id": "string", "review_id": "string", "review_body": "string",
		"rating": "int", "price": "float", "in_stock": "boolean",
		"original_timestamp": "datetime", "received_at": "datetime",
		"sent_at": "datetime", "timestamp": "datetime", "uuid_ts": "datetime", "loaded_at": "datetime",
	}

	md, err := descriptorForSchema(schema)
	require.NoError(tb, err)

	ts := time.Now().UTC().Format(time.RFC3339Nano)
	rows := make([]Row, 0, numRows)
	for i := range numRows {
		id := strconv.Itoa(i)
		rows = append(rows, Row{
			"id": id, "user_id": "user_" + id, "event": "product_reviewed", "event_text": "Product Reviewed",
			"context_destination_id": "destination1", "context_destination_type": "BQSTREAM_V2",
			"context_source_id": "source1", "context_source_type": "HTTP",
			"context_ip": "14.5.67.21", "context_request_ip": "14.5.67.21",
			"product_id": "9578257311", "review_id": "86ac1cd43",
			"review_body":        "OK for the price. It works but the material feels flimsy.",
			"rating":             float64(3),
			"price":              299.99,
			"in_stock":           true,
			"original_timestamp": ts, "received_at": ts,
			"sent_at": ts, "timestamp": ts, "uuid_ts": ts, "loaded_at": ts,
		})
	}
	return schema, md, rows
}

func wideSchemaAndRows(tb testing.TB, numRows int) (whutils.ModelTableSchema, protoreflect.MessageDescriptor, []Row) {
	tb.Helper()

	const totalColumns, populatedColumns = 300, 20

	columnTypes := []string{"string", "int", "float", "boolean", "datetime"}
	schema := make(whutils.ModelTableSchema, totalColumns)
	for i := range totalColumns {
		schema[fmt.Sprintf("column_%03d", i)] = columnTypes[i%len(columnTypes)]
	}

	md, err := descriptorForSchema(schema)
	require.NoError(tb, err)

	ts := time.Now().UTC().Format(time.RFC3339Nano)
	rows := make([]Row, 0, numRows)
	for range numRows {
		row := make(Row, populatedColumns)
		for i := range populatedColumns {
			columnName := fmt.Sprintf("column_%03d", i)
			switch schema[columnName] {
			case "string":
				row[columnName] = "some value"
			case "int":
				row[columnName] = float64(42)
			case "float":
				row[columnName] = 42.5
			case "boolean":
				row[columnName] = true
			case "datetime":
				row[columnName] = ts
			}
		}
		rows = append(rows, row)
	}
	return schema, md, rows
}
