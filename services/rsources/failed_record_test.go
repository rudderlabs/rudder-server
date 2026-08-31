package rsources

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"
)

// TestFailedRecordWireShape pins the internal v2 API leaf: {record, code, error}. The
// durable half of the same contract - the column and the read path - is in
// failed_records_error_response_test.go.
func TestFailedRecordWireShape(t *testing.T) {
	t.Run("A4.1 the error field rides alongside record and code", func(t *testing.T) {
		b, err := jsonrs.Marshal(FailedRecord{Record: []byte(`"rec-1"`), Code: 422, ErrorResponse: "rejected"})
		require.NoError(t, err)
		require.Equal(t, `"rec-1"`, gjson.GetBytes(b, "record").Raw)
		require.Equal(t, int64(422), gjson.GetBytes(b, "code").Int())
		require.Equal(t, "rejected", gjson.GetBytes(b, "error").String())
	})

	t.Run("A4.3 an old client ignores the new field", func(t *testing.T) {
		type oldFailedRecord struct {
			Record json.RawMessage `json:"record"`
			Code   int             `json:"code"`
		}
		var old oldFailedRecord
		require.NoError(t, jsonrs.Unmarshal([]byte(`{"record":"rec-1","code":422,"error":"rejected"}`), &old))
		require.Equal(t, 422, old.Code)
	})

	t.Run("A4.4 a new client tolerates a server that does not send the field", func(t *testing.T) {
		var rec FailedRecord
		require.NoError(t, jsonrs.Unmarshal([]byte(`{"record":"rec-1","code":422}`), &rec))
		require.Equal(t, 422, rec.Code)
		require.Empty(t, rec.ErrorResponse)
	})

	t.Run("the field is omitted entirely when the delegate captured nothing", func(t *testing.T) {
		b, err := jsonrs.Marshal(FailedRecord{Record: []byte(`"rec-1"`), Code: 422})
		require.NoError(t, err)
		require.False(t, gjson.GetBytes(b, "error").Exists())
		require.Equal(t, `{"record":"rec-1","code":422}`, string(b))
	})
}
