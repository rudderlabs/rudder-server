package transformer

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/araddon/dateparse"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	transformertest "github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/transformer"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	ptrans "github.com/rudderlabs/rudder-server/processor/transformer"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/types"
	"github.com/rudderlabs/rudder-server/warehouse/transformer/testhelper"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

var (
	reDateTime = regexp.MustCompile(
		`([+-]?\d{4})((-)((0[1-9]|1[0-2])(-([12]\d|0[1-9]|3[01])))([T\s]((([01]\d|2[0-3])((:)[0-5]\d))(:\d+)?)?(:[0-5]\d([.]\d+)?)?([zZ]|([+-])([01]\d|2[0-3]):?([0-5]\d)?)?)?)`,
	)
	reValidDateTime = regexp.MustCompile(`^(19[7-9]\d|20\d{2})-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])(?:[T\s]([01]\d|2[0-3]):([0-5]\d):([0-5]\d)(\.\d{0,9})?(Z)?)?$`)
)

var errInvalidDateTime = errors.New("invalid datetime format")

func FuzzTransformer(f *testing.F) {
	f.Skip()

	pool, err := dockertest.NewPool("")
	require.NoError(f, err)

	transformerResource, err := transformertest.Setup(pool, f)
	require.NoError(f, err)

	for _, destType := range whutils.WarehouseDestinations {
		f.Log("Providing seed corpus for event types for destination type: ", destType)
		f.Add(destType, `{}`, `{"type":"alias","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","previousId":"previousId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","context":{"traits":{"email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`)
		f.Add(destType, `{}`, `{"type":"alias","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","previousId":"previousId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","traits":{"title":"Home | RudderStack","url":"https://www.rudderstack.com"}}`)
		f.Add(destType, `{}`, `{"type":"alias","messageId":"messageId","userId":"userId","previousId":"previousId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","traits":{"title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`)
		f.Add(destType, `{}`, `{"type":"page","name":"Home","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`)
		f.Add(destType, `{}`, `{"type":"page","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","properties":{"name":"Home","title":"Home | RudderStack","url":"https://www.rudderstack.com"}}`)
		f.Add(destType, `{}`, `{"type":"page","messageId":"messageId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","properties":{"name":"Home","title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`)
		f.Add(destType, `{}`, `{"type":"screen","name":"Main","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`)
		f.Add(destType, `{}`, `{"type":"screen","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","properties":{"name":"Main","title":"Home | RudderStack","url":"https://www.rudderstack.com"}}`)
		f.Add(destType, `{}`, `{"type":"screen","messageId":"messageId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","properties":{"name":"Main","title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`)
		f.Add(destType, `{}`, `{"type":"group","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","groupId":"groupId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","context":{"traits":{"email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`)
		f.Add(destType, `{}`, `{"type":"group","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","groupId":"groupId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","traits":{"title":"Home | RudderStack","url":"https://www.rudderstack.com"}}`)
		f.Add(destType, `{}`, `{"type":"group","messageId":"messageId","userId":"userId","groupId":"groupId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","traits":{"title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`)
		f.Add(destType, `{}`, `{"type":"merge","mergeProperties":[{"type":"email","value":"alex@example.com"},{"type":"mobile","value":"+1-202-555-0146"}]}`)
		f.Add(destType, `{}`, `{"type":"merge"}`)
		f.Add(destType, `{}`, `{"type":"merge", "mergeProperties": "invalid"}`)
		f.Add(destType, `{}`, `{"type":"merge", "mergeProperties": []}`)
		f.Add(destType, `{}`, `{"type":"merge","mergeProperties":[{"type":"email","value":"alex@example.com"}]}`)
		f.Add(destType, `{}`, `{"type":"merge","mergeProperties":["invalid",{"type":"email","value":"alex@example.com"}]}`)
		f.Add(destType, `{}`, `{"type":"merge","mergeProperties":[{"type":"email","value":"alex@example.com"},"invalid"]}`)
		f.Add(destType, `{}`, `{"type":"merge","mergeProperties":[{"type1":"email","value1":"alex@example.com"},{"type1":"mobile","value1":"+1-202-555-0146"}]}`)
		f.Add(destType, `{}`, `{"type":"merge","mergeProperties":[{"type1":"email","value1":"alex@example.com"},{"type1":"mobile","value1":"+1-202-555-0146"}]}`)
		f.Add(destType, `{}`, `{"type":"alias","messageId":"messageId","anonymousId":"anonymousId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","traits":{"title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`)
		f.Add(destType, `{}`, `{"type":"alias","messageId":"messageId","anonymousId":"anonymousId","userId":"","previousId":"","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","traits":{"title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`)
		f.Add(destType, `{}`, `{"type":"page","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","properties":{"name":"Home","title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`)
		f.Add(destType, `{}`, `{"type":"page","messageId":"messageId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","properties":{"name":"Home","title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`)
		f.Add(destType, `{}`, `{"type":"track","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","event":"event","request_ip":"5.6.7.8","userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`)
		f.Add(destType, `{}`, `{"type":"track","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","event":"event","request_ip":"5.6.7.8","properties":{"review_id":"86ac1cd43","product_id":"9578257311"},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`)
		f.Add(destType, `{}`, `{"type":"track","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","event":"event","request_ip":"5.6.7.8","properties":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."}}`)
		f.Add(destType, `{}`, `{"type":"track","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","event":"accounts","request_ip":"5.6.7.8","properties":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`)
		f.Add(destType, `{}`, `{"type":"track","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","event":"accounts","request_ip":"5.6.7.8","properties":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"},"integrations":{"POSTGRES":{"options":{"skipReservedKeywordsEscaping":true}}}}`)
		f.Add(destType, `{}`, `{"type":"track","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","event":"users","request_ip":"5.6.7.8","properties":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`)
		f.Add(destType, `{}`, `{"type":"track","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","event":"","request_ip":"5.6.7.8","properties":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`)
		f.Add(destType, `{}`, `{"type":"track","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","properties":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`)
		f.Add(destType, `{}`, `{"type":"track","messageId":"messageId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","event":"event","properties":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`)
		f.Add(destType, `{}`, `{"type":"track","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","event":"event","request_ip":"5.6.7.8","properties":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"},"integrations":{"POSTGRES":{"options":{"skipTracksTable":true}}}}`)
		f.Add(destType, `{}`, `{"type":"identify","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`)
		f.Add(destType, `{}`, `{"type":"identify","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","traits":{"review_id":"86ac1cd43","product_id":"9578257311"},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`)
		f.Add(destType, `{}`, `{"type":"identify","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","traits":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"ip":"1.2.3.4"}}`)
		f.Add(destType, `{}`, `{"type":"identify","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","traits":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."}}`)
		f.Add(destType, `{}`, `{"type":"identify","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","traits":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"user_id":"user_id","rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`)
		f.Add(destType, `{}`, `{"type":"identify","messageId":"messageId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","traits":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`)
		f.Add(destType, `{}`, `{"type":"identify","messageId":"messageId","anonymousId":"anonymousId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","traits":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`)
		f.Add(destType, `{}`, `{"type":"identify","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","traits":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"},"integrations":{"POSTGRES":{"options":{"skipUsersTable":true}}}}`)
		f.Add(destType, `{}`, `{"type":"extract","recordId":"recordID","event":"event","receivedAt":"2021-09-01T00:00:00.000Z","context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`)
		f.Add(destType, `{}`, `{"type":"extract","recordId":"recordID","event":"event","receivedAt":"2021-09-01T00:00:00.000Z","properties":{"name":"Home","title":"Home | RudderStack","url":"https://www.rudderstack.com"}}`)
		f.Add(destType, `{}`, `{"type":"extract","recordId":"recordID","event":"accounts","receivedAt":"2021-09-01T00:00:00.000Z","properties":{"name":"Home","title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`)
		f.Add(destType, `{}`, `{"type":"extract","recordId":"recordID","event":"accounts","receivedAt":"2021-09-01T00:00:00.000Z","properties":{"name":"Home","title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"},"integrations":{"POSTGRES":{"options":{"skipReservedKeywordsEscaping":true}}}}`)
		f.Add(destType, `{}`, `{"type":"extract","recordId":"recordID","event":"users","receivedAt":"2021-09-01T00:00:00.000Z","properties":{"name":"Home","title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`)

		f.Log("Providing seed corpus for column names for destination type: ", destType)
		columnNames := []string{
			// SQL keywords and reserved words
			"select", "from", "where", "and", "or", "not", "insert", "update", "delete",
			"create", "alter", "drop", "table", "index", "view", "primary", "foreign",
			"key", "constraint", "default", "null", "unique", "check", "references",
			"join", "inner", "outer", "left", "right", "full", "on", "group", "by",
			"having", "order", "asc", "desc", "limit", "offset", "union", "all",
			"distinct", "as", "in", "between", "like", "is", "null", "true", "false",

			// Data types (which can vary by database system)
			"int", "integer", "bigint", "smallint", "tinyint", "decimal", "numeric",
			"float", "real", "double", "precision", "char", "varchar", "text", "date",
			"time", "timestamp", "datetime", "boolean", "blob", "clob", "binary",

			// Names starting with numbers or special characters
			"1column", "2_column", "@column", "#column", "$column",

			// Names with spaces or special characters
			"column name", "column-name", "column.name", "column@name", "column#name",
			"column$name", "column%name", "column&name", "column*name", "column+name",
			"column/name", "column\\name", "column'name", "column\"name", "column`name",

			// Names with non-ASCII characters
			"columnñame", "colûmnname", "columnнаме", "列名", "カラム名",

			// Very long names (may exceed maximum length in some databases)
			"this_is_a_very_long_column_name_that_exceeds_the_maximum_allowed_length_in_many_database_systems",

			// Names that could be confused with functions
			"count", "sum", "avg", "max", "min", "first", "last", "now", "current_timestamp",

			// Names with potential encoding issues
			"column\u0000name", "column\ufffdname",

			// Names that might conflict with ORM conventions
			"id", "_id", "created_at", "updated_at", "deleted_at",

			// Names that might conflict with common programming conventions
			"class", "interface", "enum", "struct", "function", "var", "let", "const",

			// Names with emoji or other Unicode symbols
			"column😀name", "column→name", "column★name",

			// Names with mathematical symbols
			"column+name", "column-name", "column*name", "column/name", "column^name",
			"column=name", "column<name", "column>name", "column≠name", "column≈name",

			// Names with comment-like syntax
			"column--name", "column/*name*/",

			// Names that might be interpreted as operators
			"column||name", "column&&name", "column!name", "column?name",

			// Names with control characters
			"column\tname", "column\nname", "column\rname",

			// Names that might conflict with schema notation
			"schema.column", "database.schema.column",

			// Names with brackets or parentheses
			"column(name)", "column[name]", "column{name}",

			// Names with quotes
			"'column'", "\"column\"", "`column`",

			// Names that might be interpreted as aliases
			"column as alias",

			// Names that might conflict with database-specific features
			"rowid", "oid", "xmin", "ctid", // These are specific to certain databases

			// Names that might conflict with common column naming conventions
			"fk_", "idx_", "pk_", "ck_", "uq_",

			// Names with invisible characters
			"column\u200bname", // Zero-width space
			"column\u00A0name", // Non-breaking space

			// Names with combining characters
			"columǹ", // 'n' with combining grave accent

			// Names with bidirectional text
			"column\u202Ename\u202C", // Using LTR and RTL markers

			// Names with unusual capitalization
			"COLUMN", "Column", "cOlUmN",

			// Names that are empty or only whitespace
			"", " ", "\t", "\n",

			// Names with currency symbols
			"column¢name", "column£name", "column€name", "column¥name",

			// Names with less common punctuation
			"column·name", "column…name", "column•name", "column‽name",

			// Names with fractions or other numeric forms
			"column½name", "column²name", "columnⅣname",
		}
		for _, columnName := range columnNames {
			f.Add(destType, `{}`, `{"type":"alias","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","previousId":"previousId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","context":{"traits":{"`+columnName+`":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`)
			f.Add(destType, `{}`, `{"type":"page","messageId":"messageId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","properties":{"name":"Home","title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"name":"Richard Hendricks","`+columnName+`":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`)
			f.Add(destType, `{}`, `{"type":"screen","messageId":"messageId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","properties":{"name":"Main","title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"name":"Richard Hendricks","`+columnName+`":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`)
			f.Add(destType, `{}`, `{"type":"group","messageId":"messageId","userId":"userId","groupId":"groupId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","traits":{"title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"`+columnName+`":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`)
			f.Add(destType, `{}`, `{"type":"track","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","event":"event","request_ip":"5.6.7.8","properties":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","`+columnName+`":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"},"integrations":{"POSTGRES":{"options":{"skipTracksTable":true}}}}`)
			f.Add(destType, `{}`, `{"type":"identify","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","traits":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","`+columnName+`":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"},"integrations":{"POSTGRES":{"options":{"skipUsersTable":true}}}}`)
			f.Add(destType, `{}`, `{"type":"extract","recordId":"recordID","event":"users","receivedAt":"2021-09-01T00:00:00.000Z","properties":{"name":"Home","title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"name":"Richard Hendricks","`+columnName+`":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`)
		}

		f.Log("Providing seed corpus for event names for destination type: ", destType)
		eventNames := []string{
			"omega",
			"omega v2 ",
			"9mega",
			"mega&",
			"ome$ga",
			"omega$",
			"ome_ ga",
			"9mega________-________90",
			"Cízǔ",
			"Rudderstack",
			"___",
			"group",
			"k3_namespace",
			"k3_namespace",
			"select",
			"drop",
			"create",
			"alter",
			"index",
			"table",
			"from",
			"where",
			"join",
			"union",
			"insert",
			"update",
			"delete",
			"truncate",
			"1invalid",
			"invalid-name",
			"invalid.name",
			"name with spaces",
			"name@with@special@chars",
			"verylongnamethatiswaytoolongforadatabasetablenameandexceedsthemaximumlengthallowed",
			"ñáme_wíth_áccents",
			"schema.tablename",
			"'quoted_name'",
			"name--with--comments",
			"name/*with*/comments",
			"name;with;semicolons",
			"name,with,commas",
			"name(with)parentheses",
			"name[with]brackets",
			"name{with}braces",
			"name+with+plus",
			"name=with=equals",
			"name<with>angle_brackets",
			"name|with|pipes",
			"name\\with\\backslashes",
			"name/with/slashes",
			"name\"with\"quotes",
			"name'with'single_quotes",
			"name`with`backticks",
			"name!with!exclamation",
			"name?with?question",
			"name#with#hash",
			"name%with%percent",
			"name^with^caret",
			"name~with~tilde",
			"primary",
			"foreign",
			"key",
			"constraint",
			"default",
			"null",
			"not null",
			"auto_increment",
			"identity",
			"unique",
			"check",
			"references",
			"on delete",
			"on update",
			"cascade",
			"restrict",
			"set null",
			"set default",
			"temporary",
			"temp",
			"view",
			"function",
			"procedure",
			"trigger",
			"序列化",     // Chinese for "serialization"
			"テーブル",    // Japanese for "table"
			"таблица", // Russian for "table"
			"0day",
			"_system",
			"__hidden__",
			"name:with:colons",
			"name★with★stars",
			"name→with→arrows",
			"name•with•bullets",
			"name‼with‼double_exclamation",
			"name⁉with⁉interrobang",
			"name‽with‽interrobang",
			"name⚠with⚠warning",
			"name☢with☢radiation",
			"name❗with❗exclamation",
			"name❓with❓question",
			"name⏎with⏎return",
			"name⌘with⌘command",
			"name⌥with⌥option",
			"name⇧with⇧shift",
			"name⌃with⌃control",
			"name⎋with⎋escape",
			"name␣with␣space",
			"name⍽with⍽space",
			"name¶with¶pilcrow",
			"name§with§section",
			"name‖with‖double_vertical_bar",
			"name¦with¦broken_bar",
			"name¬with¬negation",
			"name¤with¤currency",
			"name‰with‰permille",
			"name‱with‱permyriad",
			"name∞with∞infinity",
			"name≠with≠not_equal",
			"name≈with≈approximately_equal",
			"name≡with≡identical",
			"name√with√square_root",
			"name∛with∛cube_root",
			"name∜with∜fourth_root",
			"name∫with∫integral",
			"name∑with∑sum",
			"name∏with∏product",
			"name∀with∀for_all",
			"name∃with∃exists",
			"name∄with∄does_not_exist",
			"name∅with∅empty_set",
			"name∈with∈element_of",
			"name∉with∉not_element_of",
			"name∋with∋contains",
			"name∌with∌does_not_contain",
			"name∩with∩intersection",
			"name∪with∪union",
			"name⊂with⊂subset",
			"name⊃with⊃superset",
			"name⊄with⊄not_subset",
			"name⊅with⊅not_superset",
		}
		for _, eventName := range eventNames {
			f.Add(destType, `{}`, `{"type":"track","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","event":"`+eventName+`","request_ip":"5.6.7.8","properties":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"},"integrations":{"POSTGRES":{"options":{"skipTracksTable":true}}}}`)
			f.Add(destType, `{}`, `{"type":"extract","recordId":"recordID","event":"`+eventName+`","receivedAt":"2021-09-01T00:00:00.000Z","properties":{"name":"Home","title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`)
		}

		f.Log("Providing seed corpus for random columns for destination type: ", destType)
		f.Add(destType, `{}`, testhelper.AddRandomColumns(`{"type":"alias","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","previousId":"previousId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","traits":{"title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"email":"rhedricks@example.com","logins":2},%s,"ip":"1.2.3.4"}}`, 500))
		f.Add(destType, `{}`, testhelper.AddRandomColumns(`{"type":"page","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","properties":{"name":"Home","title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},%s,"ip":"1.2.3.4"}}`, 500))
		f.Add(destType, `{}`, testhelper.AddRandomColumns(`{"type":"screen","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","properties":{"name":"Main","title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},%s,"ip":"1.2.3.4"}}`, 500))
		f.Add(destType, `{}`, testhelper.AddRandomColumns(`{"type":"group","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","groupId":"groupId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","traits":{"title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"email":"rhedricks@example.com","logins":2},%s,"ip":"1.2.3.4"}}`, 500))
		f.Add(destType, `{}`, testhelper.AddRandomColumns(`{"type":"track","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","event":"event","request_ip":"5.6.7.8","properties":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},%s,"ip":"1.2.3.4"}}`, 500))
		f.Add(destType, `{}`, testhelper.AddRandomColumns(`{"type":"identify","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","traits":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},%s,"ip":"1.2.3.4"}}`, 500))
		f.Add(destType, `{}`, testhelper.AddRandomColumns(`{"type":"extract","recordId":"recordID","event":"event","receivedAt":"2021-09-01T00:00:00.000Z","properties":{"name":"Home","title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},%s,"ip":"1.2.3.4"}}`, 500))

		f.Log("Providing seed corpus for big columns for destination type: ", destType)
		f.Add(destType, `{}`, testhelper.AddLargeColumns(`{"type":"alias","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","previousId":"previousId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","traits":{"title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"email":"rhedricks@example.com","logins":2},%s,"ip":"1.2.3.4"}}`, 10))
		f.Add(destType, `{}`, testhelper.AddLargeColumns(`{"type":"page","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","properties":{"name":"Home","title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},%s,"ip":"1.2.3.4"}}`, 10))
		f.Add(destType, `{}`, testhelper.AddLargeColumns(`{"type":"screen","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","properties":{"name":"Main","title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},%s,"ip":"1.2.3.4"}}`, 10))
		f.Add(destType, `{}`, testhelper.AddLargeColumns(`{"type":"group","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","groupId":"groupId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","traits":{"title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"email":"rhedricks@example.com","logins":2},%s,"ip":"1.2.3.4"}}`, 10))
		f.Add(destType, `{}`, testhelper.AddLargeColumns(`{"type":"track","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","event":"event","request_ip":"5.6.7.8","properties":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},%s,"ip":"1.2.3.4"}}`, 10))
		f.Add(destType, `{}`, testhelper.AddLargeColumns(`{"type":"identify","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","traits":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},%s,"ip":"1.2.3.4"}}`, 10))
		f.Add(destType, `{}`, testhelper.AddLargeColumns(`{"type":"extract","recordId":"recordID","event":"event","receivedAt":"2021-09-01T00:00:00.000Z","properties":{"name":"Home","title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},%s,"ip":"1.2.3.4"}}`, 10))

		f.Log("Providing seed corpus for nested levels for destination type: ", destType)
		f.Add(destType, `{}`, testhelper.AddNestedLevels(`{"type":"alias","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","previousId":"previousId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","traits":{"title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"email":"rhedricks@example.com","logins":2},%s,"ip":"1.2.3.4"}}`, 10))
		f.Add(destType, `{}`, testhelper.AddNestedLevels(`{"type":"page","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","properties":{"name":"Home","title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},%s,"ip":"1.2.3.4"}}`, 10))
		f.Add(destType, `{}`, testhelper.AddNestedLevels(`{"type":"screen","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","properties":{"name":"Main","title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},%s,"ip":"1.2.3.4"}}`, 10))
		f.Add(destType, `{}`, testhelper.AddNestedLevels(`{"type":"group","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","groupId":"groupId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","traits":{"title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"email":"rhedricks@example.com","logins":2},%s,"ip":"1.2.3.4"}}`, 10))
		f.Add(destType, `{}`, testhelper.AddNestedLevels(`{"type":"track","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","event":"event","request_ip":"5.6.7.8","properties":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},%s,"ip":"1.2.3.4"}}`, 10))
		f.Add(destType, `{}`, testhelper.AddNestedLevels(`{"type":"identify","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","traits":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},%s,"ip":"1.2.3.4"}}`, 10))
		f.Add(destType, `{}`, testhelper.AddNestedLevels(`{"type":"extract","recordId":"recordID","event":"event","receivedAt":"2021-09-01T00:00:00.000Z","properties":{"name":"Home","title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},%s,"ip":"1.2.3.4"}}`, 10))

		f.Log("Providing seed corpus for channel for destination type: ", destType)
		for _, channel := range []string{"web", "sources", "android", "ios", "server", "backend", "frontend", "mobile", "desktop", "webapp", "mobileapp", "desktopapp", "website"} {
			f.Add(destType, `{}`, `{"type":"alias","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","previousId":"previousId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"`+channel+`","request_ip":"5.6.7.8","context":{"traits":{"email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`)
			f.Add(destType, `{}`, `{"type":"page","messageId":"messageId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"`+channel+`","properties":{"name":"Home","title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`)
			f.Add(destType, `{}`, `{"type":"screen","messageId":"messageId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"`+channel+`","properties":{"name":"Main","title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`)
			f.Add(destType, `{}`, `{"type":"group","messageId":"messageId","userId":"userId","groupId":"groupId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"`+channel+`","traits":{"title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`)
			f.Add(destType, `{}`, `{"type":"track","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"`+channel+`","event":"event","request_ip":"5.6.7.8","properties":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"},"integrations":{"POSTGRES":{"options":{"skipTracksTable":true}}}}`)
			f.Add(destType, `{}`, `{"type":"identify","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"`+channel+`","request_ip":"5.6.7.8","traits":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"},"integrations":{"POSTGRES":{"options":{"skipUsersTable":true}}}}`)
			f.Add(destType, `{}`, `{"type":"extract","recordId":"recordID","event":"users","receivedAt":"2021-09-01T00:00:00.000Z","channel":"`+channel+`","properties":{"name":"Home","title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`)
		}

		f.Log("Providing seed corpus for caps key names for destination type: ", destType)
		f.Add(destType, `{}`, `{"TYPE":"alias","MESSAGEID":"messageId","ANONYMOUSID":"anonymousId","USERID":"userId","PREVIOUSID":"previousId","SENTAT":"2021-09-01T00:00:00.000Z","TIMESTAMP":"2021-09-01T00:00:00.000Z","RECEIVEDAT":"2021-09-01T00:00:00.000Z","ORIGINALTIMESTAMP":"2021-09-01T00:00:00.000Z","CHANNEL":"web","REQUEST_IP":"5.6.7.8","CONTEXT":{"TRAITS":{"EMAIL":"rhedrICKS@example.com","LOGINS":2},"IP":"1.2.3.4"}}`)
		f.Add(destType, `{}`, `{"TYPE":"page","MESSAGEID":"messageId","USERID":"userId","SENTAT":"2021-09-01T00:00:00.000Z","TIMESTAMP":"2021-09-01T00:00:00.000Z","RECEIVEDAT":"2021-09-01T00:00:00.000Z","ORIGINALTIMESTAMP":"2021-09-01T00:00:00.000Z","PROPERTIES":{"NAME":"Home","TITLE":"Home | RudderStack","URL":"https://www.rudderstack.com"},"CONTEXT":{"TRAITS":{"NAME":"Richard Hendricks","EMAIL":"rhedrICKS@example.com","LOGINS":2},"IP":"1.2.3.4"}}`)
		f.Add(destType, `{}`, `{"TYPE":"screen","MESSAGEID":"messageId","USERID":"userId","SENTAT":"2021-09-01T00:00:00.000Z","TIMESTAMP":"2021-09-01T00:00:00.000Z","RECEIVEDAT":"2021-09-01T00:00:00.000Z","ORIGINALTIMESTAMP":"2021-09-01T00:00:00.000Z","PROPERTIES":{"NAME":"Main","TITLE":"Home | RudderStack","URL":"https://www.rudderstack.com"},"CONTEXT":{"TRAITS":{"NAME":"Richard Hendricks","EMAIL":"rhedrICKS@example.com","LOGINS":2},"IP":"1.2.3.4"}}`)
		f.Add(destType, `{}`, `{"TYPE":"group","MESSAGEID":"messageId","USERID":"userId","GROUPID":"groupId","SENTAT":"2021-09-01T00:00:00.000Z","TIMESTAMP":"2021-09-01T00:00:00.000Z","RECEIVEDAT":"2021-09-01T00:00:00.000Z","ORIGINALTIMESTAMP":"2021-09-01T00:00:00.000Z","TRAITS":{"TITLE":"Home | RudderStack","URL":"https://www.rudderstack.com"},"CONTEXT":{"TRAITS":{"EMAIL":"rhedrICKS@example.com","LOGINS":2},"IP":"1.2.3.4"}}`)
		f.Add(destType, `{}`, `{"TYPE":"track","MESSAGEID":"messageId","ANONYMOUSID":"anonymousId","USERID":"userId","SENTAT":"2021-09-01T00:00:00.000Z","TIMESTAMP":"2021-09-01T00:00:00.000Z","RECEIVEDAT":"2021-09-01T00:00:00.000Z","ORIGINALTIMESTAMP":"2021-09-01T00:00:00.000Z","CHANNEL":"web","EVENT":"event","REQUEST_IP":"5.6.7.8","PROPERTIES":{"REVIEW_ID":"86ac1cd43","PRODUCT_ID":"9578257311"},"USERPROPERTIES":{"RATING":3.0,"REVIEW_BODY":"OK for the price. It works but the material feels flimsy."},"CONTEXT":{"TRAITS":{"NAME":"Richard Hendricks","EMAIL":"rhedrICKS@example.com","LOGINS":2},"IP":"1.2.3.4"},"INTEGRATIONS":{"POSTGRES":{"OPTIONS":{"SKIPTRACKSTABLE":true}}}}`)
		f.Add(destType, `{}`, `{"TYPE":"identify","MESSAGEID":"messageId","ANONYMOUSID":"anonymousId","USERID":"userId","SENTAT":"2021-09-01T00:00:00.000Z","TIMESTAMP":"2021-09-01T00:00:00.000Z","RECEIVEDAT":"2021-09-01T00:00:00.000Z","ORIGINALTIMESTAMP":"2021-09-01T00:00:00.000Z","CHANNEL":"web","REQUEST_IP":"5.6.7.8","TRAITS":{"REVIEW_ID":"86ac1cd43","PRODUCT_ID":"9578257311"},"USERPROPERTIES":{"RATING":3.0,"REVIEW_BODY":"OK for the price. It works but the material feels flimsy."},"CONTEXT":{"TRAITS":{"NAME":"Richard Hendricks","EMAIL":"rhedrICKS@example.com","LOGINS":2},"IP":"1.2.3.4"},"INTEGRATIONS":{"POSTGRES":{"OPTIONS":{"SKIPUSERSTABLE":true}}}}`)
		f.Add(destType, `{}`, `{"TYPE":"extract","RECORDID":"recordID","EVENT":"users","RECEIVEDAT":"2021-09-01T00:00:00.000Z","PROPERTIES":{"NAME":"Home","TITLE":"Home | RudderStack","URL":"https://www.rudderstack.com"},"CONTEXT":{"TRAITS":{"NAME":"Richard Hendricks","EMAIL":"rhedrICKS@example.com","LOGINS":2},"IP":"1.2.3.4"}}`)

		f.Log("Providing seed corpus for caps event type for destination type: ", destType)
		f.Add(destType, `{}`, `{"type":"ALIAS","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","previousId":"previousId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","context":{"traits":{"email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`)
		f.Add(destType, `{}`, `{"type":"PAGE","messageId":"messageId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","properties":{"name":"Home","title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`)
		f.Add(destType, `{}`, `{"type":"SCREEN","messageId":"messageId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","properties":{"name":"Main","title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`)
		f.Add(destType, `{}`, `{"type":"GROUP","messageId":"messageId","userId":"userId","groupId":"groupId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","traits":{"title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`)
		f.Add(destType, `{}`, `{"type":"TRACK","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","event":"event","request_ip":"5.6.7.8","properties":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"},"integrations":{"POSTGRES":{"options":{"skipTracksTable":true}}}}`)
		f.Add(destType, `{}`, `{"type":"IDENTIFY","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","traits":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"},"integrations":{"POSTGRES":{"options":{"skipUsersTable":true}}}}`)
		f.Add(destType, `{}`, `{"type":"EXTRACT","recordId":"recordID","event":"users","receivedAt":"2021-09-01T00:00:00.000Z","properties":{"name":"Home","title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`)

		f.Log("Providing seed corpus for integrations options for destination type: ", destType)
		integrationOpts := []string{
			`{"options":{"skipReservedKeywordsEscaping":true}}`,
			`{"options":{"skipReservedKeywordsEscaping":false}}`,
			`{"options":{"useBlendoCasing":false}}`,
			`{"options":{"useBlendoCasing":true}}`,
			`{"options":{"skipTracksTable":true}}`,
			`{"options":{"skipTracksTable":false}}`,
			`{"options":{"skipUsersTable":true}}`,
			`{"options":{"skipUsersTable":false}}`,
			`{"options":{"jsonPaths":["context", "properties", "userProperties", "context.traits"]}}`,
			`{"options":{"jsonPaths":["track.context", "track.properties", "track.userProperties", "track.context.traits"]}}`,
			`{"options":{"jsonPaths":["properties", "context.traits"]}}`,
			`{"options":{"jsonPaths":["page.properties", "page.context.traits"]}}`,
			`{"options":{"jsonPaths":["screen.properties", "screen.context.traits"]}}`,
			`{"options":{"jsonPaths":["alias.properties", "alias.context.traits"]}}`,
			`{"options":{"jsonPaths":["group.properties", "group.context.traits"]}}`,
			`{"options":{"jsonPaths":["extract.properties", "extract.context.traits"]}}`,
			`{"options":{"jsonPaths":["identify.traits", "identify.context.traits", "identify.userProperties"]}}`,
			`{"options":{"skipReservedKeywordsEscaping":true,"useBlendoCasing":true,"skipTracksTable":true,"skipUsersTable":true,"jsonPaths":["context", "properties", "userProperties", "context.traits"]}}`,
			`{"options":{"skipReservedKeywordsEscaping":false,"useBlendoCasing":false,"skipTracksTable":false,"skipUsersTable":false,"jsonPaths":["track.context", "track.properties", "track.userProperties", "track.context.traits"]}}`,
			`{"options":{"skipReservedKeywordsEscaping":true,"useBlendoCasing":true,"skipTracksTable":true,"skipUsersTable":true,"jsonPaths":["properties", "context.traits"]}}`,
			`{"options":{"skipReservedKeywordsEscaping":false,"useBlendoCasing":false,"skipTracksTable":false,"skipUsersTable":false,"jsonPaths":["page.properties", "page.context.traits"]}}`,
			`{"options":{"skipReservedKeywordsEscaping":true,"useBlendoCasing":true,"skipTracksTable":true,"skipUsersTable":true,"jsonPaths":["screen.properties", "screen.context.traits"]}}`,
			`{"options":{"skipReservedKeywordsEscaping":false,"useBlendoCasing":false,"skipTracksTable":false,"skipUsersTable":false,"jsonPaths":["alias.properties", "alias.context.traits"]}}`,
			`{"options":{"skipReservedKeywordsEscaping":true,"useBlendoCasing":true,"skipTracksTable":true,"skipUsersTable":true,"jsonPaths":["group.properties", "group.context.traits"]}}`,
			`{"options":{"skipReservedKeywordsEscaping":false,"useBlendoCasing":false,"skipTracksTable":false,"skipUsersTable":false,"jsonPaths":["extract.properties", "extract.context.traits"]}}`,
			`{"options":{"skipReservedKeywordsEscaping":true,"useBlendoCasing":true,"skipTracksTable":true,"skipUsersTable":true,"jsonPaths":["identify.traits", "identify.context.traits", "identify.userProperties"]}}`,
		}
		for _, opt := range integrationOpts {
			intrOptsPayload := fmt.Sprintf(`"integrations":{"%s":%s}`, destType, opt)
			f.Add(destType, `{}`, fmt.Sprintf(`{"type":"alias","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","previousId":"previousId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","context":{"traits":{"email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"},%s}`, intrOptsPayload))
			f.Add(destType, `{}`, fmt.Sprintf(`{"type":"page","messageId":"messageId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","properties":{"name":"Home","title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"},%s}`, intrOptsPayload))
			f.Add(destType, `{}`, fmt.Sprintf(`{"type":"screen","messageId":"messageId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","properties":{"name":"Main","title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"},%s}`, intrOptsPayload))
			f.Add(destType, `{}`, fmt.Sprintf(`{"type":"group","messageId":"messageId","userId":"userId","groupId":"groupId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","traits":{"title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"},%s}`, intrOptsPayload))
			f.Add(destType, `{}`, fmt.Sprintf(`{"type":"page","messageId":"messageId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","properties":{"name":"Home","title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"},%s}`, intrOptsPayload))
			f.Add(destType, `{}`, fmt.Sprintf(`{"type":"track","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","event":"event","request_ip":"5.6.7.8","properties":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"},%s}`, intrOptsPayload))
			f.Add(destType, `{}`, fmt.Sprintf(`{"type":"IDENTIFY","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","traits":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"},%s}`, intrOptsPayload))
			f.Add(destType, `{}`, fmt.Sprintf(`{"type":"EXTRACT","recordId":"recordID","event":"users","receivedAt":"2021-09-01T00:00:00.000Z","properties":{"name":"Home","title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"},%s}`, intrOptsPayload))
		}

		f.Log("Providing seed corpus for destination configurations options for destination type: ", destType)
		destConfigOpts := []string{
			`{"skipTracksTable":true}`,
			`{"skipTracksTable":false}`,
			`{"skipUsersTable":true}`,
			`{"skipUsersTable":false}`,
			`{"underscoreDivideNumbers":true}`,
			`{"underscoreDivideNumbers":false}`,
			`{"allowUsersContextTraits":true}`,
			`{"allowUsersContextTraits":false}`,
			`{"jsonPaths":"context,properties,userProperties,context.traits"}`,
			`{"jsonPaths":"track.context,track.properties,track.userProperties,track.context.traits"}`,
			`{"jsonPaths":"properties,context.traits"}`,
			`{"jsonPaths":"page.properties,page.context.traits"}`,
			`{"jsonPaths":"screen.properties,screen.context.traits"}`,
			`{"jsonPaths":"alias.properties,alias.context.traits"}`,
			`{"jsonPaths":"group.properties,group.context.traits"}`,
			`{"jsonPaths":"extract.properties,extract.context.traits"}`,
			`{"jsonPaths":"identify.traits,identify.context.traits,identify.userProperties"}`,
			`{"skipTracksTable":true,"underscoreDivideNumbers":true,"allowUsersContextTraits":true,"jsonPaths":"context,properties,userProperties,context.traits"}`,
			`{"skipTracksTable":false,"underscoreDivideNumbers":false,"allowUsersContextTraits":false,"jsonPaths":"track.context,track.properties,track.userProperties,track.context.traits"}`,
			`{"skipUsersTable":true,"underscoreDivideNumbers":true,"allowUsersContextTraits":true,"jsonPaths":"properties,context.traits"}`,
			`{"skipUsersTable":false,"underscoreDivideNumbers":false,"allowUsersContextTraits":false,"jsonPaths":"page.properties,page.context.traits"}`,
			`{"skipTracksTable":true,"underscoreDivideNumbers":true,"allowUsersContextTraits":true,"jsonPaths":"screen.properties,screen.context.traits"}`,
			`{"skipTracksTable":false,"underscoreDivideNumbers":false,"allowUsersContextTraits":false,"jsonPaths":"alias.properties,alias.context.traits"}`,
			`{"skipUsersTable":true,"underscoreDivideNumbers":true,"allowUsersContextTraits":true,"jsonPaths":"group.properties,group.context.traits"}`,
			`{"skipUsersTable":false,"underscoreDivideNumbers":false,"allowUsersContextTraits":false,"jsonPaths":"extract.properties,extract.context.traits"}`,
			`{"skipTracksTable":true,"underscoreDivideNumbers":true,"allowUsersContextTraits":true,"jsonPaths":"identify.traits,identify.context.traits,identify.userProperties"}`,
		}
		for _, destConfig := range destConfigOpts {
			f.Add(destType, destConfig, `{"type":"alias","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","previousId":"previousId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","context":{"traits":{"email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`)
			f.Add(destType, destConfig, `{"type":"page","name":"Home","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`)
			f.Add(destType, destConfig, `{"type":"screen","name":"Main","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`)
			f.Add(destType, destConfig, `{"type":"group","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","groupId":"groupId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","context":{"traits":{"email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`)
			f.Add(destType, destConfig, `{"type":"merge","mergeProperties":[{"type":"email","value":"alex@example.com"},{"type":"mobile","value":"+1-202-555-0146"}]}`)
			f.Add(destType, destConfig, `{"type":"page","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","properties":{"name":"Home","title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`)
			f.Add(destType, destConfig, `{"type":"track","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","event":"event","request_ip":"5.6.7.8","userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`)
			f.Add(destType, destConfig, `{"type":"track","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","event":"event","request_ip":"5.6.7.8","properties":{"review_id":"86ac1cd43","product_id":"9578257311"},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`)
			f.Add(destType, destConfig, `{"type":"track","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","event":"event","request_ip":"5.6.7.8","properties":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"},"integrations":{"POSTGRES":{"options":{"skipTracksTable":true}}}}`)
			f.Add(destType, destConfig, `{"type":"identify","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`)
			f.Add(destType, destConfig, `{"type":"extract","recordId":"recordID","event":"event","receivedAt":"2021-09-01T00:00:00.000Z","context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`)
		}

		f.Log("Providing seed corpus for date formats for destination type: ", destType)
		dateFormats := []string{
			"2006-01-02",
			"2006-01-02T15:04:05",
			"2006-01-02T15:04:05.000",
			"2006-01-02T15:04:05.000000",
			"2006-01-02T15:04:05.000000000",
			"2006-01-02T15:04:05Z",
			"2006-01-02T15:04:05.000Z",
			"2006-01-02T15:04:05.000000Z",
			"2006-01-02T15:04:05.000000000Z",
			"2006-01-02T15:04:05+07:00",
			"2006-01-02T15:04:05.000+07:00",
			"2006-01-02T15:04:05-03:00",
			"2006-01-02 15:04:05",
			"2006-01-02 15:04:05.000",
			"2006-W01-2",
			"2006-001",
			"2006-001T15:04:05Z",
			"2006-01-02T15:04:05.123456789Z",
			"2006-01-02T15:04:05.0000000000Z",
			"2006-01-02T24:00:00Z",
			"2023-02-29",
			"2020-02-29",
		}
		for _, dateFormat := range dateFormats {
			f.Add(destType, `{}`, `{"type":"alias","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","previousId":"previousId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"`+dateFormat+`","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","context":{"traits":{"email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`)
			f.Add(destType, `{}`, `{"type":"page","messageId":"messageId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"`+dateFormat+`","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","properties":{"name":"Home","title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`)
			f.Add(destType, `{}`, `{"type":"screen","messageId":"messageId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"`+dateFormat+`","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","properties":{"name":"Main","title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`)
			f.Add(destType, `{}`, `{"type":"group","messageId":"messageId","userId":"userId","groupId":"groupId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"`+dateFormat+`","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","traits":{"title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`)
			f.Add(destType, `{}`, `{"type":"track","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"`+dateFormat+`","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","event":"event","request_ip":"5.6.7.8","properties":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"},"integrations":{"POSTGRES":{"options":{"skipTracksTable":true}}}}`)
			f.Add(destType, `{}`, `{"type":"identify","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"`+dateFormat+`","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","traits":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"},"integrations":{"POSTGRES":{"options":{"skipUsersTable":true}}}}`)
			f.Add(destType, `{}`, `{"type":"extract","recordId":"recordID","event":"users","receivedAt":"`+dateFormat+`","channel":"web","properties":{"name":"Home","title":"Home | RudderStack","url":"https://www.rudderstack.com"},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`)
		}
	}

	f.Fuzz(func(t *testing.T, destType, destConfigJSON, payload string) {
		if _, exist := whutils.WarehouseDestinationMap[destType]; !exist {
			return
		}

		var destConfig map[string]any
		err = json.Unmarshal([]byte(destConfigJSON), &destConfig)
		if err != nil {
			return
		}

		sanitizedPayload, err := sanitizePayload(payload)
		if err != nil {
			if !errors.Is(err, errInvalidDateTime) {
				return
			}

			t.Log("Destination Type: ", destType, "Destination Config: ", destConfigJSON, "Payload: ", payload)
			t.Skip()
		}

		var (
			eventType  = gjson.Get(sanitizedPayload, "type").String()
			eventName  = gjson.Get(sanitizedPayload, "event").String()
			messageID  = gjson.Get(sanitizedPayload, "messageId").String()
			receivedAt = gjson.Get(sanitizedPayload, "receivedAt").Time()
			recordID   = gjson.Get(sanitizedPayload, "recordId").Value()
		)

		if len(messageID) == 0 || receivedAt.IsZero() {
			return
		}

		sanitizedPayload, err = sjson.Set(sanitizedPayload, "receivedAt", receivedAt.Format(misc.RFC3339Milli))
		if err != nil {
			return
		}

		conf := setupConfig(transformerResource, map[string]any{})

		processorTransformer := ptrans.NewTransformer(conf, logger.NOP, stats.Default)
		warehouseTransformer := New(conf, logger.NOP, stats.NOP)

		eventContexts := []testhelper.EventContext{
			{
				Payload: []byte(sanitizedPayload),
				Metadata: ptrans.Metadata{
					EventType:       eventType,
					EventName:       eventName,
					DestinationType: destType,
					ReceivedAt:      receivedAt.Format(misc.RFC3339Milli),
					SourceID:        "sourceID",
					DestinationID:   "destinationID",
					SourceType:      "sourceType",
					MessageID:       messageID,
					RecordID:        recordID,
				},
				Destination: backendconfig.DestinationT{
					Name:   destType,
					Config: destConfig,
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						Name: destType,
					},
				},
			},
		}
		cmpEvents(t, eventContexts, processorTransformer, warehouseTransformer)
	})
}

func cmpEvents(t *testing.T, eventContexts []testhelper.EventContext, pTransformer, dTransformer ptrans.DestinationTransformer) {
	t.Helper()

	events := make([]ptrans.TransformerEvent, 0, len(eventContexts))
	for _, eventContext := range eventContexts {
		var singularEvent types.SingularEventT
		err := json.Unmarshal(eventContext.Payload, &singularEvent)
		require.NoError(t, err)

		events = append(events, ptrans.TransformerEvent{
			Message:     singularEvent,
			Metadata:    eventContext.Metadata,
			Destination: eventContext.Destination,
		})
	}

	ctx := context.Background()
	batchSize := 100

	pResponse := pTransformer.Transform(ctx, events, batchSize)
	wResponse := dTransformer.Transform(ctx, events, batchSize)

	require.Equal(t, len(wResponse.Events), len(pResponse.Events))
	require.Equal(t, len(wResponse.FailedEvents), len(pResponse.FailedEvents))

	for i := range pResponse.Events {
		require.EqualValues(t, wResponse.Events[i], pResponse.Events[i])
	}
	for i := range pResponse.FailedEvents {
		require.NotEmpty(t, pResponse.FailedEvents[i].Error)
		require.NotEmpty(t, wResponse.FailedEvents[i].Error)

		require.NotZero(t, pResponse.FailedEvents[i].StatusCode)
		require.NotZero(t, wResponse.FailedEvents[i].StatusCode)
	}
}

func sanitizePayload(input string) (string, error) {
	sanitized := strings.ReplaceAll(input, `\u0000`, "")
	if len(strings.TrimSpace(sanitized)) == 0 {
		return "{}", nil
	}

	// Checking for valid datetime formats in the payload
	// JS converts new Date('0001-01-01 00:00').toISOString() to 2001-01-01T00:00:00.000Z
	// https://www.programiz.com/online-compiler/1P7KHTw0ClE9R
	dateTimes := reDateTime.FindAllString(sanitized, -1)
	for _, dateTime := range dateTimes {
		_, err := dateparse.ParseAny(dateTime, dateparse.PreferMonthFirst(true), dateparse.RetryAmbiguousDateWithSwap(true))
		if err != nil {
			return "", fmt.Errorf("invalid datetime format for %s: %w", dateTime, errInvalidDateTime)
		}
		if !reValidDateTime.MatchString(dateTime) {
			return "", fmt.Errorf("invalid datetime format for %s: %w", dateTime, errInvalidDateTime)
		}
	}

	var result types.SingularEventT
	if err := json.Unmarshal([]byte(sanitized), &result); err != nil {
		return "", errors.New("invalid JSON format")
	}
	output, err := json.Marshal(result)
	if err != nil {
		return "", fmt.Errorf("marshalling error: %w", err)
	}
	return string(output), nil
}
