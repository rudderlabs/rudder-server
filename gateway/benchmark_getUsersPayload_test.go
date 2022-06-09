package gateway

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/mailru/easyjson"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/phayes/freeport"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"github.com/valyala/fastjson"

	"github.com/rudderlabs/rudder-server/gateway/response"
	mocksApp "github.com/rudderlabs/rudder-server/mocks/app"
	mocksTypes "github.com/rudderlabs/rudder-server/mocks/utils/types"
	"github.com/rudderlabs/rudder-server/services/rsources"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

//easyjson:json
type batch struct {
	Entries []batchEntry `json:"batch"`
}

//easyjson:json
type batchEntry struct {
	UserID      string `json:"userId"`
	AnonymousID string `json:"anonymousId"`
}

func TestRegressions(t *testing.T) {
	var validBody = []byte(`{"batch": [{
			"anonymousId": "anon_id_1",
			"event": "event_1_1"
		}, {
			"anonymousId": "anon_id_2",
			"event": "event_2_1"
		}, {
			"anonymousId": "anon_id_3",
			"event": "event_3_1"
		}, {
			"anonymousId": "anon_id_1",
			"event": "event_1_2"
		}, {
			"anonymousId": "anon_id_2",
			"event": "event_2_2"
		}, {
			"anonymousId": "anon_id_1",
			"event": "event_1_3"
		}]
	}`)

	resp1, err := getUsersPayloadOriginal(validBody)
	require.NoError(t, err)
	respObj1 := convertResultToMapInterface(t, resp1)

	resp2, err := getUsersPayloadFastJson(validBody)
	require.NoError(t, err)
	respObj2 := convertResultToMapInterface(t, resp2)

	resp3, err := getUsersPayloadEasyJsonDoubleAllocation(validBody)
	require.NoError(t, err)
	respObj3 := convertResultToMapInterface(t, resp3)

	resp4, err := getUsersPayloadWithMD5Cache(validBody)
	require.NoError(t, err)
	respObj4 := convertResultToMapInterface(t, resp4)

	resp5, err := getUsersPayloadEasyGJsonHybrid(validBody)
	require.NoError(t, err)
	respObj5 := convertResultToMapInterface(t, resp5)

	resp6, err := getUsersPayloadGJsonRevised(validBody)
	require.NoError(t, err)
	respObj6 := convertResultToMapInterface(t, resp6)

	if !reflect.DeepEqual(respObj1, respObj2) {
		t.Fatalf("Expected: %s\n\nGot: %s", respObj1, respObj2)
	}
	if !reflect.DeepEqual(respObj1, respObj3) {
		t.Fatalf("Expected: %s\n\nGot: %s", respObj1, respObj3)
	}
	if !reflect.DeepEqual(respObj1, respObj4) {
		t.Fatalf("Expected: %s\n\nGot: %s", respObj1, respObj4)
	}
	if !reflect.DeepEqual(respObj1, respObj5) {
		t.Fatalf("Expected: %s\n\nGot: %s", respObj1, respObj5)
	}
	if !reflect.DeepEqual(respObj1, respObj6) {
		t.Fatalf("Expected: %s\n\nGot: %s", respObj1, respObj6)
	}
}

var _ = Describe("Gateway import", func() {
	initGW()

	var c *testContext

	BeforeEach(func() {
		c = &testContext{}
		c.Setup()

		c.mockSuppressUser = mocksTypes.NewMockSuppressUserI(c.mockCtrl)
		c.mockSuppressUserFeature = mocksApp.NewMockSuppressUserFeature(c.mockCtrl)
		c.initializeEnterprizeAppFeatures()

		c.mockSuppressUserFeature.EXPECT().Setup(gomock.Any()).AnyTimes().Return(c.mockSuppressUser)
		c.mockSuppressUser.EXPECT().IsSuppressedUser(gomock.Any(), gomock.Any(), gomock.Any()).Return(true).AnyTimes()

		// setup static requirements of dependencies
		stats.Setup()

		// setup common environment, override in BeforeEach when required
		SetEnableRateLimit(false)
		SetEnableSuppressUserFeature(true)
		SetEnableEventSchemasFeature(false)
	})

	AfterEach(func() {
		c.Finish()
	})

	Context("Import benchmark", func() {
		gateway := &HandleT{}

		BeforeEach(func() {
			gateway.Setup(c.mockApp, c.mockBackendConfig, c.mockJobsDB, nil, c.mockVersionHandler, rsources.NewNoOpService())
		})

		It("should perform reasonably", func() { // run this with -memprofile mem.out
			port, err := freeport.GetFreePort()
			Expect(err).To(BeNil())

			done := make(chan struct{})
			srv := http.Server{
				Addr: ":" + strconv.Itoa(port),
				Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					gateway.webRequestHandler(gateway.irh, w, r, "import")
					close(done)
				}),
			}
			go func() {
				_ = srv.ListenAndServe()
			}()

			suppressedUserEventData := fmt.Sprintf(`{"batch":[{"userId":"%s"}]}`, SuppressedUserID)
			parsedURL, err := url.Parse(fmt.Sprintf("http://localhost:%d", port))
			Expect(err).To(BeNil())

			fileContent, err := ioutil.ReadFile("./testdata/output.json")
			Expect(err).To(BeNil())

			req := authorizedRequest(WriteKeyEnabled, bytes.NewBufferString(suppressedUserEventData))
			req.ContentLength = int64(len(fileContent))
			req.Method = "POST"
			req.Body = &nopCloser{Buffer: bytes.NewBuffer(fileContent)}
			req.URL = parsedURL

			resp, err := http.DefaultClient.Do(req)
			Expect(err).To(BeNil())

			respBody, err := ioutil.ReadAll(resp.Body)
			Expect(err).To(BeNil())
			Expect(resp.StatusCode).To(BeIdenticalTo(http.StatusOK))

			fmt.Println(string(respBody), resp.StatusCode)

			<-done
		})
	})
})

func BenchmarkGetUsersPayload(b *testing.B) {
	validBody := generatePayload(20000)
	b.Logf("Running benchmark with %s payload", byteCountIEC(len(validBody)))

	b.Run("original", func(b *testing.B) {
		var (
			err error
		)

		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			b.StartTimer()
			_, err = getUsersPayloadOriginal(validBody)
			b.StopTimer()
		}

		// check at least once that we got no errors
		require.NoError(b, err)
	})

	b.Run("easyjson-double-alloc", func(b *testing.B) {
		var (
			err error
		)

		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			b.StartTimer()
			_, err = getUsersPayloadEasyJsonDoubleAllocation(validBody)
			b.StopTimer()
		}

		// check at least once that we got no errors
		require.NoError(b, err)
	})

	b.Run("easyjson-single-alloc-md5-cache", func(b *testing.B) {
		var (
			err error
		)

		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			b.StartTimer()
			_, err = getUsersPayloadWithMD5Cache(validBody)
			b.StopTimer()
		}

		// check at least once that we got no errors
		require.NoError(b, err)
	})

	b.Run("easyjson-no-md5-cache", func(b *testing.B) {
		var (
			err error
		)

		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			b.StartTimer()
			_, err = getUsersPayloadEasyGJsonHybrid(validBody)
			b.StopTimer()
		}

		// check at least once that we got no errors
		require.NoError(b, err)
	})

	b.Run("fastjson", func(b *testing.B) {
		var (
			err error
		)

		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			b.StartTimer()
			_, err = getUsersPayloadFastJson(validBody)
			b.StopTimer()
		}

		// check at least once that we got no errors
		require.NoError(b, err)
	})

	b.Run("gjson-revised", func(b *testing.B) {
		var (
			err error
		)

		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			b.StartTimer()
			_, err = getUsersPayloadGJsonRevised(validBody)
			b.StopTimer()
		}

		// check at least once that we got no errors
		require.NoError(b, err)
	})
}

func BenchmarkFindReasonablePayload(b *testing.B) {
	maxTime := 2 * time.Second

loop:
	for no := 100000; ; no += 100000 {
		done := make(chan struct{})
		timeout := time.After(maxTime)

		var start time.Time
		go func(no int) {
			defer close(done)
			requestPayload := generatePayload(no)
			b.Logf("Processing %s payload", byteCountIEC(len(requestPayload)))

			start = time.Now()
			b.StartTimer()
			_, err := getUsersPayloadGJsonRevised(requestPayload)
			b.StopTimer()
			require.NoError(b, err)
		}(no)
		select {
		case <-done:
			b.Logf("Processing took %s", time.Since(start))
		case <-timeout:
			requestPayload := generatePayload(no)
			b.Logf("Payload of %s took more than %s to process", byteCountIEC(len(requestPayload)), maxTime)
			break loop
		}
	}
}

func getUsersPayloadOriginal(requestPayload []byte) (map[string][]byte, error) {
	userMap := make(map[string][][]byte)
	var index int

	if !gjson.ValidBytes(requestPayload) {
		return make(map[string][]byte), errors.New(response.InvalidJSON)
	}

	result := gjson.GetBytes(requestPayload, "batch")

	result.ForEach(func(_, _ gjson.Result) bool {
		anonIDFromReq := strings.TrimSpace(gjson.GetBytes(requestPayload, fmt.Sprintf(`batch.%v.anonymousId`, index)).String())
		userIDFromReq := strings.TrimSpace(gjson.GetBytes(requestPayload, fmt.Sprintf(`batch.%v.userId`, index)).String())
		rudderID, err := misc.GetMD5UUID(userIDFromReq + ":" + anonIDFromReq)
		if err != nil {
			return false
		}
		userMap[rudderID.String()] = append(userMap[rudderID.String()], []byte(gjson.GetBytes(requestPayload, fmt.Sprintf(`batch.%v`, index)).String()))
		index++
		return true
	})
	recontructedUserMap := make(map[string][]byte)
	for key := range userMap {
		var tempValue string
		var err error
		for index = 0; index < len(userMap[key]); index++ {
			tempValue, err = sjson.SetRaw(tempValue, fmt.Sprintf("batch.%v", index), string(userMap[key][index]))
			if err != nil {
				return recontructedUserMap, err
			}
		}
		recontructedUserMap[key] = []byte(tempValue)
	}
	return recontructedUserMap, nil
}

func getUsersPayloadEasyJsonDoubleAllocation(requestPayload []byte) (map[string][]byte, error) {
	var b batch
	err := easyjson.Unmarshal(requestPayload, &b)
	if err != nil {
		return nil, errors.New(response.InvalidJSON)
	}

	var (
		userMap = make(map[string][][]byte)
	)

	for index, row := range b.Entries {
		rudderID, err := misc.GetMD5UUID(row.UserID + ":" + row.AnonymousID)
		if err != nil {
			continue
		}
		userMap[rudderID.String()] = append(
			userMap[rudderID.String()],
			[]byte(gjson.GetBytes(requestPayload, fmt.Sprintf(`batch.%v`, index)).String()),
		)
	}

	reconstructedUserMap := make(map[string][]byte)
	for key := range userMap {
		var tempValue string
		var err error
		for index := 0; index < len(userMap[key]); index++ {
			tempValue, err = sjson.SetRaw(tempValue, fmt.Sprintf("batch.%v", index), string(userMap[key][index]))
			if err != nil {
				return reconstructedUserMap, err
			}
		}
		reconstructedUserMap[key] = []byte(tempValue)
	}
	return reconstructedUserMap, nil
}

func getUsersPayloadWithMD5Cache(requestPayload []byte) (map[string][]byte, error) {
	var b batch
	err := easyjson.Unmarshal(requestPayload, &b)
	if err != nil {
		return nil, errors.New(response.InvalidJSON)
	}

	var (
		userCnt = make(map[string]int)
		userMap = make(map[string][]byte)
		uuids   = make(map[string]string)
	)

	for index, row := range b.Entries {
		uuidKey := row.UserID + ":" + row.AnonymousID
		uuidStr, ok := uuids[uuidKey]
		if !ok {
			rudderID, err := misc.GetMD5UUID(uuidKey)
			if err != nil {
				continue
			}
			uuidStr = rudderID.String()
			uuids[uuidKey] = uuidStr
		}

		globalPath := "batch." + strconv.Itoa(index)
		tempValue, ok := userMap[uuidStr]
		if !ok {
			userCnt[uuidStr] = 0
			userMap[uuidStr] = []byte(`{"batch":[` + gjson.GetBytes(requestPayload, globalPath).String() + `]}`)
		} else {
			path := "batch." + strconv.Itoa(userCnt[uuidStr]+1)
			raw, err := sjson.SetRaw(string(tempValue), path, gjson.GetBytes(requestPayload, globalPath).String())
			if err != nil {
				continue
			}
			userCnt[uuidStr]++
			userMap[uuidStr] = []byte(raw)
		}
	}

	return userMap, nil
}

func getUsersPayloadEasyGJsonHybrid(requestPayload []byte) (map[string][]byte, error) {
	var b batch
	err := easyjson.Unmarshal(requestPayload, &b)
	if err != nil {
		return nil, errors.New(response.InvalidJSON)
	}

	var (
		userCnt = make(map[string]int)
		userMap = make(map[string][]byte)
	)

	for index, row := range b.Entries {
		rudderID, err := misc.GetMD5UUID(row.UserID + ":" + row.AnonymousID)
		if err != nil {
			continue
		}
		uuid := rudderID.String()
		globalPath := "batch." + strconv.Itoa(index)
		tempValue, ok := userMap[uuid]
		if !ok {
			userCnt[uuid] = 0
			userMap[uuid] = []byte(`{"batch":[` + gjson.GetBytes(requestPayload, globalPath).String() + `]}`)
		} else {
			path := "batch." + strconv.Itoa(userCnt[uuid]+1)
			raw, err := sjson.SetRaw(string(tempValue), path, gjson.GetBytes(requestPayload, globalPath).String())
			if err != nil {
				continue
			}
			userCnt[uuid]++
			userMap[uuid] = []byte(raw)
		}
	}

	return userMap, nil
}

func getUsersPayloadGJsonRevised(requestPayload []byte) (map[string][]byte, error) {
	if !gjson.ValidBytes(requestPayload) {
		return make(map[string][]byte), errors.New(response.InvalidJSON)
	}

	result := gjson.GetBytes(requestPayload, "batch")

	var (
		userCnt = make(map[string]int)
		userMap = make(map[string][]byte)
	)
	result.ForEach(func(_, value gjson.Result) bool {
		anonIDFromReq := value.Get("anonymousId").String()
		userIDFromReq := value.Get("userId").String()
		rudderID, err := misc.GetMD5UUID(userIDFromReq + ":" + anonIDFromReq)
		if err != nil {
			return false
		}

		uuidStr := rudderID.String()
		tempValue, ok := userMap[uuidStr]
		if !ok {
			userCnt[uuidStr] = 0
			userMap[uuidStr] = append([]byte(`{"batch":[`), append([]byte(value.Raw), ']', '}')...)
		} else {
			path := "batch." + strconv.Itoa(userCnt[uuidStr]+1)
			raw, err := sjson.SetRaw(string(tempValue), path, value.Raw)
			if err != nil {
				return false
			}
			userCnt[uuidStr]++
			userMap[uuidStr] = []byte(raw)
		}

		return true
	})
	return userMap, nil
}

func getUsersPayloadFastJson(requestPayload []byte) (map[string][]byte, error) {
	var p fastjson.Parser
	v, err := p.ParseBytes(requestPayload)
	if err != nil {
		return nil, errors.New(response.InvalidJSON)
	}
	batch := v.Get("batch")
	if batch == nil {
		return nil, errors.New(response.InvalidJSON)
	}
	events, err := batch.Array()
	if err != nil {
		return nil, errors.New(response.InvalidJSON)
	}

	var (
		userCnt = make(map[string]int)
		userMap = make(map[string][]byte)
	)

	for _, evt := range events {
		userID := evt.Get("userId")
		anonymousID := evt.Get("anonymousId")
		var userIDStr, anonymousIDStr string
		if userID != nil {
			userIDStr = string(userID.GetStringBytes())
		}
		if anonymousID != nil {
			anonymousIDStr = string(anonymousID.GetStringBytes())
		}
		rudderID, err := misc.GetMD5UUID(userIDStr + ":" + anonymousIDStr)
		if err != nil {
			continue
		}

		uuidStr := rudderID.String()
		tempValue, ok := userMap[uuidStr]
		if !ok {
			userCnt[uuidStr] = 0
			userMap[uuidStr] = append([]byte(`{"batch":[`), append(evt.MarshalTo(nil), ']', '}')...)
		} else {
			path := "batch." + strconv.Itoa(userCnt[uuidStr]+1)
			raw, err := sjson.SetRaw(string(tempValue), path, string(evt.MarshalTo(nil)))
			if err != nil {
				continue
			}
			userCnt[uuidStr]++
			userMap[uuidStr] = []byte(raw)
		}
	}

	return userMap, nil
}

func generatePayload(noOfEvents int) []byte {
	m := []byte(`{"batch":[`)
	for i := 0; i < noOfEvents; i++ {
		idx := strconv.Itoa(i)
		m = append(m, []byte(`{"userId":"user_id_`+idx+`",`+
			`"anonymousId":"anon_id_`+idx+`",`+
			`"event":"event_`+idx+`"},`)...)
	}
	return append(m[:len(m)-1], ']', '}')
}

func byteCountIEC(b int) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := unit, 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(b)/float64(div), "KMGTPE"[exp])
}

func convertResultToMapInterface(t *testing.T, res map[string][]byte) map[string]interface{} {
	t.Helper()
	rm := make(map[string]interface{})
	for k, v := range res {
		m := make(map[string]interface{})
		err := json.Unmarshal(v, &m)
		require.NoError(t, err)
		rm[k] = m
	}
	return rm
}

type nopCloser struct {
	*bytes.Buffer
}

func (rc *nopCloser) Close() error { return nil }
