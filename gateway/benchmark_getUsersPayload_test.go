package gateway

import (
	"errors"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/mailru/easyjson"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"

	"github.com/rudderlabs/rudder-server/gateway/response"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

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
	resp2, err := getUsersPayloadFinal(validBody)
	require.NoError(t, err)
	resp3, err := getUsersPayloadEasyJsonDoubleAllocation(validBody)
	require.NoError(t, err)
	resp4, err := getUsersPayloadWithMD5Cache(validBody)
	require.NoError(t, err)

	if !reflect.DeepEqual(resp1, resp2) {
		t.Fatalf("Expected: %s\n\nGot: %s", convertBytesMap(resp1), convertBytesMap(resp2))
	}
	if !reflect.DeepEqual(resp1, resp3) {
		t.Fatalf("Expected: %s\n\nGot: %s", convertBytesMap(resp1), convertBytesMap(resp3))
	}
	if !reflect.DeepEqual(resp1, resp4) {
		t.Fatalf("Expected: %s\n\nGot: %s", convertBytesMap(resp1), convertBytesMap(resp4))
	}
}

func BenchmarkGetUsersPayload(b *testing.B) {
	validBody, err := os.ReadFile("./testdata/small_output.json")
	require.NoError(b, err)

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

	b.Run("easyjson-final", func(b *testing.B) {
		var (
			err error
		)

		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			b.StartTimer()
			_, err = getUsersPayloadFinal(validBody)
			b.StopTimer()
		}

		// check at least once that we got no errors
		require.NoError(b, err)
	})
}

func BenchmarkFindReasonablePayload(b *testing.B) {
loop:
	for no := 10000; ; no += 10000 {
		done := make(chan struct{})
		timeout := time.After(3 * time.Minute)

		var start time.Time
		go func(no int) {
			defer close(done)
			requestPayload := generatePayload(no)
			b.Logf("Processing %s payload", byteCountIEC(len(requestPayload)))

			start = time.Now()
			b.StartTimer()
			_, err := getUsersPayloadFinal(requestPayload)
			b.StopTimer()
			require.NoError(b, err)
		}(no)
		select {
		case <-done:
			b.Logf("getUsersPayloadFinal took %s", time.Since(start))
		case <-timeout:
			requestPayload := generatePayload(no)
			b.Logf("Payload of %s took more than 3 minutes to process", byteCountIEC(len(requestPayload)))
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

func getUsersPayloadFinal(requestPayload []byte) (map[string][]byte, error) {
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

func convertBytesMap(m map[string][]byte) map[string]string {
	nm := make(map[string]string)
	for k, v := range m {
		nm[k] = string(v)
	}
	return nm
}
