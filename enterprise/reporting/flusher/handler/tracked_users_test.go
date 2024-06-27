package handler

import (
	"encoding/hex"
	"encoding/json"
	"math/rand"
	"testing"
	"time"

	"github.com/segmentio/go-hll"
	"github.com/stretchr/testify/assert"

	"github.com/rudderlabs/rudder-server/enterprise/reporting/flusher/report"
)

func hllSettings() hll.Settings {
	return hll.Settings{
		Log2m:             14,
		Regwidth:          5,
		ExplicitThreshold: hll.AutoExplicitThreshold,
		SparseEnabled:     true,
	}
}

func addDataToHLL(hllData *hll.Hll, min, max, count int) {
	for i := 0; i < count; i++ {
		num := rand.Intn(max-min+1) + min
		hllData.AddRaw(uint64(num))
	}
}

func TestMarshalJSON(t *testing.T) {
	hllData, _ := hll.NewHll(hllSettings())

	hllData.AddRaw(1)

	report := &TrackedUsersReport{
		ReportedAt:               time.Now().UTC(),
		WorkspaceID:              "workspace1",
		SourceID:                 "source1",
		InstanceID:               "instance1",
		UserIDHLL:                hllData,
		AnonymousIDHLL:           hllData,
		IdentifiedAnonymousIDHLL: hllData,
	}

	t.Run("successful marshal", func(t *testing.T) {
		data, err := report.MarshalJSON()
		assert.NoError(t, err)
		assert.NotEmpty(t, data)

		var result map[string]interface{}
		err = json.Unmarshal(data, &result)
		assert.NoError(t, err)

		assert.Equal(t, hex.EncodeToString(hllData.ToBytes()), result["userIdHLL"])
		assert.Equal(t, hex.EncodeToString(hllData.ToBytes()), result["anonymousIdHLL"])
		assert.Equal(t, hex.EncodeToString(hllData.ToBytes()), result["identifiedAnonymousIdHLL"])
	})
}

func TestHandlerAggregate(t *testing.T) {
	userIdHLL1, _ := hll.NewHll(hllSettings())
	anonymousIdHLL1, _ := hll.NewHll(hllSettings())
	identifiedAnonymousIdHLL1, _ := hll.NewHll(hllSettings())

	userIdHLL2, _ := hll.NewHll(hllSettings())
	anonymousIdHLL2, _ := hll.NewHll(hllSettings())
	identifiedAnonymousIdHLL2, _ := hll.NewHll(hllSettings())

	addDataToHLL(&userIdHLL1, 1, 1000000, 100000)
	addDataToHLL(&userIdHLL2, 1, 1000000, 1000)
	addDataToHLL(&userIdHLL2, 1000000, 2000000, 100)
	userIdCount := userIdHLL1.Cardinality()

	addDataToHLL(&anonymousIdHLL1, 1, 10000, 1000)
	addDataToHLL(&anonymousIdHLL2, 1, 10000, 10)
	addDataToHLL(&anonymousIdHLL2, 1000000, 2000000, 100)
	anonymousIdCount := anonymousIdHLL1.Cardinality()

	addDataToHLL(&identifiedAnonymousIdHLL1, 1, 100000, 10000)
	addDataToHLL(&identifiedAnonymousIdHLL1, 1, 10000, 100)
	addDataToHLL(&identifiedAnonymousIdHLL1, 1000000, 2000000, 100)
	identifiedAnonymousIdCount := identifiedAnonymousIdHLL1.Cardinality()

	report1 := &TrackedUsersReport{
		UserIDHLL:                userIdHLL1,
		AnonymousIDHLL:           anonymousIdHLL1,
		IdentifiedAnonymousIDHLL: identifiedAnonymousIdHLL1,
	}

	report2 := &TrackedUsersReport{
		UserIDHLL:                userIdHLL2,
		AnonymousIDHLL:           anonymousIdHLL2,
		IdentifiedAnonymousIDHLL: identifiedAnonymousIdHLL2,
	}

	handler := &TrackedUsersHandler{}

	t.Run("successful union of hlls", func(t *testing.T) {
		err := handler.Aggregate(report1, report2)
		assert.NoError(t, err)
		assert.GreaterOrEqual(t, report1.UserIDHLL.Cardinality(), userIdCount)
		assert.GreaterOrEqual(t, report1.AnonymousIDHLL.Cardinality(), anonymousIdCount)
		assert.GreaterOrEqual(t, report1.IdentifiedAnonymousIDHLL.Cardinality(), identifiedAnonymousIdCount)
	})
}

func TestDecodeHLL(t *testing.T) {
	hllData, _ := hll.NewHll(hllSettings())
	addDataToHLL(&hllData, 1, 1000000, 100000)
	hllCardinality := hllData.Cardinality()
	hexString := hex.EncodeToString(hllData.ToBytes())

	handler := &TrackedUsersHandler{}

	t.Run("successful decode", func(t *testing.T) {
		decoded, err := handler.decodeHLL(hexString)
		assert.NoError(t, err)
		assert.NotNil(t, decoded)
		assert.Equal(t, hllCardinality, decoded.Cardinality())
	})

	t.Run("invalid hex string", func(t *testing.T) {
		_, err := handler.decodeHLL("invalid_hex")
		assert.Error(t, err)
	})
}

func TestDecode(t *testing.T) {
	handler := &TrackedUsersHandler{}
	userIdHLL, _ := hll.NewHll(hllSettings())
	anonymousIdHLL, _ := hll.NewHll(hllSettings())
	identifiedAnonymousIdHLL, _ := hll.NewHll(hllSettings())

	addDataToHLL(&userIdHLL, 1, 1000000, 100000)
	addDataToHLL(&anonymousIdHLL, 1, 10000, 100)
	addDataToHLL(&identifiedAnonymousIdHLL, 1, 100000, 10000)

	userIdCount := userIdHLL.Cardinality()
	anonymousIdCount := anonymousIdHLL.Cardinality()
	identifiedAnonymousIdCount := identifiedAnonymousIdHLL.Cardinality()

	rawReport := report.RawReport{
		"reported_at":                time.Now(),
		"workspace_id":               "workspace1",
		"source_id":                  "source1",
		"instance_id":                "instance1",
		"userid_hll":                 hex.EncodeToString(userIdHLL.ToBytes()),
		"anonymousid_hll":            hex.EncodeToString(anonymousIdHLL.ToBytes()),
		"identified_anonymousid_hll": hex.EncodeToString(identifiedAnonymousIdHLL.ToBytes()),
	}

	t.Run("successful decode", func(t *testing.T) {
		decodedReport, err := handler.Decode(rawReport)
		assert.NoError(t, err)
		assert.NotNil(t, decodedReport)

		tuReport := decodedReport.(*TrackedUsersReport)
		assert.Equal(t, userIdCount, tuReport.UserIDHLL.Cardinality())
		assert.Equal(t, anonymousIdCount, tuReport.AnonymousIDHLL.Cardinality())
		assert.Equal(t, identifiedAnonymousIdCount, tuReport.IdentifiedAnonymousIDHLL.Cardinality())
	})

	t.Run("invalid userIDHLL", func(t *testing.T) {
		invalidRawReport := rawReport
		invalidRawReport["userid_hll"] = "invalid_hex"
		_, err := handler.Decode(invalidRawReport)
		assert.Error(t, err)
	})
}
