package embeddedcache

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/stretchr/testify/assert"
)

var _ = Describe("cache", Ordered, func() {
	Context("cache testing", func() {
		testKey := "test_key"
		testValue1 := []byte("test_value1")
		testValue2 := []byte("test_value2")
		e := EmbeddedCache[[]byte]{Origin: "test", Logger: logger.NewLogger()}

		BeforeAll(func() {
			misc.Init()
			GinkgoT().Setenv("RSERVER_LIVE_EVENT_CACHE_CLEAR_FREQ", "1")
			e.init()
		})

		AfterAll(func() {
			_ = e.Stop()
		})

		It("Cache init", func() {
			Expect(e.db).NotTo(BeNil())
			Expect(e.cleanupFreq).NotTo(Equal(0))
		})

		It("Cache update", func() {
			e.Update(testKey, testValue1)
			Expect(len(e.Read(testKey))).To(Equal(1))
			Expect(e.Read(testKey)[0]).To(Equal(testValue1))
			Eventually(func() int { return len(e.Read(testKey)) }, 6*time.Second).Should(Equal(0))
		})

		It("Cache readAndPopData", func() {
			e.Update(testKey, testValue1)
			e.Update(testKey, testValue2)
			v := e.Read(testKey)
			Expect(len(v)).To(Equal(2))
			assert.ElementsMatch(GinkgoT(), v, [][]byte{testValue1, testValue2})
			Eventually(func() int { return len(e.Read(testKey)) }, 6*time.Second).Should(Equal(0))
		})
	})
})
