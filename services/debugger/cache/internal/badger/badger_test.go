package badger

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
		e := Cache[[]byte]{Origin: "test", Logger: logger.NewLogger()}

		BeforeAll(func() {
			misc.Init()
			GinkgoT().Setenv("RSERVER_LIVE_EVENT_CACHE_CLEAR_FREQ", "1")
			GinkgoT().Setenv("RSERVER_LIVE_EVENT_CACHE_GCTIME", "1s")
			e.Init()
		})

		AfterAll(func() {
			_ = e.Stop()
		})

		It("Cache Init", func() {
			Expect(e.db).NotTo(BeNil())
			Expect(e.cleanupFreq).NotTo(Equal(0))
		})

		It("Cache update", func() {
			Expect(e.Update(testKey, testValue1)).To(BeNil())
			val, err := e.Read(testKey)
			Expect(len(val)).To(Equal(1))
			Expect(err).NotTo(BeNil())
			Expect(val[0]).To(Equal(testValue1))
			Eventually(func() int {
				val, err := e.Read(testKey)
				Expect(err).ToNot(BeNil())
				return len(val)
			}, 6*time.Second).Should(Equal(0))
		})

		It("Cache readAndPopData", func() {
			Expect(e.Update(testKey, testValue1)).To(BeNil())
			Expect(e.Update(testKey, testValue2)).To(BeNil())
			v, err := e.Read(testKey)
			Expect(len(v)).To(Equal(2))
			Expect(err).NotTo(BeNil())
			assert.ElementsMatch(GinkgoT(), v, [][]byte{testValue1, testValue2})
			Eventually(func() int {
				val, err := e.Read(testKey)
				Expect(err).ToNot(BeNil())
				return len(val)
			}, 6*time.Second).Should(Equal(0))
		})
	})
})
