package badger

import (
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/assert"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	svcMetric "github.com/rudderlabs/rudder-go-kit/stats/metric"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

var _ = Describe("cache", func() {
	Context("cache testing", func() {
		testKey := "test_key"
		testValue1 := []byte("test_value1")
		testValue2 := []byte("test_value2")
		testValue3 := []byte("test_value3")
		testValue4 := []byte("test_value4")
		var e *Cache[[]byte]
		var err error

		BeforeEach(func() {
			misc.Init()
			config.Set("LiveEvent.cache.ttl", "1s")
			s := stats.NewStats(config.Default, logger.Default, svcMetric.Instance)
			e, err = New[[]byte]("test", logger.NewLogger(), s)
			Expect(err).To(BeNil())
		})

		AfterEach(func() {
			config.Reset()
			_ = e.Stop()
		})

		It("Cache Init", func() {
			Expect(e.db).NotTo(BeNil())
			Expect(e.ttl).NotTo(Equal(0))
		})

		It("Cache update", func() {
			Expect(e.Update(testKey, testValue1)).To(BeNil())

			val, err := e.Read(testKey)
			Expect(len(val)).To(Equal(1))
			Expect(err).To(BeNil())
			Expect(val).To(Equal([][]byte{testValue1}))

			val, err = e.Read(testKey)
			Expect(err).To(BeNil())
			Expect(len(val)).To(Equal(0))
		})

		It("Cache readAndPopData", func() {
			Expect(e.Update(testKey, testValue1)).To(BeNil())
			Expect(e.Update(testKey, testValue2)).To(BeNil())
			Expect(e.Update(testKey, testValue3)).To(BeNil())
			Expect(e.Update(testKey, testValue4)).To(BeNil())
			v, err := e.Read(testKey)
			Expect(err).To(BeNil())
			Expect(len(v)).To(Equal(4))
			Expect(v).To(Equal([][]byte{testValue1, testValue2, testValue3, testValue4}))

			v, err = e.Read(testKey)
			Expect(err).To(BeNil())
			Expect(len(v)).To(Equal(0))
		})

		It("Cache read when multiple keys exist", func() {
			testKey2 := testKey + "_2"
			Expect(e.Update(testKey2, testValue1)).To(BeNil())
			Expect(e.Update(testKey, testValue1)).To(BeNil())

			Expect(e.Update(testKey2, testValue2)).To(BeNil())
			Expect(e.Update(testKey, testValue2)).To(BeNil())

			Expect(e.Update(testKey2, testValue3)).To(BeNil())
			Expect(e.Update(testKey, testValue3)).To(BeNil())

			Expect(e.Update(testKey2, testValue4)).To(BeNil())
			Expect(e.Update(testKey, testValue4)).To(BeNil())

			Expect(e.Update(testKey2, testValue1)).To(BeNil())

			v, err := e.Read(testKey)
			Expect(err).To(BeNil())
			Expect(len(v)).To(Equal(4))
			Expect(v).To(Equal([][]byte{testValue1, testValue2, testValue3, testValue4}))

			v, err = e.Read(testKey)
			Expect(err).To(BeNil())
			Expect(len(v)).To(Equal(0))

			v, err = e.Read(testKey2)
			Expect(err).To(BeNil())
			Expect(len(v)).To(Equal(5))
			Expect(v).To(Equal([][]byte{testValue1, testValue2, testValue3, testValue4, testValue1}))

			v, err = e.Read(testKey2)
			Expect(err).To(BeNil())
			Expect(len(v)).To(Equal(0))
		})

		It("should only keep the last x values for the same key", func() {
			var values [][]byte
			for i := 0; i <= e.limiter.Load(); i++ {
				value := []byte(fmt.Sprintf("test_value_%d", i))
				Expect(e.Update(testKey, value)).To(BeNil())
				values = append(values, value)
			}

			v, err := e.Read(testKey)
			Expect(err).To(BeNil())
			Expect(len(v)).To(Equal(e.limiter.Load()))

			Expect(v).To(Equal(values[1:]))
		})

		It("Cache expiry", func() {
			Expect(e.Update(testKey, testValue1)).To(BeNil())
			time.Sleep(e.ttl.Load())
			Expect(e.Update(testKey, testValue2)).To(BeNil())
			v, err := e.Read(testKey)
			Expect(err).To(BeNil())
			Expect(len(v)).To(Equal(1))
			assert.ElementsMatch(GinkgoT(), v, [][]byte{testValue2})
		})
	})
})
