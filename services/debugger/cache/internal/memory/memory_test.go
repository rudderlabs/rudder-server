package memory

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/rudderlabs/rudder-go-kit/config"
)

var _ = Describe("cache", func() {
	Context("cache testing", func() {
		testKey := "test_key"
		testValue := []byte("test_value")

		It("Cache init", func() {
			var c *Cache[[]byte]
			c, err := New[[]byte]()

			Expect(err).To(BeNil())
			Expect(c.size).NotTo(Equal(0))
			Expect(c.keyTTL).NotTo(Equal(0))
			Expect(c.cacheMap).NotTo(BeNil())
			Expect(c.Stop()).To(BeNil())
		})

		It("Cache update", func() {
			var c *Cache[[]byte]
			c, err := New[[]byte]()
			Expect(err).To(BeNil())

			Expect(c.Update(testKey, testValue)).To(BeNil())
			Expect(len(c.cacheMap)).To(Equal(1))
			Expect(len(c.cacheMap[testKey].data)).To(Equal(1))
			Expect(c.cacheMap[testKey].data[0]).To(Equal(testValue))
			Expect(c.Stop()).To(BeNil())
		})

		It("Cache timeout", func() {
			var c *Cache[[]byte]
			config.Set("LiveEvent.cache.ttl", 10*time.Millisecond)
			config.Set("LiveEvent.cache.clearFreq", 10*time.Millisecond)
			c, err := New[[]byte]()
			Expect(err).To(BeNil())
			Expect(c.Update(testKey, testValue)).To(BeNil())
			Expect(len(c.cacheMap)).To(Equal(1))
			Expect(len(c.cacheMap[testKey].data)).To(Equal(1))
			Expect(c.cacheMap[testKey].data[0]).To(Equal(testValue))
			Eventually(func() int { return len(c.cacheMap) }).Should(Equal(0))
			config.Reset()
			Expect(c.Stop()).To(BeNil())
		})

		It("Cache readAndPopData", func() {
			var c *Cache[[]byte]
			c, err := New[[]byte]()
			Expect(err).To(BeNil())
			Expect(c.Update(testKey, testValue)).To(BeNil())
			v, err := c.Read(testKey)
			Expect(v).To(Equal([][]byte{testValue}))
			Expect(err).To(BeNil())
			Expect(len(c.cacheMap)).To(Equal(0))
			Expect(c.Stop()).To(BeNil())
		})

		It("Cache data store limit", func() {
			var c *Cache[[]byte]
			c, err := New[[]byte]()
			Expect(err).To(BeNil())
			testValue2 := []byte("test_value2")
			testValue3 := []byte("test_value3")
			Expect(c.Update(testKey, testValue)).To(BeNil())
			Expect(c.Update(testKey, testValue2)).To(BeNil())
			Expect(c.Update(testKey, testValue3)).To(BeNil())
			Expect(c.Update(testKey, testValue3)).To(BeNil())
			Expect(len(c.cacheMap)).To(Equal(1))
			Expect(len(c.cacheMap[testKey].data)).To(Equal(3))
			Expect(c.cacheMap[testKey].data[0]).To(Equal(testValue2))
			Expect(c.cacheMap[testKey].data[1]).To(Equal(testValue3))
			Expect(c.Stop()).To(BeNil())
		})
	})
})
