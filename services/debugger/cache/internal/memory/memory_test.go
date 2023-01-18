package memory

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("cache", func() {
	Context("cache testing", func() {
		testKey := "test_key"
		testValue := []byte("test_value")

		It("Cache init", func() {
			var c Cache[[]byte]
			c.init()
			Expect(c.size).NotTo(Equal(0))
			Expect(c.keyTTL).NotTo(Equal(0))
			Expect(c.cacheMap).NotTo(BeNil())
		})

		It("Cache update", func() {
			var c Cache[[]byte]
			Expect(c.Update(testKey, testValue)).To(BeNil())
			Expect(len(c.cacheMap)).To(Equal(1))
			Expect(len(c.cacheMap[testKey].data)).To(Equal(1))
			Expect(c.cacheMap[testKey].data[0]).To(Equal(testValue))
		})

		It("Cache timeout", func() {
			c := Cache[[]byte]{
				keyTTL:      10 * time.Millisecond,
				cleanupFreq: 10 * time.Millisecond,
			}
			Expect(c.Update(testKey, testValue)).To(BeNil())
			Expect(len(c.cacheMap)).To(Equal(1))
			Expect(len(c.cacheMap[testKey].data)).To(Equal(1))
			Expect(c.cacheMap[testKey].data[0]).To(Equal(testValue))
			Eventually(func() int { return len(c.cacheMap) }).Should(Equal(0))
		})

		It("Cache readAndPopData", func() {
			var c Cache[[]byte]
			Expect(c.Update(testKey, testValue)).To(BeNil())
			v, err := c.Read(testKey)
			Expect(v).To(Equal([][]byte{testValue}))
			Expect(err).ToNot(BeNil())
			Expect(len(c.cacheMap)).To(Equal(0))
		})

		It("Cache data store limit", func() {
			c := Cache[[]byte]{
				size: 2,
			}
			testValue2 := []byte("test_value2")
			testValue3 := []byte("test_value3")
			Expect(c.Update(testKey, testValue)).To(BeNil())
			Expect(c.Update(testKey, testValue2)).To(BeNil())
			Expect(c.Update(testKey, testValue3)).To(BeNil())
			Expect(len(c.cacheMap)).To(Equal(1))
			Expect(len(c.cacheMap[testKey].data)).To(Equal(2))
			Expect(c.cacheMap[testKey].data[0]).To(Equal(testValue2))
			Expect(c.cacheMap[testKey].data[1]).To(Equal(testValue3))
		})
	})
})
