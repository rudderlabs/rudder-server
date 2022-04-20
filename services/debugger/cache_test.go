package debugger

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"time"
)

var _ = Describe("cache", func() {

	Context("cache testing", func() {
		testKey := "test_key"
		testValue := []byte("test_value")

		It("Cache init", func() {
			var c Cache
			c.init()
			Expect(c.Size).NotTo(Equal(0))
			Expect(c.KeyTTL).NotTo(Equal(0))
			Expect(c.cacheMap).NotTo(BeNil())
		})

		It("Cache update", func() {
			var c Cache
			c.Update(testKey, testValue)
			Expect(len(c.cacheMap)).To(Equal(1))
			Expect(len(c.cacheMap[testKey].data)).To(Equal(1))
			Expect(c.cacheMap[testKey].data[0]).To(Equal(testValue))
		})

		It("Cache timeout", func() {
			c := Cache{
				KeyTTL:      10 * time.Millisecond,
				CleanupFreq: 10 * time.Millisecond,
			}
			c.Update(testKey, testValue)
			Expect(len(c.cacheMap)).To(Equal(1))
			Expect(len(c.cacheMap[testKey].data)).To(Equal(1))
			Expect(c.cacheMap[testKey].data[0]).To(Equal(testValue))
			Eventually(func() int { return len(c.cacheMap) }).Should(Equal(0))
		})

		It("Cache readAndPopData", func() {
			var c Cache
			c.Update(testKey, testValue)
			v := c.ReadAndPopData(testKey)
			Expect(v).To(Equal([][]byte{testValue}))
			Expect(len(c.cacheMap)).To(Equal(0))
		})

		It("Cache data store limit", func() {
			c := Cache{
				Size: 2,
			}
			testValue2 := []byte("test_value2")
			testValue3 := []byte("test_value3")
			c.Update(testKey, testValue)
			c.Update(testKey, testValue2)
			c.Update(testKey, testValue3)
			Expect(len(c.cacheMap)).To(Equal(1))
			Expect(len(c.cacheMap[testKey].data)).To(Equal(2))
			Expect(c.cacheMap[testKey].data[0]).To(Equal(testValue2))
			Expect(c.cacheMap[testKey].data[1]).To(Equal(testValue3))
		})

	})
})
