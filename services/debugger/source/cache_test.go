package sourcedebugger

import (
	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/utils"
	"time"
)

var _ = Describe("cache", func() {
	initEventUploader()
	var (
		c              *eventUploaderContext
	)

	BeforeEach(func() {
		c = &eventUploaderContext{}
		c.Setup()
	})

	AfterEach(func() {
		c.Finish()
	})

	Context("cache testing", func() {
		It("Cache init", func() {
			c.mockBackendConfig.EXPECT().Subscribe(gomock.Any(), backendconfig.TopicProcessConfig).
				Do(func(channel chan utils.DataEvent, topic backendconfig.Topic) {
					// on Subscribe, emulate a backend configuration event
					go func() {
						channel <- utils.DataEvent{Data: sampleBackendConfig, Topic: string(topic)}
						c.configInitialised = true
					}()
				}).
				Do(c.asyncHelper.ExpectAndNotifyCallback()).Return().Times(1)

			var m Cache
			m.init()
			Expect(m.Size).To(Equal(CacheMaxSize))
			Expect(m.KeyTTL).To(Equal(CacheTTL))
			Expect(m.m).NotTo(BeNil())
		})

		It("Cache update", func() {
			c.mockBackendConfig.EXPECT().Subscribe(gomock.Any(), backendconfig.TopicProcessConfig).
				Do(func(channel chan utils.DataEvent, topic backendconfig.Topic) {
					// on Subscribe, emulate a backend configuration event
					go func() {
						channel <- utils.DataEvent{Data: sampleBackendConfig, Topic: string(topic)}
						c.configInitialised = true
					}()
				}).
				Do(c.asyncHelper.ExpectAndNotifyCallback()).Return().Times(1)

			var m Cache
			m.update("test_key", "test_value")
			Expect(len(m.m)).To(Equal(1))
			Expect(len(m.m["test_key"].objs)).To(Equal(1))
			Expect(m.m["test_key"].objs[0]).To(Equal("test_value"))
		})

		It("Cache timeout", func() {
			c.mockBackendConfig.EXPECT().Subscribe(gomock.Any(), backendconfig.TopicProcessConfig).
				Do(func(channel chan utils.DataEvent, topic backendconfig.Topic) {
					// on Subscribe, emulate a backend configuration event
					go func() {
						channel <- utils.DataEvent{Data: sampleBackendConfig, Topic: string(topic)}
						c.configInitialised = true
					}()
				}).
				Do(c.asyncHelper.ExpectAndNotifyCallback()).Return().Times(1)

			var m Cache
			m.init()
			m.KeyTTL = time.Second
			Expect(m.KeyTTL).To(Equal(time.Second))
			m.update("test_key", "test_value")
			Expect(len(m.m)).To(Equal(1))
			Expect(len(m.m["test_key"].objs)).To(Equal(1))
			Expect(m.m["test_key"].objs[0]).To(Equal("test_value"))
			time.Sleep(time.Second * 2)
			Expect(len(m.m)).To(Equal(0))
		})

		It("Cache readAndPopData", func() {
			c.mockBackendConfig.EXPECT().Subscribe(gomock.Any(), backendconfig.TopicProcessConfig).
				Do(func(channel chan utils.DataEvent, topic backendconfig.Topic) {
					// on Subscribe, emulate a backend configuration event
					go func() {
						channel <- utils.DataEvent{Data: sampleBackendConfig, Topic: string(topic)}
						c.configInitialised = true
					}()
				}).
				Do(c.asyncHelper.ExpectAndNotifyCallback()).Return().Times(1)

			var m Cache
			m.update("test_key", "test_value")
			v := m.readAndPopData("test_key")
			Expect(v).To(Equal([]string{"test_value"}))
			Expect(len(m.m)).To(Equal(0))
		})

		It("Cache data store limit", func() {
			c.mockBackendConfig.EXPECT().Subscribe(gomock.Any(), backendconfig.TopicProcessConfig).
				Do(func(channel chan utils.DataEvent, topic backendconfig.Topic) {
					// on Subscribe, emulate a backend configuration event
					go func() {
						channel <- utils.DataEvent{Data: sampleBackendConfig, Topic: string(topic)}
						c.configInitialised = true
					}()
				}).
				Do(c.asyncHelper.ExpectAndNotifyCallback()).Return().Times(1)

			var m Cache
			m.Size = 2
			m.update("test_key", "test_value1")
			m.update("test_key", "test_value2")
			m.update("test_key", "test_value3")
			Expect(len(m.m)).To(Equal(1))
			Expect(len(m.m["test_key"].objs)).To(Equal(2))
			Expect(m.m["test_key"].objs[0]).To(Equal("test_value2"))
			Expect(m.m["test_key"].objs[1]).To(Equal("test_value3"))
		})
	})

})
