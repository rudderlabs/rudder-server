package crash_test

import (
	"encoding/json"
	"io/ioutil"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/rudderlabs/rudder-server/app/crash"
)

var _ = Describe("Crash Report", func() {
	var c *crash.Manager

	BeforeEach(func() {
		c = crash.New()
		baseDir, err := ioutil.TempDir("", "")
		if err != nil {
			panic(err)
		}

		c.Report.BaseDir = baseDir
		c.Report.EnsureTempDir()
	})

	It("should create report metadata file", func() {
		c.Report.Metadata = map[string]interface{}{
			"key": "value",
		}

		// save metadata
		metadataPath, err := c.Report.SaveMetadata()
		if err != nil {
			panic(err)
		}

		// file should be created in c.Report.BaseDir
		Expect(metadataPath).To(HavePrefix(c.Report.BaseDir))

		// read stored metadata file
		data, err := ioutil.ReadFile(metadataPath)
		if err != nil {
			panic(err)
		}

		// convert to map and compare
		var metadata = make(map[string]interface{})
		err = json.Unmarshal(data, &metadata)
		if err != nil {
			panic(err)
		}

		Expect(metadata["key"]).To(Equal(c.Report.Metadata["key"]))
	})
})
