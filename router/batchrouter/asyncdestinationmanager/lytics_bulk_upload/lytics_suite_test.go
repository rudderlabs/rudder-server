package lyticsBulkUpload_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestLyticsbulkupload(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "lytics_bulk_upload Suite")
}
