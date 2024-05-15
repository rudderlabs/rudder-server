package klaviyobulkupload_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestKlaviyobulkupload(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Klaviyobulkupload Suite")
}
