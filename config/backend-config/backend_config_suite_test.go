package backendconfig_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestBackendConfig(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "BackendConfig Suite")
}
