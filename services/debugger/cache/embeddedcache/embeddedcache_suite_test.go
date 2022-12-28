package embeddedcache_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestEmbeddedcache(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Embeddedcache Suite")
}
