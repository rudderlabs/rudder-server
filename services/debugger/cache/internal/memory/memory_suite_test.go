package memory_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestMemcache(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "memory Suite")
}
