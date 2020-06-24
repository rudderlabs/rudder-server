package crash_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestCrash(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Crash Suite")
}
