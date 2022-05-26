package misc_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestMisc(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Misc Suite")
}
