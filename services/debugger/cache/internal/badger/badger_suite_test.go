package badger_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestBadgercache(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Badgercache Suite")
}
