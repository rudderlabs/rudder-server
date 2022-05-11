package pgnotifier_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestPgnotifier(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Pgnotifier Suite")
}
