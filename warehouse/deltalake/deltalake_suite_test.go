package deltalake_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestDeltalake(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Deltalake Suite")
}
