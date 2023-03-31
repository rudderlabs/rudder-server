package deltalake_native_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestDeltalakeNative(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Deltalake Native Suite")
}
