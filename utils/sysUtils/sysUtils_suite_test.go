package sysUtils_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestSysUtils(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "SysUtils Suite")
}
