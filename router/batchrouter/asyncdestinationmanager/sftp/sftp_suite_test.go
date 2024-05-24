package sftp

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestSFTP(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "SFTP Suite")
}
