package replay_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestReplay(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Replay Suite")
}
