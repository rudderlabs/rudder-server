package snowflake_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestSnowflake(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Snowflake Suite")
}
