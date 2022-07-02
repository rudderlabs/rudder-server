package redshift_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestRedshift(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Redshift Suite")
}
