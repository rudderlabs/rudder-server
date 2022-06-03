package bigquery_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestBigquery(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Bigquery Suite")
}
