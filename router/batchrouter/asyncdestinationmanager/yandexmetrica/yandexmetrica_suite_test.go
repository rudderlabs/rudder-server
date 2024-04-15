package yandexmetrica_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestYandexmetrica(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Yandexmetrica Suite")
}
