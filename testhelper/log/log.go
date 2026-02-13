package log

import (
	"github.com/onsi/ginkgo/v2"

	"github.com/rudderlabs/rudder-go-kit/logger"
)

var GinkgoLogger logger.Logger = &ginkgoLogger{logger.NOP}

type ginkgoLogger struct {
	logger.Logger
}

func (ginkgoLogger) Debug(args ...any) { ginkgo.GinkgoT().Log(args...) }
func (ginkgoLogger) Info(args ...any)  { ginkgo.GinkgoT().Log(args...) }
func (ginkgoLogger) Warn(args ...any)  { ginkgo.GinkgoT().Log(args...) }
func (ginkgoLogger) Error(args ...any) { ginkgo.GinkgoT().Log(args...) }
func (ginkgoLogger) Fatal(args ...any) { ginkgo.GinkgoT().Log(args...) }
func (ginkgoLogger) Debugf(format string, args ...any) {
	ginkgo.GinkgoT().Logf(format, args...)
}
func (ginkgoLogger) Infof(format string, args ...any) { ginkgo.GinkgoT().Logf(format, args...) }
func (ginkgoLogger) Warnf(format string, args ...any) { ginkgo.GinkgoT().Logf(format, args...) }
func (ginkgoLogger) Errorf(format string, args ...any) {
	ginkgo.GinkgoT().Logf(format, args...)
}

func (ginkgoLogger) Fatalf(format string, args ...any) {
	ginkgo.GinkgoT().Logf(format, args...)
}

func (ginkgoLogger) Debugw(format string, args ...any) {
	ginkgo.GinkgoT().Logf(format, args...)
}

func (ginkgoLogger) Infow(msg string, keysAndValues ...any) {
	ginkgo.GinkgoT().Log(append([]any{msg}, keysAndValues...)...)
}

func (ginkgoLogger) Warnw(msg string, keysAndValues ...any) {
	ginkgo.GinkgoT().Log(append([]any{msg}, keysAndValues...)...)
}

func (ginkgoLogger) Errorw(msg string, keysAndValues ...any) {
	ginkgo.GinkgoT().Log(append([]any{msg}, keysAndValues...)...)
}

func (ginkgoLogger) Fatalw(msg string, keysAndValues ...any) {
	ginkgo.GinkgoT().Log(append([]any{msg}, keysAndValues...)...)
}
func (ginkgoLogger) With(_ ...any) logger.Logger  { return GinkgoLogger }
func (ginkgoLogger) Child(_ string) logger.Logger { return GinkgoLogger }
func (ginkgoLogger) IsDebugLevel() bool           { return true }
