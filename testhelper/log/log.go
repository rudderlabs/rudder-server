package log

import (
	"github.com/onsi/ginkgo/v2"

	"github.com/rudderlabs/rudder-go-kit/logger"
)

var GinkgoLogger logger.Logger = &ginkgoLogger{logger.NOP}

type ginkgoLogger struct {
	logger.Logger
}

func (ginkgoLogger) Debug(args ...interface{}) { ginkgo.GinkgoT().Log(args...) }
func (ginkgoLogger) Info(args ...interface{})  { ginkgo.GinkgoT().Log(args...) }
func (ginkgoLogger) Warn(args ...interface{})  { ginkgo.GinkgoT().Log(args...) }
func (ginkgoLogger) Error(args ...interface{}) { ginkgo.GinkgoT().Log(args...) }
func (ginkgoLogger) Fatal(args ...interface{}) { ginkgo.GinkgoT().Log(args...) }
func (ginkgoLogger) Debugf(format string, args ...interface{}) {
	ginkgo.GinkgoT().Logf(format, args...)
}
func (ginkgoLogger) Infof(format string, args ...interface{}) { ginkgo.GinkgoT().Logf(format, args...) }
func (ginkgoLogger) Warnf(format string, args ...interface{}) { ginkgo.GinkgoT().Logf(format, args...) }
func (ginkgoLogger) Errorf(format string, args ...interface{}) {
	ginkgo.GinkgoT().Logf(format, args...)
}

func (ginkgoLogger) Fatalf(format string, args ...interface{}) {
	ginkgo.GinkgoT().Logf(format, args...)
}

func (ginkgoLogger) Debugw(format string, args ...interface{}) {
	ginkgo.GinkgoT().Logf(format, args...)
}

func (ginkgoLogger) Infow(msg string, keysAndValues ...interface{}) {
	ginkgo.GinkgoT().Log(append([]interface{}{msg}, keysAndValues...)...)
}

func (ginkgoLogger) Warnw(msg string, keysAndValues ...interface{}) {
	ginkgo.GinkgoT().Log(append([]interface{}{msg}, keysAndValues...)...)
}

func (ginkgoLogger) Errorw(msg string, keysAndValues ...interface{}) {
	ginkgo.GinkgoT().Log(append([]interface{}{msg}, keysAndValues...)...)
}

func (ginkgoLogger) Fatalw(msg string, keysAndValues ...interface{}) {
	ginkgo.GinkgoT().Log(append([]interface{}{msg}, keysAndValues...)...)
}
func (ginkgoLogger) With(_ ...interface{}) logger.Logger { return GinkgoLogger }
func (ginkgoLogger) Child(_ string) logger.Logger        { return GinkgoLogger }
func (ginkgoLogger) IsDebugLevel() bool                  { return true }
