package logger

import "net/http"

var _ LoggerI = &NOP{}

type NOP struct{}

func (N NOP) Debug(_ ...interface{})            {}
func (N NOP) Info(_ ...interface{})             {}
func (N NOP) Warn(_ ...interface{})             {}
func (N NOP) Error(_ ...interface{})            {}
func (N NOP) Fatal(_ ...interface{})            {}
func (N NOP) Debugf(_ string, _ ...interface{}) {}
func (N NOP) Infof(_ string, _ ...interface{})  {}
func (N NOP) Warnf(_ string, _ ...interface{})  {}
func (N NOP) Errorf(_ string, _ ...interface{}) {}
func (N NOP) Fatalf(_ string, _ ...interface{}) {}
func (N NOP) LogRequest(_ *http.Request)        {}
func (N NOP) Child(_ string) LoggerI            { return &NOP{} }
func (N NOP) IsDebugLevel() bool                { return false }
