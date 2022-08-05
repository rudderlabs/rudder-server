package logger

import "net/http"

var _ LoggerI = &NOP{}

type NOP struct{}

func (NOP) Debug(_ ...interface{})            {}
func (NOP) Info(_ ...interface{})             {}
func (NOP) Warn(_ ...interface{})             {}
func (NOP) Error(_ ...interface{})            {}
func (NOP) Fatal(_ ...interface{})            {}
func (NOP) Debugf(_ string, _ ...interface{}) {}
func (NOP) Infof(_ string, _ ...interface{})  {}
func (NOP) Warnf(_ string, _ ...interface{})  {}
func (NOP) Errorf(_ string, _ ...interface{}) {}
func (NOP) Fatalf(_ string, _ ...interface{}) {}
func (NOP) LogRequest(_ *http.Request)        {}
func (NOP) Child(_ string) LoggerI            { return &NOP{} }
func (NOP) IsDebugLevel() bool                { return false }
