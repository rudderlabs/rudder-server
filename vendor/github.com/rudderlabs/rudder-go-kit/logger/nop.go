package logger

import "net/http"

var NOP Logger = nop{}

type nop struct{}

func (nop) Debug(_ ...interface{})            {}
func (nop) Info(_ ...interface{})             {}
func (nop) Warn(_ ...interface{})             {}
func (nop) Error(_ ...interface{})            {}
func (nop) Fatal(_ ...interface{})            {}
func (nop) Debugf(_ string, _ ...interface{}) {}
func (nop) Infof(_ string, _ ...interface{})  {}
func (nop) Warnf(_ string, _ ...interface{})  {}
func (nop) Errorf(_ string, _ ...interface{}) {}
func (nop) Fatalf(_ string, _ ...interface{}) {}
func (nop) Debugw(_ string, _ ...interface{}) {}
func (nop) Infow(_ string, _ ...interface{})  {}
func (nop) Warnw(_ string, _ ...interface{})  {}
func (nop) Errorw(_ string, _ ...interface{}) {}
func (nop) Fatalw(_ string, _ ...interface{}) {}
func (nop) LogRequest(_ *http.Request)        {}
func (nop) With(_ ...interface{}) Logger      { return NOP }
func (nop) Child(_ string) Logger             { return NOP }
func (nop) IsDebugLevel() bool                { return false }
