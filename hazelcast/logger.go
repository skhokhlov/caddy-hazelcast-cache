package hazelcast

import "github.com/darkweak/storages/core"

// noopCoreLogger satisfies core.Logger with do-nothing methods. The provider
// currently swallows the structured logs emitted by core.MappingUpdater /
// core.MappingElection; full structured logging is wired up in Phase 4.3.
type noopCoreLogger struct{}

func (noopCoreLogger) Debug(args ...interface{})                   {}
func (noopCoreLogger) Info(args ...interface{})                    {}
func (noopCoreLogger) Warn(args ...interface{})                    {}
func (noopCoreLogger) Error(args ...interface{})                   {}
func (noopCoreLogger) DPanic(args ...interface{})                  {}
func (noopCoreLogger) Panic(args ...interface{})                   {}
func (noopCoreLogger) Fatal(args ...interface{})                   {}
func (noopCoreLogger) Debugf(template string, args ...interface{}) {}
func (noopCoreLogger) Infof(template string, args ...interface{})  {}
func (noopCoreLogger) Warnf(template string, args ...interface{})  {}
func (noopCoreLogger) Errorf(template string, args ...interface{}) {}
func (noopCoreLogger) DPanicf(template string, args ...interface{}){}
func (noopCoreLogger) Panicf(template string, args ...interface{}) {}
func (noopCoreLogger) Fatalf(template string, args ...interface{}) {}

var _ core.Logger = noopCoreLogger{}
