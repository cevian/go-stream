// Package operator implements streaming operators
package stream

import (
	"fmt"
	"reflect"
)

// This is the default channel slack operators should use when creating output channels
const CHAN_SLACK = 100

type Object interface{}

type FTResponder interface {
	Target() int64
	Offset() int64
	Epoch() int64
}

type FTResponse struct {
	target int64
	offset int64
	epoch  int64
}

func (f FTResponse) Target() int64 {
	return f.target
}

func (f FTResponse) Offset() int64 {

	return f.offset
}

func (f FTResponse) Epoch() int64 {

	return f.epoch
}

type FTResetter interface {
	Reset() bool
	Cause() string
	ID() int64
}

type FTReset struct {
	Reason    string
	Source_id int64
}

func (ftr FTReset) Reset() bool {
	return true
}

func (ftr FTReset) Cause() string {
	return ftr.Reason
}

func (ftr FTReset) ID() int64 {
	return ftr.Source_id
}

/* soft stops are created by closing the input channel */
type Operator interface {
	// Init() bool   //?? do we want this?

	// Run runs the operation of the stream. It should never return before all the goroutines it started have quit
	// It should return in 3 cases:
	// i) on error (returning the error)
	// ii) on soft close (Its input has closed the channel, return nil)
	// iii) on hard close (Stop was called on the operator, return nil)
	Run() error

	// Stop force a hard close of the stream. Look at HardStopChannelCloser for a possible implementation. Should be thread-safe
	// Stop will only be called once but it can be called before or after run exits. If called after, is a no-op
	Stop() error
}

type ParallelizableOperator interface {
	Operator
	IsParallel() bool
	IsOrdered() bool
	MakeOrdered() ParallelizableOperator
}

type Out interface {
	Out() chan Object
	SetOut(c chan Object)
}

type In interface {
	In() chan Object
	SetIn(c chan Object)
	GetInDepth() int
}

type InOutOperator interface {
	Operator
	Out
	In
}

func Name(op Operator) string {
	stringer, ok := op.(fmt.Stringer)
	if ok {
		return stringer.String()
	}
	return reflect.TypeOf(op).String()
}
