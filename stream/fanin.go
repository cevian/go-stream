package stream

import (
	"github.com/cevian/go-stream/util/slog"
)

/*
TODO: consider case where distributor followed by a fan-in. The distributor makes no branches for fan-in and closes.
fan-in will never close then
*/

type faninDestOp interface {
	Operator
	In
	Out
}

type faninSrcOp interface {
	Operator
	Out
	/* tells the operator whether or not to close its output channel on exit */
	SetCloseOnExit(flag bool)
}

type FaninOperator struct {
	*HardStopChannelCloser
	//*BaseIn
	dst     faninDestOp
	channel chan Object

	runnerSrc Runner
	runnerDst Runner
	isRunning bool
	//ops     []fanoutChildOp // this can be a single operator or a chain
	err error
}

func NewFaninOp() *FaninOperator {
	runnerSrc := NewRunner()
	runnerDst := NewRunner()

	op := &FaninOperator{NewHardStopChannelCloser() /*NewBaseIn(CHAN_SLACK),*/, nil, make(chan Object, CHAN_SLACK), runnerSrc, runnerDst, false, nil}

	srcErr := func(err error) {
		slog.Errorf("Unexpected src err in fanin op %v", err)
		op.Stop()
		if op.err == nil {
			op.err = err
		}
	}

	runnerSrc.SetErrorHandler(srcErr)

	dstErr := func(err error) {
		slog.Errorf("Unexpected dst err in fanin op %v", err)
		op.Stop()
		if op.err == nil {
			op.err = err
		}
	}

	runnerDst.SetErrorHandler(dstErr)

	return op
}

func (op *FaninOperator) SetDest(newOp faninDestOp) {
	newOp.SetIn(op.channel)
	op.runnerDst.Add(newOp)
	op.dst = newOp
}

func (op *FaninOperator) AddSrc(newOp faninSrcOp) {
	newOp.SetOut(op.channel)
	newOp.SetCloseOnExit(false)
	op.runnerSrc.Add(newOp)
	if op.isRunning {
		op.runnerSrc.AsyncRun(newOp)
	}
}

func (op *FaninOperator) Out() chan Object {
	return op.dst.Out()
}

func (o *FaninOperator) SetOut(c chan Object) {
	o.dst.SetOut(c)
}

func (op *FaninOperator) Stop() error {
	op.runnerSrc.HardStop()
	op.runnerDst.HardStop()
	return nil
}

func (op *FaninOperator) Run() error {
	defer op.runnerDst.Wait()
	op.isRunning = true
	op.runnerSrc.AsyncRunAll()
	op.runnerDst.AsyncRunAll()

	//only gets here if runnerSrc closed

	op.runnerSrc.Wait()
	close(op.channel)
	slog.Infof("Fanin srcs exited")

	return op.err
}
