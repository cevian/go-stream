package stream

import (
	"github.com/cloudflare/go-stream/util/slog"
)

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

	runnerSrc *Runner
	runnerDst *Runner
	//ops     []fanoutChildOp // this can be a single operator or a chain
}

func NewFaninOp() *FaninOperator {
	return &FaninOperator{NewHardStopChannelCloser() /*NewBaseIn(CHAN_SLACK),*/, nil, make(chan Object, CHAN_SLACK), NewRunner(), NewRunner()}
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
	defer op.runnerSrc.Wait()
	defer op.runnerDst.Wait()
	op.runnerSrc.AsyncRunAll()
	op.runnerDst.AsyncRunAll()

	select {
	case err, errOk := <-op.runnerSrc.ErrorChannel():
		if errOk {
			slog.Errorf("Unexpected src err in fanin op", err)
			op.Stop()
			return err
		}
	case err := <-op.runnerDst.ErrorChannel():
		slog.Errorf("Unexpected dst err in fanin op", err)
		op.Stop()
		return err
	}

	//only gets here if runnerSrc closed errorChannel with no errors
	//meaning that all srces exited softly
	close(op.channel)
	slog.Infof("Fanin srcs exited without error")

	return nil

	/*	select {
		case <-op.runner.CloseNotifier():

			slog.Logf(logger.Levels.Error, "Unexpected child close in fanin op")
			op.runner.HardStop()
			return errors.New("Unexpected child close")
		}*/

}
