package stream

import (
	"errors"
	"fmt"

	"github.com/cevian/go-stream/util/slog"
)

type fanoutChildOp interface {
	Operator
	In
}

type FanoutOperator struct {
	*HardStopChannelCloser
	*BaseIn
	outputs []chan Object
	runner  Runner
	//ops     []fanoutChildOp // this can be a single operator or a chain
	fin chan struct{}
	err error
}

func NewFanoutOp() *FanoutOperator {
	r := NewFailFastRunner()
	op := &FanoutOperator{NewHardStopChannelCloser(), NewBaseIn(CHAN_SLACK), make([]chan Object, 0, 2), r, make(chan struct{}), nil}

	errHandler := func(err error) {
		slog.Errorf("Unexpected child error in fanout op %v", err)
		op.err = err
	}

	finHandler := func() {
		select {
		case <-op.fin:
		default:
			close(op.fin)
		}
	}

	r.SetErrorHandler(errHandler)
	r.SetFinishedHandler(finHandler)
	return op
}

func (op *FanoutOperator) Add(newOp fanoutChildOp) {
	ch := make(chan Object, CHAN_SLACK)
	newOp.SetIn(ch)
	op.outputs = append(op.outputs, ch)
	op.runner.Add(newOp)
}

func (op *FanoutOperator) Run() error {
	defer op.runner.WaitGroup().Wait()
	op.runner.AsyncRunAll()

	defer func() {
		for _, out := range op.outputs {
			close(out)
		}
	}()

	for {
		select {
		case obj, ok := <-op.In():
			if ok {
				for _, out := range op.outputs {
					out <- obj
				}
			} else {
				return nil
			}
		case <-op.StopNotifier:
			op.runner.HardStop()
			return nil
		case <-op.fin:
			slog.Errorf("Unexpected child close in fanout op")
			return errors.New(fmt.Sprintf("Unexpected child close: %v", op.err))
		}
	}
}
