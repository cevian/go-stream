package stream

import (
	"errors"
	"github.com/cevian/go-stream/util/slog"
)

type DistributorChildOp interface {
	Operator
	In
}

type DistribKey interface{}

type DistributeOperator struct {
	*HardStopChannelCloser
	*BaseIn
	mapper        func(Object) DistribKey
	branchCreator func(DistribKey) (DistributorChildOp, bool)
	//the 2nd returnd tells the distributor whether or not to run the new op
	outputs map[DistribKey]chan<- Object
	runner  *Runner
}

func NewDistributor(mapp func(Object) DistribKey, creator func(DistribKey) (DistributorChildOp, bool)) *DistributeOperator {
	return &DistributeOperator{NewHardStopChannelCloser(),
		NewBaseIn(CHAN_SLACK), mapp, creator, make(map[DistribKey]chan<- Object), NewRunner()}
}

func (op *DistributeOperator) createBranch(key DistribKey) {
	newop, shouldRun := op.branchCreator(key)
	ch := make(chan Object, CHAN_SLACK)
	newop.SetIn(ch)
	op.runner.Add(newop)
	op.outputs[key] = ch
	if shouldRun {
		op.runner.AsyncRun(newop, false)
	}
}

func (op *DistributeOperator) Run() error {
	defer op.runner.WaitGroup().Wait()
	defer func() {
		for _, out := range op.outputs {
			close(out)
		}
	}()

	for {
		select {
		case obj, ok := <-op.In():
			if ok {
				key := op.mapper(obj)
				ch, ok := op.outputs[key]
				if !ok {
					op.createBranch(key)
					ch, ok = op.outputs[key]
					if !ok {
						slog.Fatalf("couldn't find channel right after key create")
					}

				}
				ch <- obj
			} else {
				//slog.Fatalf("Nil!")
				return nil
			}
		case <-op.StopNotifier:
			op.runner.HardStop()
			return nil
		case <-op.runner.CloseNotifier():
			slog.Errorf("Unexpected child close in distribute op")
			op.runner.HardStop()
			return errors.New("Unexpected distribute child close")
		}
	}
}
