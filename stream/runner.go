package stream

import (
	"github.com/cloudflare/go-stream/util/slog"
	"sync"
)

type Runner struct {
	ops           []Operator
	closenotifier chan bool
	errors        chan error
	wg            *sync.WaitGroup
	errorcloser   sync.Once
	finished      bool
}

func NewRunner() *Runner {
	return &Runner{make([]Operator, 0, 2), make(chan bool), make(chan error, 1), &sync.WaitGroup{}, sync.Once{}, false}
}

func (r *Runner) WaitGroup() *sync.WaitGroup {
	return r.wg
}

func (r *Runner) Wait() {
	r.wg.Wait()
}

/* error channel returns errors of the ops, as many as it can, will close after all ops finish */
func (r *Runner) ErrorChannel() <-chan error {
	return r.errors
}

/* This fires when an operator is first exited */
func (r *Runner) CloseNotifier() <-chan bool {
	return r.closenotifier
}

func (r *Runner) Operators() []Operator {
	return r.ops
}

func (r *Runner) AsyncRun(op Operator) {
	if r.finished {
		panic("Runner finished")
	}

	r.wg.Add(1)

	r.errorcloser.Do(func() {
		go func() {
			r.wg.Wait()
			close(r.errors)
			r.finished = true
		}()
	})

	go func() {
		defer r.wg.Done()
		err := op.Run()
		if err != nil {
			slog.Errorf("Got an err from a child in runner: %v", err)
			select {
			case r.errors <- err:
			default:
			}
		}
		//on first exit, the cn channel is closed
		select {
		case <-r.closenotifier: //if already closed no-op
		default:
			close(r.closenotifier)
		}
	}()
}

func (r *Runner) Add(op Operator) {
	r.ops = append(r.ops, op)
}

func (r *Runner) AsyncRunAll() {
	for _, op := range r.ops {
		r.AsyncRun(op)
	}
}

func (r *Runner) HardStop() {
	for _, op := range r.ops {
		op.Stop()
	}

}
