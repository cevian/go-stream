package stream

import (
	"reflect"
	"sync"

	"github.com/cevian/go-stream/util/slog"
)

type Runner interface {
	SetName(name string)
	WaitGroup() *sync.WaitGroup
	Wait() error

	Operators() []Operator
	AsyncRun(op Operator)

	Add(op Operator)
	AsyncRunAll()
	HardStop()

	/* operator compat */
	Run() error
	Stop() error

	/* handler will be called on each error */
	SetErrorHandler(handler func(error))

	/* handler will be called when all ops exited */
	SetFinishedHandler(handler func())

	/* handler to be called when an op exits */
	SetOpCloseHandler(handler func(Operator, error))
}

type FailFastRunner struct {
	*FailSilentRunner
	errorHandler func(err error)
}

func NewRunner() Runner {
	return NewFailFastRunner()
}

func NewFailFastRunner() *FailFastRunner {
	r := NewFailSilentRunner()
	t := &FailFastRunner{r, nil}

	stopped := false
	errorHandler := func(err error) {
		if !stopped {
			r.HardStop()
		}
		if t.errorHandler != nil {
			t.errorHandler(err)
		}
	}
	r.SetErrorHandler(errorHandler)
	return t
}

func (t *FailFastRunner) SetErrorHandler(h func(error)) {
	t.errorHandler = h
}

type FailSilentRunner struct {
	ops            []Operator
	wg             *sync.WaitGroup
	finished       bool
	Name           string
	errorHandler   func(error)
	finHandler     func()
	finOnce        sync.Once
	opCloseHandler func(Operator, error)
	firstError     error
	firstErrorOnce sync.Once
}

func NewFailSilentRunner() *FailSilentRunner {
	return &FailSilentRunner{make([]Operator, 0, 2), &sync.WaitGroup{}, false, "GenericRunner", nil, nil, sync.Once{}, nil, nil, sync.Once{}}
}

func (c *FailSilentRunner) SetName(name string) {
	c.Name = name
}

func (r *FailSilentRunner) WaitGroup() *sync.WaitGroup {
	return r.wg
}

func (c *FailSilentRunner) Run() error {
	c.AsyncRunAll()
	return c.Wait()
}

func (c *FailSilentRunner) Stop() error {
	c.HardStop()
	return nil
}

func (r *FailSilentRunner) Wait() error {
	r.wg.Wait()
	return r.firstError
}

func (r *FailSilentRunner) Operators() []Operator {
	return r.ops
}

func (r *FailSilentRunner) AsyncRun(op Operator) {
	if r.finished {
		panic("FailSilentRunner finished")
	}

	r.wg.Add(1)

	r.finOnce.Do(func() {
		if r.finHandler != nil {
			go func() {
				r.wg.Wait()
				r.finHandler()
				r.finished = true
			}()
		}
	})

	go func() {
		defer r.wg.Done()
		err := op.Run()
		slog.Errorf("The following operator exited (%v, %v) in runner %s: %v", op, reflect.TypeOf(op), r.Name, err)
		if err != nil {
			//slog.Errorf("Got an err from a child (%v, %v) in runner %s: %v", op, reflect.TypeOf(op), r.Name, err)
			if r.errorHandler != nil {
				r.errorHandler(err)
			}
			r.firstErrorOnce.Do(func() {
				r.firstError = err
			})
		}

		if r.opCloseHandler != nil {
			r.opCloseHandler(op, err)
		}
	}()
}

func (r *FailSilentRunner) Add(op Operator) {
	r.ops = append(r.ops, op)
}

func (r *FailSilentRunner) AsyncRunAll() {
	for _, op := range r.ops {
		r.AsyncRun(op)
	}
}

func (r *FailSilentRunner) HardStop() {
	for _, op := range r.ops {
		op.Stop()
	}
}

/* handler will be called on each error */
func (r *FailSilentRunner) SetErrorHandler(handler func(error)) {
	r.errorHandler = handler
}

/* handler will be called when all ops exited */
func (r *FailSilentRunner) SetFinishedHandler(handler func()) {
	r.finHandler = handler
}

/* handler to be called when an op exits */
func (r *FailSilentRunner) SetOpCloseHandler(handler func(Operator, error)) {
	r.opCloseHandler = handler
}
