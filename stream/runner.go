package stream

import (
	"sync"

	"github.com/cevian/go-stream/util/slog"
)

type Runner interface {
	SetName(name string)
	WaitGroup() *sync.WaitGroup
	Wait() error
	/* error channel returns errors of the ops, as many as it can, will close after all ops finish */
	ErrorChannel() <-chan error
	/* This fires when an operator is first exited */
	CloseNotifier() <-chan bool
	Operators() []Operator
	AsyncRun(op Operator, startCloser bool)

	Add(op Operator)
	AsyncRunAll()
	HardStop()
}

type FailFastRunner struct {
	*FailSilentRunner
	failError sync.Once
}

func NewRunner() *FailFastRunner {
	return &FailFastRunner{NewFailSilentRunner(), sync.Once{}}
}

func (t *FailFastRunner) AsyncRun(op Operator, startCloser bool) {
	t.FailSilentRunner.AsyncRun(op, startCloser)
	t.failError.Do(func() {
		go t.monitorErrors()
	})
}

func (t *FailFastRunner) AsyncRunAll() {
	t.FailSilentRunner.AsyncRunAll()
	t.failError.Do(func() {
		go t.monitorErrors()
	})
}

func (t *FailFastRunner) monitorErrors() error {
	slog.Infof("Starting monitorErrors for failfast in %s", t.Name)
	err, errOk := <-t.ErrorChannel()
	if errOk {
		slog.Warnf("Hard Close in FailFastRunner %s %v", t.Name, err)
		t.HardStop()
		select {
		case t.errors <- err:
		default:
		}
		return err
	}
	return nil
}

/*
func (t *FailFastRunner) Wait() error {
	//slog.Logf(logger.Levels.Info, "Waiting for closenotify %s", c.Name)
	//<-c.runner.CloseNotifier()
	err, errOk := <-t.ErrorChannel()
	if errOk {
		slog.Warnf("Hard Close in FailFastRunner %s %v", t.Name, err)
		t.HardStop()
	}
	slog.Infof("Waiting for runner %s to finish", t.Name)
	t.FailSilentRunner.Wait()
	slog.Infof("Exiting Runner %s", t.Name)

	return err
}*/

type FailSilentRunner struct {
	ops                []Operator
	closenotifier      chan bool
	closenotifiermutex sync.Mutex
	errors             chan error
	wg                 *sync.WaitGroup
	errorcloser        sync.Once
	finished           bool
	Name               string
}

func NewFailSilentRunner() *FailSilentRunner {
	return &FailSilentRunner{make([]Operator, 0, 2), make(chan bool), sync.Mutex{}, make(chan error, 1), &sync.WaitGroup{}, sync.Once{}, false, "GenericRunner"}
}

func (c *FailSilentRunner) SetName(name string) {
	c.Name = name
}

func (r *FailSilentRunner) WaitGroup() *sync.WaitGroup {
	return r.wg
}

func (r *FailSilentRunner) Wait() error {
	r.wg.Wait()
	select {
	case e := <-r.ErrorChannel():
		return e
	default:
		return nil
	}
}

/* error channel returns errors of the ops, as many as it can, will close after all ops finish */
func (r *FailSilentRunner) ErrorChannel() <-chan error {
	return r.errors
}

/* This fires when an operator is first exited */
func (r *FailSilentRunner) CloseNotifier() <-chan bool {
	return r.closenotifier
}

func (r *FailSilentRunner) Operators() []Operator {
	return r.ops
}

func (r *FailSilentRunner) AsyncRun(op Operator, startCloser bool) {
	if r.finished {
		panic("FailSilentRunner finished")
	}

	r.wg.Add(1)

	if startCloser {
		r.errorcloser.Do(func() {
			go func() {
				r.wg.Wait()
				close(r.errors)
				r.finished = true
			}()
		})
	}

	go func() {
		defer r.wg.Done()
		err := op.Run()
		if err != nil {
			slog.Errorf("Got an err from a child (%v) in runner %s: %v", op, r.Name, err)
			select {
			case r.errors <- err:
			default:
			}
		}
		//on first exit, the cn channel is closed
		r.closenotifiermutex.Lock()
		select {
		case <-r.closenotifier: //if already closed no-op
		default:
			close(r.closenotifier)
		}
		r.closenotifiermutex.Unlock()
	}()
}

func (r *FailSilentRunner) Add(op Operator) {
	r.ops = append(r.ops, op)
}

func (r *FailSilentRunner) AsyncRunAll() {
	if len(r.ops) > 1 {
		for _, op := range r.ops[:len(r.ops)-1] {
			r.AsyncRun(op, false)
		}
	}
	if len(r.ops) > 0 {
		r.AsyncRun(r.ops[len(r.ops)-1], true)
	}
}

func (r *FailSilentRunner) HardStop() {
	for _, op := range r.ops {
		op.Stop()
	}

}
