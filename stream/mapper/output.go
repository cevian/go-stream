package mapper

import "github.com/cevian/go-stream/stream"

type Outputer interface {
	Out(int) chan<- stream.Object
	SetError(err error)
	HasError() bool
	Error() error
}

type SimpleOutputer struct {
	*ConcurrentErrorHandler
	ch chan<- stream.Object
}

func (o *SimpleOutputer) Out(num int) chan<- stream.Object {
	return o.ch
}

func NewSimpleOutputer(ch chan<- stream.Object) Outputer {
	return &SimpleOutputer{NewConcurrentErrorHandler(), ch}
}

type ConcurrentErrorHandler struct {
	errCh chan error
}

func NewConcurrentErrorHandler() *ConcurrentErrorHandler {
	return &ConcurrentErrorHandler{make(chan error, 1)}
}

func (o *ConcurrentErrorHandler) SetError(err error) {
	//non-block
	select {
	case o.errCh <- err:
	default:
	}
}

func (o *ConcurrentErrorHandler) HasError() bool {
	return len(o.errCh) > 0
}

func (o *ConcurrentErrorHandler) Error() error {
	//non-block
	select {
	case err := <-o.errCh:
		o.errCh <- err
		return err
	default:
		return nil
	}
}
