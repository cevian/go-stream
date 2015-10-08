package mapper

import "github.com/cevian/go-stream/stream"

type Sender interface {
	Send(stream.Object)
}

type Outputer interface {
	/* to be deprecated. Use Sending instead*/
	Out(int) chan<- stream.Object

	Sending(int) Sender
}

type SimpleOutputer struct {
	ch           chan<- stream.Object
	stopNotifier <-chan bool
}

func (o *SimpleOutputer) Out(num int) chan<- stream.Object {
	return o.ch
}

func (o *SimpleOutputer) Sending(num int) Sender {
	return o
}

func (o *SimpleOutputer) Send(rec stream.Object) {
	select {
	case o.ch <- rec:
	case <-o.stopNotifier:
	}
}

func NewSimpleOutputer(ch chan<- stream.Object, stopNotifier <-chan bool) Outputer {
	return &SimpleOutputer{ch, stopNotifier}
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
