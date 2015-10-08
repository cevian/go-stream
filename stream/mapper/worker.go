package mapper

import (
	"github.com/cevian/go-stream/stream"
	"github.com/cevian/go-stream/util/slog"
	//"reflect"
)

type Worker interface {
	Map(input stream.Object, out Outputer) error
	Validate(inCh chan stream.Object, typeName string) bool
}

func NewWorker(mapCallback func(obj stream.Object, out Outputer) error, typename string) *EfficientWorker {
	return &EfficientWorker{MapCallback: mapCallback, typename: typename}
}

type EfficientWorker struct {
	MapCallback   func(obj stream.Object, out Outputer) error
	CloseCallback func(out Outputer) error //on soft close, can output some final stuff
	StopCallback  func()                   //on hard close only
	ExitCallback  func()                   //on soft or hard close
	outCh         chan stream.Object
	typename      string
}

func (w *EfficientWorker) Start(out chan stream.Object) {
	w.outCh = out
}

func (w *EfficientWorker) Close(out Outputer) error {
	if w.CloseCallback != nil {
		return w.CloseCallback(out)
	}
	return nil
}

func (w *EfficientWorker) Stop() {
	if w.StopCallback != nil {
		w.StopCallback()
	}
}

func (w *EfficientWorker) Exit() {
	if w.ExitCallback != nil {
		w.ExitCallback()
	}
}

func (w *EfficientWorker) Map(input stream.Object, out Outputer) error {
	return w.MapCallback(input, out)
}

func (w *EfficientWorker) Validate(inCh chan stream.Object, typeName string) bool {
	slog.Infof("Checking %s", typeName)
	return true
}
