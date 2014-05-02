package mapper

import (
	"github.com/cevian/go-stream/stream"
	"github.com/cevian/go-stream/util/slog"
	//"reflect"
)

type Worker interface {
	Map(input stream.Object, out Outputer)
	Validate(inCh chan stream.Object, typeName string) bool
	//to handle a reset tuple
	MapReset(obj stream.FTResetter, out Outputer)
}

func NewWorker(mapCallback func(obj stream.Object, out Outputer), typename string) *EfficientWorker {
	resetcall := func(obj stream.FTResetter, out Outputer) {

		slog.Debugf("Reset Flush Cause in " + typename + ": " + obj.Cause())
		out.Out(1) <- obj

	}

	return &EfficientWorker{MapCallback: mapCallback, ResetCallback: resetcall, typename: typename}
}

type EfficientWorker struct {
	MapCallback   func(obj stream.Object, out Outputer)
	CloseCallback func(out Outputer) //on soft close, can output some final stuff
	StopCallback  func()             //on hard close only
	ExitCallback  func()             //on soft or hard close
	ResetCallback func(obj stream.FTResetter, out Outputer)
	outCh         chan stream.Object
	typename      string
}

func (w *EfficientWorker) Start(out chan stream.Object) {
	w.outCh = out
}

func (w *EfficientWorker) Close(out Outputer) {
	if w.CloseCallback != nil {
		w.CloseCallback(out)
	}
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

func (w *EfficientWorker) Map(input stream.Object, out Outputer) {
	w.MapCallback(input, out)
}
func (w *EfficientWorker) MapReset(input stream.FTResetter, out Outputer) {
	w.ResetCallback(input, out)
}

func (w *EfficientWorker) Validate(inCh chan stream.Object, typeName string) bool {
	slog.Infof("Checking %s", typeName)
	return true
}
