package mapper

//import "reflect"
import "github.com/cloudflare/go-stream/stream"

//import "log"

//import "encoding/json"

type Generator interface {
	GetWorker() Worker
}

func NewGenerator(mapCallback func(obj stream.Object, out Outputer), tn string) *SimpleGenerator {
	return &SimpleGenerator{MapCallback: mapCallback, typename: tn}
}

type SimpleGenerator struct {
	MapCallback        func(obj stream.Object, out Outputer)
	CloseCallback      func(out Outputer)
	StopCallback       func()
	WorkerExitCallback func() //called once per worker
	SingleExitCallback func() //called once per op
	typename           string
}

func (g *SimpleGenerator) GetWorker() Worker {
	w := NewWorker(g.MapCallback, g.typename)
	w.CloseCallback = g.CloseCallback
	w.StopCallback = g.StopCallback
	w.ExitCallback = g.WorkerExitCallback
	return w
}

func (w *SimpleGenerator) Exit() {
	if w.SingleExitCallback != nil {
		w.SingleExitCallback()
	}
}

type ClosureGenerator struct {
	createWorker       func() Worker
	singleExitCallback func()
	typename           string
}

func (w *ClosureGenerator) GetWorker() Worker {
	return w.createWorker()
}

func (w *ClosureGenerator) Exit() {
	if w.singleExitCallback != nil {
		w.singleExitCallback()
	}
}

/*type CallbackGenerator struct {
	callback     interface{}
	exitCallback func()
	typename     string
}

func (w *CallbackGenerator) GetWorker() Worker {
	direct, ok := w.callback.(func(obj stream.Object, out Outputer))
	if ok {
		return &EfficientWorker{callback: direct, typename: w.typename}
	}
	return &CallbackWorker{callback: reflect.ValueOf(w.callback), typename: w.typename}
}

func (w *CallbackGenerator) Exit() {
	if w.exitCallback != nil {
		w.exitCallback()
	}
}*/
/*
type WorkerFactoryGenerator struct {
	callback interface{}
}

func (w *WorkerFactoryGenerator) GetWorker() Worker {
	ret := reflect.ValueOf(w.callback).Call(nil)
	if len(ret) != 1 {
		log.Fatal("Cannot return more than one function in WorkerFactory")
	}
	direct, ok := ret[0].Elem().Interface().(func(obj stream.Object, out Outputer))
	if ok {
		return &EfficientWorker{callback: direct}
	}
	return &CallbackWorker{callback: ret[0].Elem()}
}

type WorkerCloserFactoryGenerator struct {
	callback interface{}
}

func (w *WorkerCloserFactoryGenerator) GetWorker() Worker {
	ret := reflect.ValueOf(w.callback).Call(nil)
	if len(ret) != 2 {
		log.Fatal("WorkerCloserFactory callback must return 2 function")
	}
	return &CallbackWorker{callback: ret[0].Elem(), closeCallback: ret[1].Interface().(func())}
}

type WorkerFinalItemsFactoryGenerator struct {
	callback interface{}
}

func (w *WorkerFinalItemsFactoryGenerator) GetWorker() Worker {
	ret := reflect.ValueOf(w.callback).Call(nil)
	if len(ret) != 2 {
		log.Fatal("WorkerCloserFactory callback must return 2 function")
	}
	fiCall := ret[1].Elem()
	return &CallbackWorker{callback: ret[0].Elem(), finalItemsCallback: &fiCall}
}*/
