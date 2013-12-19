package mapper

import (
	"github.com/cloudflare/go-stream/stream"
)

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
