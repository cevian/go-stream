package mapper

import "github.com/cevian/go-stream/stream"

/* there is 3 types of state:
1. operator state
2. worker state (per parrallel execution)
3. map callback state (per-tuple state)

Simple generators supports 1 and 3 but not 2
Closure generator supprts 1,2,3
*/

type Generator interface {
	GetWorker() Worker
}

func NewGenerator(mapCallback func(obj stream.Object, out Outputer) error, tn string) *SimpleGenerator {
	return &SimpleGenerator{MapCallback: mapCallback, typename: tn}
}

/* The simple generator can have state shared across workers of an op or state
that is unique to each tuple processed but not state that is unique to a worker */

type SimpleGenerator struct {
	MapCallback        func(obj stream.Object, out Outputer) error
	CloseCallback      func(out Outputer) error
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

/* Allow creation of new state per worker. Shared across map callbacks but
unique to the worker instead of the operator */

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
