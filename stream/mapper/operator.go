package mapper

import "runtime"
import "sync"
import "github.com/cevian/go-stream/stream"

func NewOp(mapCallback func(obj stream.Object, out Outputer), tn string) *Op {
	gen := NewGenerator(mapCallback, tn)
	return NewOpFromGenerator(gen, tn)
}

func NewOpExitor(mapCallback func(obj stream.Object, out Outputer),
	exitCallback func(),
	tn string) *Op {

	gen := NewGenerator(mapCallback, tn)
	gen.SingleExitCallback = exitCallback
	return NewOpFromGenerator(gen, tn)
}

func NewClosureOp(createWorker func() Worker,
	singleExitCallback func(),
	tn string) *Op {

	gen := ClosureGenerator{createWorker, singleExitCallback, tn}
	return NewOpFromGenerator(&gen, tn)
}

func NewOpFromGenerator(gen Generator, tn string) *Op {
	base := stream.NewBaseInOutOp(stream.CHAN_SLACK)
	op := Op{base, NewConcurrentErrorHandler(), gen, tn, true, 0}
	op.Init()
	return &op
}

type Closer interface {
	Close(out Outputer) //happens on worker for soft close only
}

type Stopper interface {
	Stop() //happens on hard close on worker
}

type Exitor interface {
	Exit() //happens on worker or generator. Occurs on either hard or soft close
}

type Op struct {
	*stream.BaseInOutOp
	*ConcurrentErrorHandler
	Gen        Generator
	Typename   string
	Parallel   bool
	MaxWorkers int
}

func (o *Op) Init() bool {
	w := o.Gen.GetWorker()
	return w.Validate(o.In(), o.Typename)
}

func (o *Op) IsParallel() bool {
	return o.Parallel
}

func (o *Op) IsOrdered() bool {
	return false
}

func (o *Op) MakeOrdered() stream.ParallelizableOperator {
	return NewOrderedOpWrapper(o)
}

func (o *Op) SetParallel(flag bool) *Op {
	o.Parallel = flag
	return o
}

func (op *Op) Stop() error {
	err := op.BaseInOutOp.Stop()

	/* TODO: deprecate */
	/* hack to prevent blocking on output when stop is called */
	/* important with Out() interface of Outputer solved with the */
	/* Sending(x).Send() interface. Deprecate after Out() retired. */
	go func() {
		for _ = range op.Out() {
		}
	}()
	return err
}

func (o *Op) String() string {
	return o.Typename
}

func (o *Op) WorkerStop(worker Worker) {
	stopper, ok := worker.(Stopper)
	if ok {
		stopper.Stop()
	}
	exitor, ok := worker.(Exitor)
	if ok {
		exitor.Exit()
	}
}

func (o *Op) WorkerClose(worker Worker, outputer Outputer) {
	closer, ok := worker.(Closer)
	if ok {
		closer.Close(outputer)
	}
	exitor, ok := worker.(Exitor)
	if ok {
		exitor.Exit()
	}
}

func (o *Op) runWorker(worker Worker, outCh chan stream.Object) {
	outputer := NewSimpleOutputer(outCh, o.StopNotifier)
	for {
		select {
		case obj, ok := <-o.In():
			if ok {
				worker.Map(obj, outputer)
				if outputer.HasError() {
					o.SetError(outputer.Error())
					return
				}
			} else {
				o.WorkerClose(worker, outputer)
				if outputer.HasError() {
					o.SetError(outputer.Error())
				}
				return
			}
		case <-o.StopNotifier:
			o.WorkerStop(worker)
			return
		}
	}
}

func (o *Op) Exit() {
	exitor, ok := o.Gen.(Exitor)
	if ok {
		exitor.Exit()
	}
}

func (o *Op) Run() error {
	defer o.CloseOutput()
	//perform some validation
	//Processor.Validate()

	maxWorkers := o.MaxWorkers
	if !o.Parallel {
		maxWorkers = 1
	} else if o.MaxWorkers == 0 {
		maxWorkers = runtime.NumCPU()
	}

	println("Starting ", maxWorkers, " workers for ", o.String())
	opwg := sync.WaitGroup{}
	opwg.Add(maxWorkers)

	for wid := 0; wid < maxWorkers; wid++ {
		worker := o.Gen.GetWorker()
		go func() {
			defer opwg.Done()
			o.runWorker(worker, o.Out())
		}()
	}
	opwg.Wait()
	o.Exit()
	//stop or close here?
	return o.Error()
}
