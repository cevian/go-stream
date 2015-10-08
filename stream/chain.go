package stream

import (
	"reflect"

	"github.com/cevian/go-stream/util/slog"
)

type Chain interface {
	Operators() []Operator
	Run() error
	Stop() error
	Add(o Operator) Chain
	SetName(string) Chain
	//NewSubChain creates a new empty
	//chain inheriting the properties of the parent chain
	//Usefull for distribute/fanout building functions
	NewSubChain() Chain
	//async functions
	Start() error
	Wait() error
}

/* A SimpleChain implements the operator interface too! */
type SimpleChain struct {
	runner Runner
	//	Ops         []Operator
	//	wg          *sync.WaitGroup
	//	closenotify chan bool
	//	closeerror  chan error
	sentstop bool
	Name     string
}

func NewChain() *SimpleChain {
	return NewSimpleChain()
}

func NewSimpleChain() *SimpleChain {
	c := &SimpleChain{runner: NewFailSilentRunner(), Name: "SimpleChain"}

	stopped := false
	opCloseHandler := func(op Operator, err error) {
		if !stopped {
			stopped = true
			if err != nil {
				slog.Warnf("Hard close of chain %s was triggered by op  (%v, %v). Error: %v", c.Name, op, reflect.TypeOf(op), err)
				c.Stop()
			} else {
				slog.Infof("Soft close of chain %s was triggered by op  (%v, %v).", c.Name, op, reflect.TypeOf(op))
				c.SoftStop()
			}
		}
	}

	c.runner.SetOpCloseHandler(opCloseHandler)

	return c
}

func (c *SimpleChain) Operators() []Operator {
	return c.runner.Operators()
}

func (c *SimpleChain) SetName(name string) Chain {
	c.Name = name
	c.runner.SetName(name)
	return c
}

func (c *SimpleChain) NewSubChain() Chain {
	return NewSimpleChain()
}

func (c *SimpleChain) Add(o Operator) Chain {
	ops := c.runner.Operators()
	opIn, isIn := o.(In)
	if isIn && len(ops) > 0 {
		slog.Infof("Setting input channel of %s", Name(o))
		last := ops[len(ops)-1]

		lastOutCh := last.(Out).Out()

		opIn.SetIn(lastOutCh)
	}

	out, ok := o.(Out)
	if ok {
		slog.Infof("Setting output channel of %s", Name(o))
		ch := make(chan Object, CHAN_SLACK)
		out.SetOut(ch)
	}

	c.runner.Add(o)
	return c
}

func (c *SimpleChain) Start() error {
	c.runner.AsyncRunAll()
	return nil
}

func (c *SimpleChain) SoftStop() error {
	if !c.sentstop {
		c.sentstop = true
		slog.Warnf("In soft close of chain %s", c.Name)
		ops := c.runner.Operators()
		ops[0].Stop()
	}
	return nil
}

/* A stop is a hard stop as per the Operator interface */
func (c *SimpleChain) Stop() error {
	if !c.sentstop {
		c.sentstop = true
		slog.Warnf("In hard close of chain %s", c.Name)
		c.runner.HardStop()
	}
	return nil
}

func (c *SimpleChain) Wait() error {
	slog.Infof("Waiting for runner to finish in chain %s", c.Name)
	err := c.runner.Wait()
	slog.Infof("Exiting chain %s with error: %v", c.Name, err)

	return err
}

/* Operator compatibility */
func (c *SimpleChain) Run() error {
	if err := c.Start(); err != nil {
		return err
	}
	return c.Wait()
}

type OrderedChain struct {
	*SimpleChain
}

func NewOrderedChain() *OrderedChain {
	return &OrderedChain{NewChain()}
}

func (c *OrderedChain) Add(o Operator) Chain {
	parallel, ok := o.(ParallelizableOperator)
	if ok && parallel.IsParallel() {
		if !parallel.IsOrdered() {
			parallel = parallel.MakeOrdered()
			if !parallel.IsOrdered() {
				slog.Fatalf("%s", "Couldn't make parallel operator ordered")
			}
		}
		c.SimpleChain.Add(parallel)
	} else {
		c.SimpleChain.Add(o)
	}
	return c
}

func (c *OrderedChain) NewSubChain() Chain {
	return NewOrderedChain()
}

type InChain interface {
	Chain
	In
}

type inChain struct {
	Chain
}

func NewInChainWrapper(c Chain) InChain {
	return &inChain{c}
}

func (c *inChain) In() chan Object {
	ops := c.Operators()
	return ops[0].(In).In()
}

func (c *inChain) GetInDepth() int {
	ops := c.Operators()
	return ops[0].(In).GetInDepth()
}

func (c *inChain) SetIn(ch chan Object) {
	ops := c.Operators()
	ops[0].(In).SetIn(ch)
}
