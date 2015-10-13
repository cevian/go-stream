package stream

type HardStopChannelCloser struct {
	StopNotifier chan bool
}

func (op *HardStopChannelCloser) Stop() error {
	//prevents double close
	select {
	case <-op.StopNotifier:
	default:
		close(op.StopNotifier)
	}
	return nil
}

func NewHardStopChannelCloser() *HardStopChannelCloser {
	return &HardStopChannelCloser{make(chan bool)}
}

type BaseIn struct {
	in chan Object
}

func (o *BaseIn) In() chan Object {
	return o.in
}

func (o *BaseIn) GetInDepth() int {
	return len(o.in)
}

func (o *BaseIn) SetIn(c chan Object) {
	o.in = c
}

func NewBaseIn(slack int) *BaseIn {
	return &BaseIn{make(chan Object, slack)}
}

type BaseOut struct {
	out         chan Object
	shouldClose bool
}

func (o *BaseOut) Out() chan Object {
	return o.out
}

func (o *BaseOut) SetOut(c chan Object) {
	o.out = c
	//the only real reason to set out is to have a shared output, in which case shouldn't close
	o.shouldClose = false
}

func (o *BaseOut) SetCloseOnExit(flag bool) {
	o.shouldClose = flag
}

func (o *BaseOut) CloseOutput() {
	if o.shouldClose {
		close(o.out)
	}
}

func NewBaseOut(slack int) *BaseOut {
	return &BaseOut{make(chan Object, slack), true}
}

type BaseInOutOp struct {
	*HardStopChannelCloser
	*BaseIn
	*BaseOut
}

func NewBaseInOutOp(slack int) *BaseInOutOp {
	obj := &BaseInOutOp{NewHardStopChannelCloser(), NewBaseIn(slack), NewBaseOut(slack)}
	return obj
}
