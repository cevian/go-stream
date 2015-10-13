package stream

// ChannelFanoutOperator does a fanout of info across a set of downstream channels.
// acts synchronously in that it will write to all downstreams before reading the next upstream channel.
type ChannelFanoutOperator struct {
	*HardStopChannelCloser
	*BaseIn
	outputs []chan Object
	err     error
}

func NewChannelFanoutOp() *ChannelFanoutOperator {
	op := &ChannelFanoutOperator{NewHardStopChannelCloser(), NewBaseIn(CHAN_SLACK), make([]chan Object, 0), nil}
	return op
}

func (op *ChannelFanoutOperator) Add(newChannel chan Object) {
	op.outputs = append(op.outputs, newChannel)
}

func (op *ChannelFanoutOperator) Run() error {
	defer func() {
		for _, out := range op.outputs {
			close(out)
		}
	}()

	for {
		select {
		case obj, ok := <-op.In():
			if ok {
				for _, out := range op.outputs {
					select {
					case out <- obj:
					case <-op.StopNotifier:
						return nil
					}
				}
			} else {
				return nil
			}
		case <-op.StopNotifier:
			return nil
		}
	}
}
