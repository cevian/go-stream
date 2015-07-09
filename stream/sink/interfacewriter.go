package sink

import "github.com/cevian/go-stream/stream"

type InterfaceWriter interface {
	Write(i interface{}) error
}

type InterfaceCloser interface {
	Close()
}

type InterfaceWriterSink struct {
	*stream.HardStopChannelCloser
	*stream.BaseIn
	writer InterfaceWriter
}

func (sink InterfaceWriterSink) Run() error {
	for {
		select {
		case msg, ok := <-sink.In():
			if ok {
				if err := sink.writer.Write(msg); err != nil {
					return err
				}
			} else {
				if cl, ok := sink.writer.(InterfaceCloser); ok {
					cl.Close()
				}
				return nil
			}
		case <-sink.StopNotifier:
			return nil
		}

	}

}

func NewInterfaceWriterSink(writer InterfaceWriter) Sinker {
	iws := InterfaceWriterSink{stream.NewHardStopChannelCloser(), stream.NewBaseIn(stream.CHAN_SLACK), writer}
	return iws
}
