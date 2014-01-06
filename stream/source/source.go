package source

import "github.com/cevian/go-stream/stream"

type Sourcer interface {
	stream.Operator
	stream.Out
	SetCloseOnExit(flag bool)
}
