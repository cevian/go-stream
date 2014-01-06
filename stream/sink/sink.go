package sink

import (
	"github.com/cevian/go-stream/stream"
)

type Sinker interface {
	stream.Operator
	stream.In
}
