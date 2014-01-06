package compress

import (
	"code.google.com/p/snappy-go/snappy"
	"github.com/cevian/go-stream/stream"
	"github.com/cevian/go-stream/stream/mapper"
	"log"
)

func NewSnappyEncodeOp() stream.Operator {
	name := "SnappyEncodeOp"
	generator := func() mapper.Worker {
		fn := func(obj stream.Object, out mapper.Outputer) {
			compressed, err := snappy.Encode(nil, obj.([]byte))
			if err != nil {
				log.Printf("Error in snappy compression %v", err)
			}
			out.Out(1) <- compressed
		}
		return mapper.NewWorker(fn, name)
	}
	return mapper.NewClosureOp(generator, nil, name)
}

func NewSnappyDecodeOp() stream.Operator {
	name := "SnappyDecodeOp"
	generator := func() mapper.Worker {
		fn := func(obj stream.Object, out mapper.Outputer) {
			decompressed, err := snappy.Decode(nil, obj.([]byte))
			if err != nil {
				log.Printf("Error in snappy decompression %v", err)
			}
			out.Out(1) <- decompressed
		}
		return mapper.NewWorker(fn, name)
	}
	return mapper.NewClosureOp(generator, nil, name)
}
