package compress

import (
	"fmt"

	"github.com/cevian/go-stream/stream"
	"github.com/cevian/go-stream/stream/mapper"
	"github.com/golang/snappy"
)

func NewSnappyEncodeOp() stream.Operator {
	name := "SnappyEncodeOp"
	generator := func() mapper.Worker {
		fn := func(obj stream.Object, out mapper.Outputer) error {
			compressed := snappy.Encode(nil, obj.([]byte))
			out.Sending(1).Send(compressed)
			return nil
		}
		return mapper.NewWorker(fn, name)
	}
	return mapper.NewClosureOp(generator, nil, name)
}

func NewSnappyDecodeOp() stream.Operator {
	name := "SnappyDecodeOp"
	generator := func() mapper.Worker {
		fn := func(obj stream.Object, out mapper.Outputer) error {
			decompressed, err := snappy.Decode(nil, obj.([]byte))
			if err != nil {
				return fmt.Errorf("Error in snappy decompression %v", err)
			}
			out.Sending(1).Send(decompressed)
			return nil
		}
		return mapper.NewWorker(fn, name)
	}
	return mapper.NewClosureOp(generator, nil, name)
}
