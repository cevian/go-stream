package util

import (
	"log"
	"os"

	"github.com/cevian/go-stream/stream"
	"github.com/cevian/go-stream/stream/mapper"
)

func NewDropOp() *mapper.Op {
	dropfn := func(input stream.Object, out mapper.Outputer) {
	}

	return mapper.NewOp(dropfn, "DropOp")
}

func NewPassthruOp() *mapper.Op {
	fn := func(input stream.Object, out mapper.Outputer) {
		out.Sending(1).Send(input)
	}

	return mapper.NewOp(fn, "PassthruOp")
}

func NewTailDataOp() stream.Operator {
	name := "TailDropOp"
	createWorker := func() mapper.Worker {

		logger := log.New(os.Stdout, "", log.LstdFlags|log.Lshortfile)

		fn := func(input stream.Object, outputer mapper.Outputer) {

			if value, ok := input.([]byte); ok {
				logger.Printf("%s", string(value))
			} else if value, ok := input.(string); ok {
				logger.Printf("%s", string(value))
			} else {
				logger.Printf("%v", input)
			}

			outputer.Sending(1).Send(input)
		}

		return mapper.NewWorker(fn, name)
	}
	op := mapper.NewClosureOp(createWorker, nil, name)
	op.Parallel = false
	return op
}
