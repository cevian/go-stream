package util

import (
	"github.com/cevian/go-stream/stream"
	"github.com/cevian/go-stream/stream/mapper"
	"log"
	"os"
)

func NewDropOp() *mapper.Op {
	dropfn := func(input stream.Object, out mapper.Outputer) {
	}

	return mapper.NewOp(dropfn, "DropOp")
}

func NewPassthruOp() *mapper.Op {
	fn := func(input stream.Object, out mapper.Outputer) {
		out.Out(1) <- input
	}

	return mapper.NewOp(fn, "PassthruOp")
}

/*func NewMakeInterfaceOp() *mapper.Op {
	fn := func(in interface{}) []interface{} {
		return []interface{}{in}
	}

	return mapper.NewOp(fn, "MakeInterfaceOp")
}*/

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

			outputer.Out(1) <- input
		}

		return mapper.NewWorker(fn, name)
	}
	op := mapper.NewClosureOp(createWorker, nil, name)
	op.Parallel = false
	return op
}
