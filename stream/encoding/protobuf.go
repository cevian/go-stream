package encoding

import (
	"code.google.com/p/goprotobuf/proto"
	"github.com/cevian/go-stream/stream"
	"github.com/cevian/go-stream/stream/mapper"
	"log"
	//"reflect"
)

/* Example Decoder Usage
decFn := func (in []byte, decoder func([]byte, proto.Message) ) stream.Object{
	var i <protobuf object>
	decoder(in, &i)
	return i
}

intDecOp := encoding.NewProtobufDecodeOp(decFn)
*/

func ProtobufGeneralDecoder() func([]byte, proto.Message) {
	fn := func(input []byte, to_populate proto.Message) {
		err := proto.Unmarshal(input, to_populate)
		if err != nil {
			log.Printf("Error unmarshaling protobuf: %v\n", err.Error())
		}
	}
	return fn
}

func NewProtobufDecodeOp(decFn func([]byte, func([]byte, proto.Message)) stream.Object) stream.InOutOperator {
	name := "ProtobufDecodeOp"
	workerCreator := func() mapper.Worker {
		decoder := ProtobufGeneralDecoder()
		fn := func(obj stream.Object, out mapper.Outputer) {
			decoded := decFn(obj.([]byte), decoder)
			out.Out(1) <- decoded
		}
		return mapper.NewWorker(fn, name)
	}
	return mapper.NewClosureOp(workerCreator, nil, name)
}

func NewProtobufEncodeOp() stream.Operator {
	name := "ProtobufEncodeOp"
	workerCreator := func() mapper.Worker {
		fn := func(obj stream.Object, outputer mapper.Outputer) {
			in := obj.(proto.Message)
			out, err := proto.Marshal(in)
			if err != nil {
				log.Printf("Error marshaling protobuf %v\t%#v", err, in)
			}
			outputer.Out(1) <- out
		}
		return mapper.NewWorker(fn, name)
	}

	return mapper.NewClosureOp(workerCreator, nil, name)
}

/*
func NewMakeProtobufMessageOp() stream.Operator {
	fn := func(in interface{}) []proto.Message {
		return []proto.Message{in.(proto.Message)}
	}

	return mapper.NewOp(fn, "MakeProtobufMessageOp")
}*/
