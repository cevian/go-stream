package encoding

import (
	"encoding/json"
	"github.com/cloudflare/go-stream/stream"
	"github.com/cloudflare/go-stream/stream/mapper"
	"github.com/cloudflare/go-stream/util/slog"
	//"reflect"
)

/* Example Decoder Usage
decFn := func (in []byte, decoder func([]byte, interface{}) ) stream.Object{
	var i int
	decoder(in, &i)
	return i
}

intDecOp := encoding.NewJsonDecodeOp(decFn)
*/

func JsonGeneralDecoder() func([]byte, interface{}) {
	fn := func(input []byte, to_populate interface{}) {
		err := json.Unmarshal(input, to_populate)
		if err != nil {
			slog.Errorf("Error unmarshaling json: %v %v\n", err.Error(), string(input))
		}
	}
	return fn
}

func JsonGeneralEncoder() func(interface{}) ([]byte, error) {
	fn := func(input interface{}) ([]byte, error) {
		return json.Marshal(input)
	}
	return fn
}

func NewJsonDecodeOp(decFn func([]byte, func([]byte, interface{})) stream.Object) stream.InOutOperator {
	name := "JsonDecodeOp"
	workerCreator := func() mapper.Worker {
		decoder := JsonGeneralDecoder()
		fn := func(obj stream.Object, out mapper.Outputer) {
			decoded := decFn(obj.([]byte), decoder)
			out.Out(1) <- decoded
		}
		return mapper.NewWorker(fn, name)
	}
	return mapper.NewClosureOp(workerCreator, nil, name)
}

func NewJsonEncodeOp() stream.Operator {
	name := "JsonEncodeOp"
	workerCreator := func() mapper.Worker {
		fn := func(obj stream.Object, out mapper.Outputer) {
			res, err := json.Marshal(obj.([]byte))
			if err != nil {
				slog.Errorf("Error marshaling json %v\t%+v", err, obj)
			}
			out.Out(1) <- res
		}
		return mapper.NewWorker(fn, name)
	}

	return mapper.NewClosureOp(workerCreator, nil, name)
}
