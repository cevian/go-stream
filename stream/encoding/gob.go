package encoding

import (
	"bytes"
	"encoding/gob"
	"github.com/cevian/go-stream/stream"
	"github.com/cevian/go-stream/stream/mapper"
	"io"
	"log"
	//"reflect"
)

/* Example Decoder Usage
//intDecGenFn := func (obj stream.Object, out mapper.Outputter) {
//	decoder := encoding.GobGeneralDecoder()
//	return func(obj stream.Object, out mapper.Outputter) {
//		var i int
//		decoder(obj.([]byte), &i)
//		out.Out(1) <- i
//	}
//}

decFn := func (in []byte, decoder func([]byte, interface{}) ) stream.Object{
	var i int
	decoder(in, &i)
	return i
}

intDecOp := encoding.NewGobDecodeOp(decFn)
*/

func GobGeneralDecoder() func([]byte, interface{}) {
	/* Look at the notes for encoder since they are relevant here.
	   Basically this uses a model where each recieved input corresponds
	   to a separate gob stream */
	br := new(ByteReader)
	fn := func(input []byte, to_populate interface{}) {
		dec := gob.NewDecoder(br) //each input is an indy stream
		br.s = input
		br.i = 0
		err := dec.Decode(to_populate)
		if err != nil {
			log.Printf("Error unmarshaling gob: %v\n", err.Error())
		}
	}
	return fn
}

func NewGobDecodeOp(
	decFn func([]byte, func([]byte, interface{})) stream.Object) stream.InOutOperator {
	name := "GobDecodeOp"
	closure := func() mapper.Worker {
		decoder := GobGeneralDecoder()
		fn := func(obj stream.Object, out mapper.Outputer) {
			decoded := decFn(obj.([]byte), decoder)
			out.Out(1) <- decoded
		}
		return mapper.NewWorker(fn, name)
	}
	return mapper.NewClosureOp(closure, nil, name)
}

func NewGobEncodeOp() stream.InOutOperator {
	/* Each encoder provides a stateful stream. So we have to choices:
	   Either run this operator not in parallel and get  a stateful stream
	   Or run this in parallel but use a new encoder for each input. We
	   choose the latter but plan to buffer upstream so we get big streams
	   coming out. We will compress each output separately here.
	*/

	name := "GobEncodeOp"
	workerCreator := func() mapper.Worker {
		var buf bytes.Buffer
		fn := func(obj stream.Object, outputter mapper.Outputer) {
			enc := gob.NewEncoder(&buf) //each output is an indy stream
			err := enc.Encode(obj)
			if err != nil {
				log.Printf("Error marshaling gob: %v\n", err.Error())
			}
			n := buf.Len()
			out := make([]byte, n)
			if out == nil {
				log.Printf("Make failed")
			}
			newn, err := buf.Read(out)
			if newn != n || err != nil {
				if err == nil {
					log.Printf("Error marshaling gob on read: %v\t%v\n", newn, n)
				} else {
					log.Printf("Error marshaling gob on read: %v\t%v\t%v\n", newn, n, err.Error())
				}
			}

			outputter.Out(1) <- out
		}
		return mapper.NewWorker(fn, name)
	}

	op := mapper.NewClosureOp(workerCreator, nil, name)
	//op.Parallel = false
	return op
}

/////////////////////////////////////////////////// Misc ///////////////////////////////////////////////

//stolen from bytes.Reader. Slightly changed to allow us to reuse gob.Decoder on same object
type ByteReader struct {
	s []byte
	i int // current reading index
}

func (r *ByteReader) Read(b []byte) (n int, err error) {
	if len(b) == 0 {
		return 0, nil
	}
	if r.i >= len(r.s) {
		return 0, io.EOF
	}
	n = copy(b, r.s[r.i:])
	r.i += n
	return
}

///////////////////////////////////////////OLD STUFF///////////////////////////////////////////////////////////
/*
Works but we dont want to support transform functions that output []interface{}
func NewGobDecodeRopUnsafe(inch chan []byte, outch interface{}, typ reflect.Type) stream.Operator { //the chain constructor should decide on the format of outch
	br := ByteReader{}
	dec := gob.NewDecoder(&br)
	fn := func(in []byte, closenotifier chan<- bool) []interface{} {
		br.s = in
		br.i = 0
		obj := reflect.New(typ)
		err := dec.Decode(obj.Interface())
		if err != nil {
			log.Printf("Error unmarshaling gob: %v\n", err.Error())
		}
		return []interface{}{obj.Elem().Interface()}
	}
	return stream.NewReflectOp(fn, inch, outch, "GobDecodeRop")
}
*/
