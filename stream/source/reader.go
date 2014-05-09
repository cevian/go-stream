package source

import (
	"bufio"
	"encoding/binary"
	//"errors"
	//"fmt"
	"github.com/cevian/go-stream/stream"
	"github.com/cevian/go-stream/util/slog"
	"io"
	"math"
	"strconv"
	"time"
)

type NextReader interface {

	//ReadNext should/can block until Stop Called
	ReadNext() (next []byte, is_eof bool, err error)
	Seek(offset int64) (ret int64, err error)
	Stop()
}

// note that IONextReader will always close the underlying reader when exiting.
// It must do that to exit the ReadNext blocking call
type IONextReader struct {
	reader      io.ReadCloser
	bufReader   *bufio.Reader //can also use bufio.Scanner
	LengthDelim bool
}

func (rn IONextReader) Stop() {
	rn.reader.Close()
}

/*Only use for FT*/
func (rn IONextReader) Seek(offset int64) (ret int64, err error) {

	f, ok := rn.reader.(io.ReadSeeker)
	if ok {
		ret, err := f.Seek(offset, 0)
		return ret, err
	} else {
		panic("Unseekable file")
	}

	return -1, nil

}

func (rn IONextReader) ReadNext() (next []byte, is_eof bool, err error) {
	if rn.LengthDelim {
		var length uint32
		err := binary.Read(rn.bufReader, binary.LittleEndian, &length)
		if err == io.EOF {
			return nil, true, nil
		} else if err != nil {
			//log.Println("Got error in readNexter,", err) //this may not be an error but a valid Stop
			return nil, false, err
		}

		b := make([]byte, length)

		read_len := 0
		for read_len < int(length) {
			n, err := rn.bufReader.Read(b[read_len:])
			//fmt.Println("Length in ReadNext()", read_len)
			read_len += n
			if err != nil {
				return nil, false, nil
			}
		}
		return b, false, err
	} else {

		b, err := rn.bufReader.ReadBytes('\n')
		if err == io.EOF {
			return b, true, nil
		}
		return b, false, err
	}
}

func NewIOReaderWrapper(r io.ReadCloser) NextReader {
	return IONextReader{r, bufio.NewReader(r), false}
}

func NewIOReaderWrapperLengthDelim(r io.ReadCloser) NextReader {
	return IONextReader{r, bufio.NewReader(r), true}
}

type NextReaderSource struct {
	*stream.HardStopChannelCloser
	*stream.BaseOut
	readnexter    NextReader
	MaxItems      uint32
	ProduceOffset bool //whether it provides an offset

}

type FTNextReaderSource struct {
	NextReaderSource
	SourceDescription string
	SourceID          int64
	AckChan           chan stream.Object
}

func NewIOReaderSource(reader io.ReadCloser) Sourcer {
	rn := NewIOReaderWrapper(reader)
	return NewNextReaderSource(rn)
}

func NewFTReaderSource(reader io.ReadCloser, ackchan chan stream.Object, source_name string, source_id int64) Sourcer {

	_, ok := reader.(io.ReadSeeker)
	if !ok {
		panic("Cannot be an FT Source because we can't Seek")

	}

	rn := NewIOReaderWrapper(reader)
	return NewNextFTReaderSourceMax(rn, ackchan, math.MaxUint32, source_name, source_id)

}

func NewIOReaderSourceLengthDelim(reader io.ReadCloser) Sourcer {
	rn := NewIOReaderWrapperLengthDelim(reader)
	return NewNextReaderSource(rn)
}

func NewNextReaderSource(reader NextReader) Sourcer {

	return NewNextReaderSourceMax(reader, math.MaxUint32)
}

func NewNextFTReaderSourceMax(reader NextReader, ackchan chan stream.Object, max uint32, source_name string, source_id int64) Sourcer {

	hcc := stream.NewHardStopChannelCloser()
	o := stream.NewBaseOut(stream.CHAN_SLACK)
	nnrs := NextReaderSource{hcc, o, reader, max, true}
	nrs := FTNextReaderSource{nnrs, source_name, source_id, ackchan}
	return &nrs
}

func NewNextReaderSourceMax(reader NextReader, max uint32) Sourcer {

	hcc := stream.NewHardStopChannelCloser()
	o := stream.NewBaseOut(stream.CHAN_SLACK)
	nrs := NextReaderSource{hcc, o, reader, max, false}

	return &nrs
}

func (src *NextReaderSource) Stop() error {
	close(src.StopNotifier)
	src.readnexter.Stop()
	return nil
}

type FTData struct {
	Data              interface{}
	Offset            uint32
	SourceID          int64
	SourceDescription string
}

func (src *FTNextReaderSource) Run() error {

	defer src.CloseOutput()
	var count, off uint32
	//here's where the recovery protocol comes in

	//send a reset packet
	t0 := time.Now()
	src.Out() <- stream.FTReset{Source_id: src.SourceID, Reason: "Source Restart"}
	yas := <-src.AckChan
	offset, err := strconv.ParseInt(string(yas.([]byte)), 10, 0)
	if err != nil {
		panic("offset not a number")
	}
	src.NextReaderSource.readnexter.Seek(offset)
	t1 := time.Now()
	slog.Debugf("The call took %v to run.\n", t1.Sub(t0))
	off, count = 0, 0
	slog.Debugf("Reading up to %d %s", src.MaxItems, " tuples")
	for {

		//if I've been stopped, exit no matter what I've read
		select {
		case <-src.StopNotifier:
			//In this case readNexter was stopped
			return nil
		case obj, ok := <-src.AckChan:
			slog.Debugf("Received an ack in source")
			if ok {

				r, k := obj.(stream.FTResponder)
				if k {

					if r.Target() == src.SourceID {
						_, err := src.NextReaderSource.readnexter.Seek(r.Offset())

						if err != nil {
							panic("could not seek")
						}
					}

				} else {
					panic("debug")
				}

			}
		default:
		}
		b, eofReached, err := src.readnexter.ReadNext()
		if err != nil {
			slog.Errorf("Reader encountered error %v", err)
			src.readnexter.Stop()
			return err
		} else if len(b) > 0 {
			count++
			off += count * uint32(len(b))

			//fmt.Println("Count in Run", count)
			src.Out() <- FTData{Data: b, Offset: off, SourceID: src.SourceID, SourceDescription: src.SourceDescription}

		}
		if eofReached || (count >= src.MaxItems) {
			slog.Debugf("Got eof in Next Reader Source %d, %d", count, src.MaxItems)
			src.readnexter.Stop()
			return nil
		}
	}

}

func (src *NextReaderSource) Run() error {
	//This operator always stops the read nexter before exiting.
	//But can't defer here since in the case of a hardstop readnexter.Stop() was already called

	defer src.CloseOutput()
	var count uint32
	//here's where the recovery protocol comes in
	count = 0
	slog.Debugf("Reading up to %d %s", src.MaxItems, " tuples")
	for {
		b, eofReached, err := src.readnexter.ReadNext()
		//if I've been stopped, exit no matter what I've read
		select {
		case <-src.StopNotifier:
			//In this case readNexter was stopped
			return nil

		default:
		}
		if err != nil {
			slog.Errorf("Reader encountered error %v", err)
			src.readnexter.Stop()
			return err
		} else if len(b) > 0 {
			count++

			src.Out() <- b

		}
		if eofReached || (count >= src.MaxItems) {
			slog.Debugf("Got eof in Next Reader Source %d, %d", count, src.MaxItems)
			src.readnexter.Stop()
			return nil
		}
	}

}
