package mapper

import (
	"fmt"
	"testing"

	"github.com/cevian/go-stream/stream"
)

func TestStoppedWhileSendingOutput(t *testing.T) {
	fn := func(input stream.Object, out Outputer) error {
		out.Out(1) <- input
		return nil
	}

	passThru := NewOp(fn, "TestPassthruOp")

	inch := make(chan stream.Object, 1)
	outch := make(chan stream.Object, 1)

	passThru.SetIn(inch)
	passThru.SetOut(outch)

	run := stream.NewRunner()
	run.Add(passThru)
	run.AsyncRunAll()

	inch <- 1
	inch <- 2

	fmt.Println("Calling Stop")
	passThru.Stop()

	run.Wait()
}

func TestStoppedWhileSendingSendInt(t *testing.T) {
	fn := func(input stream.Object, out Outputer) error {
		out.Sending(1).Send(input)
		return nil
	}

	passThru := NewOp(fn, "TestPassthruOp")

	inch := make(chan stream.Object, 1)
	outch := make(chan stream.Object, 1)

	passThru.SetIn(inch)
	passThru.SetOut(outch)

	run := stream.NewRunner()
	run.Add(passThru)
	run.AsyncRunAll()

	inch <- 1
	inch <- 2

	fmt.Println("Calling Stop")
	passThru.Stop()

	run.Wait()
}
