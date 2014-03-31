package pg

import "github.com/cevian/go-stream/cube"
import "github.com/cevian/go-stream/stream"
import "github.com/cevian/go-stream/stream/mapper"

import (
	"database/sql"
	"log"
)

func NewUpsertOp(dbconnect string, tableName string, cd cube.CubeDescriber) 
	(stream.Operator, stream.ProcessedNotifier, *Executor) {
	db, err := sql.Open("postgres", dbconnect)
	if err != nil {
		log.Fatal(err)
	}
	drv := db.Driver()
	conn, err := drv.Open(dbconnect)
	if err != nil {
		log.Fatal(err)
	}

	table := MakeTable(tableName, cd)

	exec := NewExecutor(table, conn)

	//exec.CreateBaseTable()

	ready := stream.NewNonBlockingProcessedNotifier(2)

	f := func(input stream.Object, out mapper.Outputer) {
		in := input.(*cube.TimeRepartitionedCube)
		// wrap with transactions
		visitor := func(part cube.Partition, c cube.Cuber) {
			exec.UpsertCube(part, c)
		}
		in.VisitPartitions(visitor)
		//update here
		//commit
		ready.Notify(1)
	}

	exit := func() {
		log.Println("Db Upser Exit: ")
	}

	name := "DbUpsert"
	gen := mapper.NewGenerator(f, name)
	gen.SingleExitCallback = exit
	op := mapper.NewOpFromGenerator(gen, name)
	op.Parallel = false
	return op, ready, exec
}
