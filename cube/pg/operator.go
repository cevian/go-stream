package pg

import (
	"database/sql"
	"fmt"
	"github.com/cevian/go-stream/cube"
	"github.com/cevian/go-stream/stream"
	"github.com/cevian/go-stream/stream/mapper"
	"github.com/cevian/go-stream/util/slog"
	"log"
)

func NewFTUpsertOp(dbconnect string, tableName string, cd cube.CubeDescriber) (stream.Operator, stream.ProcessedNotifier, *Executor) {
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

	exec.CreateBaseTable()

	exec.Exec("CREATE TABLE IF NOT EXISTS Source_Vectors (source_id bigserial, source_description VARCHAR(255),s_offset bigserial, PRIMARY KEY(source_id) );")

	ready := stream.NewNonBlockingProcessedNotifier(2)

	f := func(input stream.Object, out mapper.Outputer) {
		//in, ok := input.(*cube.TimeRepartitionedCube)
		//ok := true
		flushed, ok := input.(cube.FT_Flush)

		fmt.Println(flushed.SourceMap)
		// wrap with transactions
		if ok {
			in := flushed.Cont
			tx, err := exec.conn.Begin()
			if err != nil {
				slog.Fatalf("Error starting transaction %v", err)
			}
			visitor := func(part cube.Partition, c cube.Cuber) {
				//exec.UpsertCube(part, c)
				exec.FTUpsertCube(part, c)
			}
			in.VisitPartitions(visitor)
			//update here
			var row string
			var count int
			row = ""
			length := len(flushed.SourceMap)
			for s_ident, offset := range flushed.SourceMap {

				row = fmt.Sprintf("('%d', '%s', '%d')", s_ident.ID, s_ident.Description, offset)
				if count == length-2 {
					row = row + ","
				}
				//fmt.Println(row)
			}
			if row != "" {
				fmt.Println(row)

				query := fmt.Sprintf("INSERT INTO Source_Vectors(source_id, source_description, s_offset) VALUES %s;", row)
				fmt.Println(query)
				exec.Exec(query)
			}

			//fmt.Println(in.Sourcemap)
			//commit
			err = tx.Commit()
			if err != nil {
				slog.Fatalf("Error Committing tx %v ", err)
			}
			ready.Notify(1)
		}
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
func NewUpsertOp(dbconnect string, tableName string, cd cube.CubeDescriber) (stream.Operator, stream.ProcessedNotifier, *Executor) {
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
