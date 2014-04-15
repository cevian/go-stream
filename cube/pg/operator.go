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

/*
type FTCubeDimensions struct {
	//source cube.TimeDimension `db:"d1"`
	source cube.StringDimension()
	D2 cube.IntDimension  `db:"d2"`
}

type FTCubeAggregates struct {
}

func NewTestCube() *cube.Cube {
	return cube.NewCube(FTCubeDimensions{}, FTCubeAggregates{})
}*/

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
	source_vtablename := fmt.Sprintf("%s_Source_Vectors", tableName)
	s_vect_table := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s(source_id bigserial, source_description VARCHAR(255),s_offset bigserial, PRIMARY KEY(source_id));",
		source_vtablename)
	exec.Exec(s_vect_table)

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
			//var count int
			row = ""
			//length := len(flushed.SourceMap)
			for s_ident, offset := range flushed.SourceMap {

				row_update := fmt.Sprintf("WITH upsert AS (UPDATE %s SET s_offset='%d', source_description='%s' WHERE source_id='%d' RETURNING *) ",
					source_vtablename, offset, s_ident.Description, s_ident.ID)
				row_insert := fmt.Sprintf("INSERT INTO %s(source_id, source_description, s_offset) SELECT '%d', '%s', '%d' WHERE NOT EXISTS (SELECT * FROM upsert)\n",
					source_vtablename, s_ident.ID, s_ident.Description, offset)
				/*if count == length-2 {
					row = row + ","
				}*/
				row = row + row_update + row_insert
				//fmt.Println(row)
			}
			if row != "" {
				//fmt.Println(row)

				query := row
				fmt.Println("FT Query: ", query)
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
