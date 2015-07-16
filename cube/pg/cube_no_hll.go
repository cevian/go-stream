// +build !cgo

package pg

import "github.com/cevian/go-stream/cube"
import (
	"log"
	"reflect"
)

func getAggregatePgTypeVisit(fieldValue reflect.Value, fieldDescription reflect.StructField) AggregateColumn {
	name := fieldDescription.Name
	tagName := fieldDescription.Tag.Get("db")
	if tagName != "" {
		name = tagName
	}

	var ca *cube.CountAggregate
	switch da := fieldValue.Type(); da {
	default:
		log.Fatal("Unknown Aggregate type", da, "for field", name)
	case reflect.TypeOf(ca):

		tn := "INT"
		tagtype := fieldDescription.Tag.Get("dbtype")
		if tagtype != "" {
			tn = tagtype
		}

		return &CountCol{&IntCol{NewDefaultCol(name), tn}}
	}
	return nil

}
