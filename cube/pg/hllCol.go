// +build cgo

package pg

import (
	"encoding/hex"
	//"database/sql"
	"fmt"
	"github.com/cevian/go-stream/cube"
)

// HLL code
type HllCol struct {
	*DefaultCol
	tn string
}

func (c *HllCol) TypeName() string {
	return "HLL"
}

// This guy is how the value is supposed to be printed out (for a COPY command to PG).
func (c *HllCol) PrintInterface(in interface{}) interface{} {
	if td, ok := in.(cube.HllAggregate); ok {
		defer td.Hll.Delete()
		return "\\\\x" + hex.EncodeToString(td.Hll.Serialize())
	} else if td, ok := in.(cube.HllDimension); ok {
		defer td.Hll.Delete()
		return "\\\\x" + hex.EncodeToString(td.Hll.Serialize())
	}
	return 0
}

func (c *HllCol) UpdateSql(intoTableName string, updateTableName string) string {
	cn := c.Name()
	return fmt.Sprintf("%s = %s.%s || %s.%s", cn, intoTableName, cn, updateTableName, cn)
}
