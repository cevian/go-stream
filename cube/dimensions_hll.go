// +build cgo

package cube

import (
	"github.com/cevian/go-stream/cube/pg/hll"
)

type HllDimension struct {
	Hll *hll.Hll
}

func NewHllDimension(i *hll.Hll) *HllDimension {
	return &HllDimension{Hll: i}
}
