package cube

import (
	"time"
)

type Dimension interface{}

type TimeDimension time.Time

func NewTimeDimension(t time.Time) *TimeDimension {
	ret := TimeDimension(t)
	return &ret
}

func (t *TimeDimension) Unix() int64 {
	return (*time.Time)(t).Unix()
}

func (t *TimeDimension) UnixNano() int64 {
	return (*time.Time)(t).UnixNano()
}

func (t *TimeDimension) Time() time.Time {
	return *(*time.Time)(t)
}

type IntDimension int

func NewIntDimension(i int) *IntDimension {
	ret := IntDimension(i)
	return &ret

}

type StringDimension string

func NewStringDimension(i string) *StringDimension {
	ret := StringDimension(i)
	return &ret

}
