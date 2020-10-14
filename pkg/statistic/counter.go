package statistic

import "sync/atomic"

type Counter struct {
	val int32
}

func NewCounter() *Counter {
	return &Counter{
		val: 0,
	}
}

func (c *Counter) AddOne() {
	atomic.AddInt32(&c.val, 1)
}

func (c *Counter) Value() int {
	return int(c.val)
}
