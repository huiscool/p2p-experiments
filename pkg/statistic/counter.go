package statistic

type Counter int

func NewCounter() Counter {
	return Counter(0)
}

func (c Counter) AddOne() {
	c++
}

func (c Counter) Value() int {
	return int(c)
}
