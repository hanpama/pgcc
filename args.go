package pgcc

// Args represents cursor connection field arguments
type Args []interface{}

// NewArgs creates new Args
func NewArgs(first *int32, after interface{}, last *int32, before interface{}, more ...interface{}) Args {
	return append(Args{first, after, last, before}, more...)
}

// SetFirst sets the query's parameter `first` as given value
func (a Args) SetFirst(count int32) { a[0] = &count }

// SetAfter sets the query's parameter `after` as given value
func (a Args) SetAfter(cursor interface{}) { a[1] = &cursor }

// SetLast sets the query's parameter `last` as given value
func (a Args) SetLast(count int32) { a[2] = &count }

// SetBefore sets the query's parameter `before` as given value
func (a Args) SetBefore(cursor interface{}) { a[3] = &cursor }
