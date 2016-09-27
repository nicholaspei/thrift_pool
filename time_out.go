package thrift_pool

type TimeOut struct {
	message string
}

func (this TimeOut) Error() string {
	return this.message
}
