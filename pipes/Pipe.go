package pipes

type Pipe[T any] interface {
	Send(item T)
}
