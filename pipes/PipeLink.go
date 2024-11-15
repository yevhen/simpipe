package pipes

type PipeLink[T any] struct {
	receiver func(item T)
	filter   func(item T) bool
	next     func(item T) Pipe[T]
}

func (link *PipeLink[T]) Send(item T) {
	if link.filter(item) {
		link.receiver(item)
		return
	}

	link.SendNext(item)
}

func (link *PipeLink[T]) SendNext(item T) {
	if link.next == nil {
		return
	}

	next := link.next(item)
	if next != nil {
		next.Send(item)
	}
}
