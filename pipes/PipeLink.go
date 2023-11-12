package pipes

type PipeLink[T any] struct {
	receiver func(item T)
	filter   func(item T) bool
	next     func(item T) Pipe[T]
}

func (p *PipeLink[T]) Send(item T) {
	if p.filter(item) {
		p.receiver(item)
		return
	}

	p.SendNext(item)
}

func (p *PipeLink[T]) SendNext(item T) {
	if p.next == nil {
		return
	}

	next := p.next(item)
	if next != nil {
		next.Send(item)
	}
}
