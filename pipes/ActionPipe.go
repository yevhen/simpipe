package pipes

import "simpipe/blocks"

type ActionPipe[T any] struct {
	in    chan T
	link  *PipeLink[T]
	block *blocks.ActionBlock[T]
}

func (p *ActionPipe[T]) Run() {
	p.block.Run()
}

func (p *ActionPipe[T]) Send(item T) {
	p.link.Send(item)
}

func (p *ActionPipe[T]) Close() {
	close(p.in)
}

func (p *ActionPipe[T]) LinkNext(pipe *ActionPipe[T]) {
	p.link.next = func(item T) Pipe[T] {
		return pipe
	}
}

func (p *ActionPipe[T]) Link(pipe *ActionPipe[T], when func(item T) bool) {
	prev := p.link.next
	p.link.next = func(item T) Pipe[T] {
		if when(item) {
			return pipe
		}
		return prev(item)
	}
}

func NewActionPipe[T any](
	capacity int,
	parallelism int,
	action func(item T),
) *ActionPipe[T] {
	filter := func(item T) bool {
		return true
	}

	next := func(item T) Pipe[T] {
		return nil
	}

	return CreateActionPipe(capacity, parallelism, action, filter, next)
}

func CreateActionPipe[T any](
	capacity int,
	parallelism int,
	action func(item T),
	filter func(item T) bool,
	next func(item T) Pipe[T],
) *ActionPipe[T] {
	input := make(chan T, capacity)

	pipe := &PipeLink[T]{
		receiver: func(item T) {
			input <- item
		},
		filter: filter,
		next:   next,
	}

	block := blocks.CreateActionBlock(input, pipe.SendNext, parallelism, action)

	return &ActionPipe[T]{
		in:    input,
		link:  pipe,
		block: block,
	}
}
