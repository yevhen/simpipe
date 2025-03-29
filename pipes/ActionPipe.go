package pipes

import "simpipe/blocks"

type ActionPipe[T any] struct {
	in    chan T
	link  *PipeLink[T]
	block *blocks.ActionBlock[T]
}

func (pipe *ActionPipe[T]) Run() {
	pipe.block.Run()
}

func (pipe *ActionPipe[T]) Send(item T) {
	pipe.link.Send(item)
}

func (pipe *ActionPipe[T]) Close() {
	close(pipe.in)
}

func (pipe *ActionPipe[T]) LinkNext(next *ActionPipe[T]) {
	pipe.link.next = func(item T) Pipe[T] {
		return next
	}
}

func (pipe *ActionPipe[T]) Link(next *ActionPipe[T], when func(item T) bool) {
	prev := pipe.link.next
	pipe.link.next = func(item T) Pipe[T] {
		if when(item) {
			return next
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

	block := &blocks.ActionBlock[T]{
		Input:       input,
		Done:        pipe.SendNext,
		Parallelism: parallelism,
		Action:      action,
	}

	return &ActionPipe[T]{
		in:    input,
		link:  pipe,
		block: block,
	}
}
