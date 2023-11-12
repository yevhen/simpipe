package pipes

import (
	"simpipe/blocks"
	"time"
)

type Pipe[T any] interface {
	Send(item T)
}

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

	p.sendNext(item)
}

func (p *PipeLink[T]) sendNext(item T) {
	if p.next == nil {
		return
	}

	next := p.next(item)
	if next != nil {
		next.Send(item)
	}
}

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

	block := blocks.CreateActionBlock(input, pipe.sendNext, parallelism, action)

	return &ActionPipe[T]{
		in:    input,
		link:  pipe,
		block: block,
	}
}

type BatchActionPipe[T any] struct {
	in    chan T
	link  *PipeLink[T]
	block *blocks.BatchActionBlock[T]
}

func (p *BatchActionPipe[T]) Run() {
	p.block.Run()
}

func (p *BatchActionPipe[T]) Send(item T) {
	p.link.Send(item)
}

func (p *BatchActionPipe[T]) Close() {
	close(p.in)
}

func CreateBatchActionPipe[T any](
	capacity int,
	parallelism int,
	batchSize int,
	action func(items []T),
	filter func(item T) bool,
	next func(item T) Pipe[T],
) *BatchActionPipe[T] {
	input := make(chan T, capacity)

	pipe := &PipeLink[T]{
		receiver: func(item T) {
			input <- item
		},
		filter: filter,
		next:   next,
	}

	block := blocks.CreateBatchActionBlock(input, pipe.sendNext, batchSize, time.Hour, parallelism, action)

	return &BatchActionPipe[T]{
		in:    input,
		link:  pipe,
		block: block,
	}
}
