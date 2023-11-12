package pipes

import (
	"simpipe/blocks"
	"time"
)

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

	block := blocks.CreateBatchActionBlock(input, pipe.SendNext, batchSize, time.Hour, parallelism, action)

	return &BatchActionPipe[T]{
		in:    input,
		link:  pipe,
		block: block,
	}
}
