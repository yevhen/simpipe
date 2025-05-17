package blocks

import (
	"time"
)

type BatchActionBlock[T any] struct {
	Input        chan T
	Done         func(item T)
	BatchSize    int
	FlushTimeout time.Duration
	Parallelism  int
	Action       func(batch []T)
	batches      chan []T
	batchBlock   *BatchBlock[T]
	actionBlock  *ActionBlock[[]T]
}

func (block *BatchActionBlock[T]) Run() {
	if block.batches != nil {
		panic("BatchActionBlock is already running")
	}

	block.batches = make(chan []T)
	block.batchBlock = createInnerBatchBlock(block.batches, block.Input, block.BatchSize, block.FlushTimeout)
	block.actionBlock = createInnerActionBlock(block.batches, block.Done, block.Parallelism, block.Action)

	block.batchBlock.Run()
	block.actionBlock.Run()
}

func createInnerBatchBlock[T any](batches chan []T, in chan T, batchSize int, flushTimeout time.Duration) *BatchBlock[T] {
	sendBatch := func(batch []T) {
		batches <- batch
	}
	return &BatchBlock[T]{
		Input:        in,
		Done:         sendBatch,
		BatchSize:    batchSize,
		FlushTimeout: flushTimeout,
		buffer:       make([]T, 0, batchSize),
		timer:        time.NewTicker(flushTimeout),
	}
}

func createInnerActionBlock[T any](batches chan []T, done func(item T), parallelism int, action func(batch []T)) *ActionBlock[[]T] {
	batchDone := func(batch []T) {
		for _, item := range batch {
			done(item)
		}
	}

	return &ActionBlock[[]T]{
		Input:       batches,
		Done:        batchDone,
		Parallelism: parallelism,
		Action:      action,
	}
}
