package blocks

import (
	"time"
)

type BatchActionBlock[T any] struct {
	Input        <-chan T
	Done         func(item T)
	BatchSize    int
	FlushTimeout time.Duration
	Parallelism  int
	Action       func(batch []T)
	batches      chan []T
	batchBlock   *BatchBlock[T]
	actionBlock  *ActionBlock[[]T]
}

func (b *BatchActionBlock[T]) Run() {
	b.batchBlock.Run()
	b.actionBlock.Run()
}

func CreateBatchActionBlock[T any](
	in chan T,
	done func(item T),
	batchSize int,
	flushTimeout time.Duration,
	parallelism int,
	action func(batch []T),
) *BatchActionBlock[T] {

	var batches = make(chan []T)

	batchBlock := createInnerBatchBlock(batches, in, batchSize, flushTimeout)
	actionBlock := createInnerActionBlock(batches, done, parallelism, action)

	return &BatchActionBlock[T]{
		Input:        in,
		Done:         done,
		BatchSize:    batchSize,
		FlushTimeout: flushTimeout,
		Parallelism:  parallelism,
		Action:       action,
		batches:      batches,
		batchBlock:   batchBlock,
		actionBlock:  actionBlock,
	}
}

func createInnerBatchBlock[T any](batches chan []T, in chan T, batchSize int, flushTimeout time.Duration) *BatchBlock[T] {
	sendBatch := func(batch []T) {
		batches <- batch
	}
	return CreateBatchBlock(in, batchSize, flushTimeout, sendBatch)
}

func createInnerActionBlock[T any](batches chan []T, done func(item T), parallelism int, action func(batch []T)) *ActionBlock[[]T] {
	batchDone := func(batch []T) {
		for _, item := range batch {
			done(item)
		}
	}
	return CreateActionBlock(batches, batchDone, parallelism, action)
}

func RunBatchActionBlock[T any](
	in chan T,
	done func(item T),
	batchSize int,
	flushTimeout time.Duration,
	parallelism int,
	action func(batch []T),
) {
	block := CreateBatchActionBlock(in, done, batchSize, flushTimeout, parallelism, action)
	block.Run()
}
