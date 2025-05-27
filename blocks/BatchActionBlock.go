package blocks

import (
	"time"
)

// BatchActionBlockOption defines a configuration option for a BatchActionBlock
type BatchActionBlockOption[T any] func(*BatchActionBlock[T])

// WithBatchActionParallelism returns an option that sets the parallelism level for the BatchActionBlock
func WithBatchActionParallelism[T any](parallelism int) BatchActionBlockOption[T] {
	return func(block *BatchActionBlock[T]) {
		block.Parallelism = parallelism
	}
}

// WithBatchActionDoneCallback returns an option that sets the callback function called after each item is processed
func WithBatchActionDoneCallback[T any](done func(item T)) BatchActionBlockOption[T] {
	return func(block *BatchActionBlock[T]) {
		block.Done = done
	}
}

// WithBatchActionBatchSize returns an option that sets the batch size for the BatchActionBlock
func WithBatchActionBatchSize[T any](batchSize int) BatchActionBlockOption[T] {
	return func(block *BatchActionBlock[T]) {
		block.BatchSize = batchSize
	}
}

// WithBatchActionFlushTimeout returns an option that sets the flush timeout for the BatchActionBlock
func WithBatchActionFlushTimeout[T any](timeout time.Duration) BatchActionBlockOption[T] {
	return func(block *BatchActionBlock[T]) {
		block.FlushTimeout = timeout
	}
}

// BatchActionBlock combines batching and action processing in a single block
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

// NewBatchActionBlock creates a new BatchActionBlock with the given input channel, action function, and options
func NewBatchActionBlock[T any](input chan T, action func(batch []T), options ...BatchActionBlockOption[T]) *BatchActionBlock[T] {
	block := &BatchActionBlock[T]{
		Input:        input,
		Action:       action,
		BatchSize:    100,             // Default batch size
		FlushTimeout: 1 * time.Second, // Default flush timeout
		Parallelism:  1,               // Default parallelism
		Done:         func(item T) {}, // Default no-op
	}

	for _, option := range options {
		option(block)
	}

	return block
}

// Run starts the BatchActionBlock processing
func (block *BatchActionBlock[T]) Run() {
	if block.batches != nil {
		panic("BatchActionBlock is already running")
	}

	block.batches = make(chan []T)

	sendBatch := func(batch []T) {
		block.batches <- batch
	}

	block.batchBlock = NewBatchBlock(block.Input,
		WithBatchSize[T](block.BatchSize),
		WithBatchFlushTimeout[T](block.FlushTimeout),
		WithBatchDoneCallback[T](sendBatch),
	)

	block.actionBlock = NewActionBlock(block.batches,
		block.Action,
		WithActionParallelism[[]T](block.Parallelism),
		WithActionDoneCallback[[]T](func(batch []T) {
			for _, item := range batch {
				block.Done(item)
			}
		}),
	)

	block.batchBlock.Run()
	block.actionBlock.Run()
}
