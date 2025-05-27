package blocks

import (
	"time"
)

// BatchBlockOption defines a configuration option for a BatchBlock
type BatchBlockOption[T any] func(*BatchBlock[T])

// WithBatchSize returns an option that sets the batch size for the BatchBlock
func WithBatchSize[T any](batchSize int) BatchBlockOption[T] {
	return func(block *BatchBlock[T]) {
		block.BatchSize = batchSize
		block.buffer = make([]T, 0, batchSize)
	}
}

// WithBatchFlushTimeout returns an option that sets the flush timeout for the BatchBlock
func WithBatchFlushTimeout[T any](timeout time.Duration) BatchBlockOption[T] {
	return func(block *BatchBlock[T]) {
		block.FlushTimeout = timeout
		if block.timer != nil {
			block.timer.Reset(timeout)
		} else {
			block.timer = time.NewTicker(timeout)
		}
	}
}

// WithBatchDoneCallback returns an option that sets the callback function called after a batch is processed
func WithBatchDoneCallback[T any](done func(batch []T)) BatchBlockOption[T] {
	return func(block *BatchBlock[T]) {
		block.Done = done
	}
}

// BatchBlock collects items into batches before processing
type BatchBlock[T any] struct {
	Input        <-chan T
	Done         func(batch []T)
	BatchSize    int
	FlushTimeout time.Duration
	timer        *time.Ticker
	buffer       []T
	batchFlushed bool
}

// NewBatchBlock creates a new BatchBlock with the given input channel and options
func NewBatchBlock[T any](input <-chan T, options ...BatchBlockOption[T]) *BatchBlock[T] {
	block := &BatchBlock[T]{
		Input:        input,
		BatchSize:    100,                // Default batch size
		FlushTimeout: 1 * time.Second,    // Default flush timeout
		Done:         func(batch []T) {}, // Default no-op
		batchFlushed: false,
	}

	block.buffer = make([]T, 0, block.BatchSize)
	block.timer = time.NewTicker(block.FlushTimeout)

	for _, option := range options {
		option(block)
	}

	return block
}

// Run starts the BatchBlock processing
func (block *BatchBlock[T]) Run() {
	go func() {
		for {
			select {
			case item, ok := <-block.Input:
				if !ok {
					block.flushBuffer()
					return
				}
				block.flushBySize(item)
			case <-block.timer.C:
				block.flushByTimer()
			}
		}
	}()
}

func (block *BatchBlock[T]) flushBySize(item T) {
	block.buffer = append(block.buffer, item)
	if len(block.buffer) == block.BatchSize {
		block.flushBuffer()
		block.restartTimer()
		block.batchFlushed = true
	}
}

func (block *BatchBlock[T]) flushByTimer() {
	if block.batchFlushed {
		block.batchFlushed = false
		return
	}

	block.flushBuffer()
}

func (block *BatchBlock[T]) flushBuffer() {
	if len(block.buffer) == 0 {
		return
	}

	batch := block.buffer
	block.buffer = make([]T, 0, block.BatchSize)

	block.Done(batch)
}

func (block *BatchBlock[T]) restartTimer() {
	block.timer.Reset(block.FlushTimeout)
}
