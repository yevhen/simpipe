package simpipe

import (
	"time"
)

type BatchBlock[T any] struct {
	Input        <-chan T
	Done         func(batch []T)
	BatchSize    int
	FlushTimeout time.Duration
	timer        *time.Ticker
	buffer       []T
	batchFlushed bool
}

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

	batch := make([]T, len(block.buffer))
	copy(batch, block.buffer)

	block.Done(batch)
	block.buffer = block.buffer[:0]
}

func (block *BatchBlock[T]) restartTimer() {
	block.timer.Reset(block.FlushTimeout)
}

func CreateBatchBlock[T any](in chan T, batchSize int, flushTimeout time.Duration, done func(batch []T)) *BatchBlock[T] {
	return &BatchBlock[T]{
		Input:        in,
		Done:         done,
		BatchSize:    batchSize,
		FlushTimeout: flushTimeout,
		buffer:       make([]T, 0, batchSize),
		timer:        time.NewTicker(flushTimeout),
	}
}

func RunBatchBlock[T any](in chan T, batchSize int, flushTimeout time.Duration, done func(batch []T)) {
	block := CreateBatchBlock(in, batchSize, flushTimeout, done)
	block.Run()
}
