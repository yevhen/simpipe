package blocks

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

	batch := block.buffer
	block.buffer = make([]T, 0, block.BatchSize)

	block.Done(batch)
}

func (block *BatchBlock[T]) restartTimer() {
	block.timer.Reset(block.FlushTimeout)
}
