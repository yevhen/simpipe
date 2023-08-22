package simpipe

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestFlushesBySize(t *testing.T) {
	batches := make(chan []string)
	done := func(batch []string) {
		batches <- batch
	}

	in := make(chan string)
	go runBatchBlock(in, 2, time.Hour, done)

	in <- "bar"
	in <- "baz"

	batch := <-batches
	assert.Equal(t, "bar", batch[0])
	assert.Equal(t, "baz", batch[1])
}

func TestFlushesByTimer(t *testing.T) {
	batches := make(chan []string)
	done := func(batch []string) {
		batches <- batch
	}

	flushTimeout := 500 * time.Millisecond

	in := make(chan string)
	go runBatchBlock(in, 2, flushTimeout, done)

	in <- "bar"

	batch := <-batches
	assert.Equal(t, "bar", batch[0])
}

func TestDoesNotFlushesByTimerIfFlushedBySize(t *testing.T) {
	batches := make(chan []string, 2)
	done := func(batch []string) {
		batches <- batch
	}

	flushTimeout := time.Second

	in := make(chan string, 3)
	go runBatchBlock(in, 2, flushTimeout, done)

	in <- "foo"
	in <- "bar"
	in <- "baz"

	time.Sleep(flushTimeout + 500*time.Millisecond)

	batch := <-batches
	assert.Equal(t, "foo", batch[0])
	assert.Equal(t, "bar", batch[1])
	assert.Equal(t, 0, len(batches))
}

func TestFlushesOnChannelClose(t *testing.T) {
	batches := make(chan []string)
	done := func(batch []string) {
		batches <- batch
	}

	in := make(chan string)
	go runBatchBlock(in, 2, time.Hour, done)

	in <- "bar"
	close(in)

	batch := <-batches
	assert.Equal(t, "bar", batch[0])
}

type BatchBlock[T any] struct {
	Input        <-chan T
	Done         func(batch []T)
	BatchSize    int
	FlushTimeout time.Duration
	timer        *time.Ticker
	buffer       []T
}

func (block *BatchBlock[T]) Run() {
	go func() {
		for {
			select {
			case item, ok := <-block.Input:
				if !ok {
					if len(block.buffer) > 0 {
						block.flushBuffer()
						return
					}
				}
				block.buffer = append(block.buffer, item)
				if len(block.buffer) == block.BatchSize {
					block.flushBuffer()
					block.restartTimer()
				}
			case <-block.timer.C:
				if len(block.buffer) > 0 {
					block.flushBuffer()
				}
			}
		}
	}()
}

func (block *BatchBlock[T]) flushBuffer() {
	block.Done(block.buffer)
	block.buffer = block.buffer[:0]
}

func (block *BatchBlock[T]) restartTimer() {
	block.timer.Stop()
	<-block.timer.C
	block.timer.Reset(block.FlushTimeout)
}

func runBatchBlock[T any](in chan T, batchSize int, flushTimeout time.Duration, done func(batch []T)) {
	block := CreateBatchBlock(in, batchSize, flushTimeout, done)
	block.Run()
}

func CreateBatchBlock[T any](in chan T, batchSize int, flushTimeout time.Duration, done func(batch []T)) BatchBlock[T] {
	return BatchBlock[T]{
		Input:        in,
		Done:         done,
		BatchSize:    batchSize,
		FlushTimeout: flushTimeout,
		buffer:       make([]T, 0, batchSize),
		timer:        time.NewTicker(flushTimeout),
	}
}
