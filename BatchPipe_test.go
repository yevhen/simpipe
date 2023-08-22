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
	go createBatchPipe(in, 2, time.Hour, done)

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
	go createBatchPipe(in, 2, flushTimeout, done)

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
	go createBatchPipe(in, 2, flushTimeout, done)

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
	go createBatchPipe(in, 2, time.Hour, done)

	in <- "bar"
	close(in)

	batch := <-batches
	assert.Equal(t, "bar", batch[0])
}

type BatchPipe[T any] struct {
	Input        <-chan T
	Done         func(batch []T)
	BatchSize    int
	FlushTimeout time.Duration
	timer        *time.Ticker
	buffer       []T
}

func (pipe *BatchPipe[T]) Run() {
	go func() {
		for {
			select {
			case item, ok := <-pipe.Input:
				if !ok {
					if len(pipe.buffer) > 0 {
						pipe.flushBuffer()
						return
					}
				}
				pipe.buffer = append(pipe.buffer, item)
				if len(pipe.buffer) == pipe.BatchSize {
					pipe.flushBuffer()
					pipe.restartTimer()
				}
			case <-pipe.timer.C:
				if len(pipe.buffer) > 0 {
					pipe.flushBuffer()
				}
			}
		}
	}()
}

func (pipe *BatchPipe[T]) flushBuffer() {
	pipe.Done(pipe.buffer)
	pipe.buffer = pipe.buffer[:0]
}

func (pipe *BatchPipe[T]) restartTimer() {
	pipe.timer.Stop()
	<-pipe.timer.C
	pipe.timer.Reset(pipe.FlushTimeout)
}

func createBatchPipe[T any](in chan T, batchSize int, flushTimeout time.Duration, done func(batch []T)) {
	pipe := BatchPipe[T]{
		Input:        in,
		Done:         done,
		BatchSize:    batchSize,
		FlushTimeout: flushTimeout,
		buffer:       make([]T, 0, batchSize),
		timer:        time.NewTicker(flushTimeout),
	}
	pipe.Run()
}
