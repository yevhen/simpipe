package blocks

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
	block := NewBatchBlock(in,
		WithBatchSize[string](2),
		WithBatchFlushTimeout[string](time.Hour),
		WithBatchDoneCallback[string](done),
	)
	go block.Run()

	in <- "bar"
	in <- "baz"

	batch := <-batches
	assert.Equal(t, "bar", batch[0])
	assert.Equal(t, "baz", batch[1])
}

func TestFlushesBySizeSequentially(t *testing.T) {
	batches := make(chan []string)
	done := func(batch []string) {
		batches <- batch
	}

	in := make(chan string, 2)
	block := NewBatchBlock(in,
		WithBatchSize[string](1),
		WithBatchFlushTimeout[string](time.Hour),
		WithBatchDoneCallback[string](done),
	)
	go block.Run()

	in <- "bar"
	in <- "baz"

	batch0 := <-batches
	batch1 := <-batches

	assert.Equal(t, []string{"bar"}, batch0)
	assert.Equal(t, []string{"baz"}, batch1)
}

func TestFlushesByTimer(t *testing.T) {
	batches := make(chan []string)
	done := func(batch []string) {
		batches <- batch
	}

	flushTimeout := 500 * time.Millisecond

	in := make(chan string)
	block := NewBatchBlock(in,
		WithBatchSize[string](2),
		WithBatchFlushTimeout[string](flushTimeout),
		WithBatchDoneCallback[string](done),
	)
	go block.Run()

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
	block := NewBatchBlock(in,
		WithBatchSize[string](2),
		WithBatchFlushTimeout[string](flushTimeout),
		WithBatchDoneCallback[string](done),
	)
	go block.Run()

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
	block := NewBatchBlock(in,
		WithBatchSize[string](2),
		WithBatchFlushTimeout[string](time.Hour),
		WithBatchDoneCallback[string](done),
	)
	go block.Run()

	in <- "bar"
	close(in)

	batch := <-batches
	assert.Equal(t, "bar", batch[0])
}
