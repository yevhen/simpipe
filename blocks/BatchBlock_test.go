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

func TestBatchBlockWithOptions(t *testing.T) {
	batches := make(chan []string)
	done := func(batch []string) {
		batches <- batch
	}

	in := make(chan string)
	block := NewBatchBlock(in,
		WithBatchSize[string](3),
		WithBatchFlushTimeout[string](250*time.Millisecond),
		WithBatchDoneCallback[string](done),
	)
	go block.Run()

	in <- "one"
	in <- "two"
	in <- "three" // Should trigger size-based flush

	batch1 := <-batches
	assert.Equal(t, 3, len(batch1))
	assert.Equal(t, "one", batch1[0])
	assert.Equal(t, "two", batch1[1])
	assert.Equal(t, "three", batch1[2])

	// Test timeout-based flush
	in <- "four"
	batch2 := <-batches
	assert.Equal(t, 1, len(batch2))
	assert.Equal(t, "four", batch2[0])
}
