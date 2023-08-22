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
	go RunBatchBlock(in, 2, time.Hour, done)

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
	go RunBatchBlock(in, 2, flushTimeout, done)

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
	go RunBatchBlock(in, 2, flushTimeout, done)

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
	go RunBatchBlock(in, 2, time.Hour, done)

	in <- "bar"
	close(in)

	batch := <-batches
	assert.Equal(t, "bar", batch[0])
}
