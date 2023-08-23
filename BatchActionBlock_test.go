package simpipe

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestExecutesActionOnBatches(t *testing.T) {
	batches := make(chan []string)
	action := func(batch []string) {
		batches <- batch
	}

	const itemCount = 4
	done := func(item string) {}

	in := make(chan string)
	go RunBatchActionBlock(in, done, 1, 2, time.Hour, action)

	for i := 0; i < itemCount; i++ {
		in <- fmt.Sprintf("i%d", i+1)
	}

	batch0 := <-batches
	batch1 := <-batches

	assert.ElementsMatch(t, []string{"i1", "i2"}, batch0)
	assert.ElementsMatch(t, []string{"i3", "i4"}, batch1)
}

func RunBatchActionBlock[T any](
	in chan T,
	done func(item T),
	parallelism int,
	batchSize int,
	flushTimeout time.Duration,
	action func(batch []T),
) {
	var batches = make(chan []T)
	sendToActionBlock := func(batch []T) {
		batches <- batch
	}

	batchBlock := CreateBatchBlock(in, batchSize, flushTimeout, sendToActionBlock)
	go batchBlock.Run()

	batchDone := func(batch []T) {
		for _, item := range batch {
			done(item)
		}
	}

	actionBlock := CreateActionBlock(batches, batchDone, parallelism, action)
	go actionBlock.Run()
}
