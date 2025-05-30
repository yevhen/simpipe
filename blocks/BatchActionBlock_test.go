package blocks

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
	go runBatchActionBlock(in, done, 2, time.Hour, 1, action)

	for i := 0; i < itemCount; i++ {
		in <- fmt.Sprintf("i%d", i+1)
	}

	batch0 := <-batches
	batch1 := <-batches

	assert.ElementsMatch(t, []string{"i1", "i2"}, batch0)
	assert.ElementsMatch(t, []string{"i3", "i4"}, batch1)
}

func runBatchActionBlock[T any](
	in chan T,
	done func(item T),
	batchSize int,
	flushTimeout time.Duration,
	parallelism int,
	action func(batch []T),
) {
	block := &BatchActionBlock[T]{
		Input:        in,
		Done:         done,
		BatchSize:    batchSize,
		FlushTimeout: flushTimeout,
		Parallelism:  parallelism,
		Action:       action,
	}
	block.Run()
}
