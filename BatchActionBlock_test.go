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
	go RunBatchActionBlock(in, done, 2, time.Hour, 1, action)

	for i := 0; i < itemCount; i++ {
		in <- fmt.Sprintf("i%d", i+1)
	}

	batch0 := <-batches
	batch1 := <-batches

	assert.ElementsMatch(t, []string{"i1", "i2"}, batch0)
	assert.ElementsMatch(t, []string{"i3", "i4"}, batch1)
}

type BatchActionBlock[T any] struct {
	Input        <-chan T
	Done         func(item T)
	BatchSize    int
	FlushTimeout time.Duration
	Parallelism  int
	Action       func(batch []T)
	batches      chan []T
	batchBlock   *BatchBlock[T]
	actionBlock  *ActionBlock[[]T]
}

func CreateBatchActionBlock[T any](
	in chan T,
	done func(item T),
	batchSize int,
	flushTimeout time.Duration,
	parallelism int,
	action func(batch []T),
) *BatchActionBlock[T] {

	var batches = make(chan []T)

	batchBlock := createInnerBatchBlock(batches, in, batchSize, flushTimeout)
	actionBlock := createInnerActionBlock(batches, done, parallelism, action)

	return &BatchActionBlock[T]{
		Input:        in,
		Done:         done,
		BatchSize:    batchSize,
		FlushTimeout: flushTimeout,
		Parallelism:  parallelism,
		Action:       action,
		batches:      batches,
		batchBlock:   batchBlock,
		actionBlock:  actionBlock,
	}
}

func (b *BatchActionBlock[T]) Run() {
	go b.batchBlock.Run()
	go b.actionBlock.Run()
}

func createInnerBatchBlock[T any](batches chan []T, in chan T, batchSize int, flushTimeout time.Duration) *BatchBlock[T] {
	sendBatch := func(batch []T) {
		batches <- batch
	}
	return CreateBatchBlock(in, batchSize, flushTimeout, sendBatch)
}

func createInnerActionBlock[T any](batches chan []T, done func(item T), parallelism int, action func(batch []T)) *ActionBlock[[]T] {
	batchDone := func(batch []T) {
		for _, item := range batch {
			done(item)
		}
	}
	return CreateActionBlock(batches, batchDone, parallelism, action)
}

func RunBatchActionBlock[T any](
	in chan T,
	done func(item T),
	batchSize int,
	flushTimeout time.Duration,
	parallelism int,
	action func(batch []T),
) {
	block := CreateBatchActionBlock(in, done, batchSize, flushTimeout, parallelism, action)
	block.Run()
}
