package blocks

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"sync"
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
	block := NewBatchActionBlock(in, action,
		WithBatchActionBatchSize[string](2),
		WithBatchActionFlushTimeout[string](time.Hour),
		WithBatchActionParallelism[string](1),
		WithBatchActionDoneCallback[string](done),
	)
	go block.Run()

	for i := 0; i < itemCount; i++ {
		in <- fmt.Sprintf("i%d", i+1)
	}

	batch0 := <-batches
	batch1 := <-batches

	assert.ElementsMatch(t, []string{"i1", "i2"}, batch0)
	assert.ElementsMatch(t, []string{"i3", "i4"}, batch1)
}

func TestBatchActionBlockWithOptions(t *testing.T) {
	items := []string{"a", "b", "c", "d"}

	var wg sync.WaitGroup
	wg.Add(len(items))

	processed := make(map[string]bool)
	action := func(batch []string) {
		for _, item := range batch {
			processed[item] = true
		}
	}

	done := func(item string) {
		wg.Done()
	}

	in := make(chan string)
	block := NewBatchActionBlock(in, action,
		WithBatchActionBatchSize[string](len(items)-1),
		WithBatchActionFlushTimeout[string](250*time.Millisecond),
		WithBatchActionParallelism[string](len(items)),
		WithBatchActionDoneCallback[string](done),
	)
	go block.Run()

	in <- items[0]
	in <- items[1]
	in <- items[2] // Should trigger size-based flush
	in <- items[3] // Should trigger timeout-based flush

	// Wait for all items to be processed and completed
	wg.Wait()

	assert.Equal(t, 4, len(processed))

	for _, item := range items {
		assert.True(t, processed[item], "Item %s should be processed", item)
	}
}
