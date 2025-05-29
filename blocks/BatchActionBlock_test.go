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
