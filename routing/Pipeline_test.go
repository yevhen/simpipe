package routing

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
)

type Item struct {
	Text string
}

func TestSingleStepPipeline(t *testing.T) {
	message := &Item{"foo"}
	var waiter sync.WaitGroup

	var completed *Item
	pipeline := NewPipeline(func(message *Item) {
		completed = message
		waiter.Done()
	})

	processor := Action(1, func(message *Item) {
		message.Text = "processed"
	})

	pipeline.AddProcessor(processor)

	waiter.Add(1)
	pipeline.Send(message)
	waiter.Wait()

	assert.Equal(t, "processed", message.Text)
	assert.Equal(t, message, completed)
}

func TestMultiStepPipeline(t *testing.T) {
	message := &Item{"foo"}
	var waiter sync.WaitGroup

	var completedText string
	pipeline := NewPipeline(func(message *Item) {
		completedText = message.Text
		waiter.Done()
	})

	processorA := Action(1, func(message *Item) {
		message.Text += ".A"
	})
	processorB := Action(1, func(message *Item) {
		message.Text += ".B"
	})
	processorC := Action(1, func(message *Item) {
		message.Text += ".C"
	})

	pipeline.AddProcessor(processorA)
	pipeline.AddProcessor(processorB)
	pipeline.AddProcessor(processorC)

	waiter.Add(1)
	pipeline.Send(message)
	waiter.Wait()

	assert.Equal(t, "foo.A.B.C", message.Text)
	assert.Equal(t, "foo.A.B.C", completedText, "Should complete only at the final step")
}

func TestFork(t *testing.T) {
	const maxRuns = 1000
	for i := 0; i < maxRuns; i++ {
		t.Run(fmt.Sprintf("Run%d", i+1), func(t *testing.T) {
			t.Parallel() // Optional: comment this out for serial execution
			runForkTest(t)
		})
	}
}

func runForkTest(t *testing.T) {
	message := &Item{"fork-"}
	var waiter sync.WaitGroup

	var completedText string
	pipeline := NewPipeline(func(message *Item) {
		completedText = message.Text
		waiter.Done()
	})

	processorA := Action(1, func(message *Item) {
		message.Text += "A"
	})
	processorB := Action(1, func(message *Item) {
		message.Text += "B"
	})
	processorC := Action(1, func(message *Item) {
		message.Text += "-C"
	})

	pipeline.AddFork(processorA, processorB)
	pipeline.AddProcessor(processorC)

	waiter.Add(1)
	pipeline.Send(message)
	waiter.Wait()

	assert.True(t, "fork-AB-C" == completedText || "fork-BA-C" == completedText,
		"Should advance to next step only after all forked processors done: "+completedText)
}

func TestPatch(t *testing.T) {
	message := &Item{"foo"}
	var waiter sync.WaitGroup

	var completed *Item
	pipeline := NewPipeline(func(message *Item) {
		completed = message
		waiter.Done()
	})

	processor := Patch(1, func(message Item) func(*Item) {
		patchedText := message.Text + ".processed"
		return func(patch *Item) {
			patch.Text = patchedText
		}
	})

	pipeline.AddProcessor(processor)

	waiter.Add(1)
	pipeline.Send(message)
	waiter.Wait()

	assert.Equal(t, "foo.processed", message.Text)
	assert.Equal(t, message, completed)
}
