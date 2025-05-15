package routing

import (
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
	"time"
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

	processor := NewActionProcessor(1, func(message *Item) {
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

	processorA := NewActionProcessor(1, func(message *Item) {
		message.Text += ".A"
	})
	processorB := NewActionProcessor(1, func(message *Item) {
		message.Text += ".B"
	})
	processorC := NewActionProcessor(1, func(message *Item) {
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
	message := &Item{"fork-"}
	var waiter sync.WaitGroup

	var completedText string
	pipeline := NewPipeline(func(message *Item) {
		completedText = message.Text
		waiter.Done()
	})

	processorA := NewActionProcessor(1, func(message *Item) {
		message.Text += "A"
	})
	processorB := NewActionProcessor(1, func(message *Item) {
		time.Sleep(50)
		message.Text += "B"
	})
	processorC := NewActionProcessor(1, func(message *Item) {
		message.Text += "-C"
	})

	pipeline.AddFork(processorA, processorB)
	pipeline.AddProcessor(processorC)

	waiter.Add(1)
	pipeline.Send(message)
	waiter.Wait()

	assert.Equal(t, "fork-AB-C", message.Text)
	assert.Equal(t, "fork-AB-C", completedText, "Should advance to next step only after all forked processors done")
}
