package blocks

import (
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
	"time"
)

func TestExecutesGivenLambdaOnItemReceivedFromChannel(t *testing.T) {
	result := make(chan string, 2)
	action := func(item string) {
		time.Sleep(1 * time.Second)
		result <- item
	}

	in := make(chan string)
	block := NewActionBlock(in, action)
	go block.Run()

	in <- "bar"
	in <- "baz"

	assert.Equal(t, "bar", <-result)
	assert.Equal(t, "baz", <-result)
}

func TestPassesItemAfterExecutionToDone(t *testing.T) {
	action := func(item *struct{ text string }) {
		item.text += ".test"
	}

	result := make(chan *struct{ text string }, 2)
	done := func(item *struct{ text string }) {
		result <- item
	}

	in := make(chan *struct{ text string })
	block := NewActionBlock(in, action, WithDoneCallback(done))
	go block.Run()

	var i1 struct{ text string }
	i1.text = "bar"

	var i2 struct{ text string }
	i2.text = "baz"

	in <- &i1
	in <- &i2

	assert.Equal(t, "bar.test", (<-result).text)
	assert.Equal(t, "baz.test", (<-result).text)
}

func TestDegreeOfParallelism(t *testing.T) {
	var waiter sync.WaitGroup
	delay := 1 * time.Second

	action := func(item string) {
		time.Sleep(delay)
		waiter.Done()
	}

	in := make(chan string)
	block := NewActionBlock(in, action, WithParallelism[string](2))
	go block.Run()

	now := time.Now()

	waiter.Add(2)
	in <- "bar"
	in <- "baz"
	waiter.Wait()

	elapsed := time.Since(now)

	assert.InDelta(t, delay.Seconds(), elapsed.Seconds(), delay.Seconds()/2)
}

func TestWithOptionsChaining(t *testing.T) {
	var waiter sync.WaitGroup
	var result []string
	var mu sync.Mutex

	action := func(item string) {
		mu.Lock()
		result = append(result, item)
		mu.Unlock()
		waiter.Done()
	}

	done := func(item string) {
		// no-op
	}

	in := make(chan string)
	block := NewActionBlock(in, action,
		WithParallelism[string](2),
		WithDoneCallback[string](done),
	)
	go block.Run()

	waiter.Add(2)
	in <- "one"
	in <- "two"
	waiter.Wait()

	assert.Equal(t, 2, len(result))
	assert.Contains(t, result, "one")
	assert.Contains(t, result, "two")
}
