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

	done := func(item string) {}

	in := make(chan string)
	go runActionBlock(in, done, 1, action)

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
	go runActionBlock(in, done, 1, action)

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

	done := func(item string) {}

	in := make(chan string)
	go runActionBlock(in, done, 2, action)

	now := time.Now()

	waiter.Add(2)
	in <- "bar"
	in <- "baz"
	waiter.Wait()

	elapsed := time.Now().Sub(now)

	assert.InDelta(t, delay.Seconds(), elapsed.Seconds(), delay.Seconds()/2)
}

func runActionBlock[T any](in chan T, done func(item T), parallelism int, action func(item T)) {
	block := &ActionBlock[T]{
		Input:       in,
		Done:        done,
		Parallelism: parallelism,
		Action:      action,
	}
	block.Run()
}
