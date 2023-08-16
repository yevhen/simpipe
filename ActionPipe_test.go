package simpipe

import (
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
	"time"
)

func TestExecutesGivenLambdaOnItemReceivedFromChannel(t *testing.T) {
	res := make([]string, 0)

	var waiter sync.WaitGroup
	delay := 1 * time.Second
	f := func(item string) {
		time.Sleep(delay)
		res = append(res, item)
		waiter.Done()
	}

	in := make(chan string)
	out := make(chan string, 2)
	go createActionPipe(in, out, 1, f)

	waiter.Add(2)
	in <- "bar"
	in <- "baz"

	waiter.Wait()
	assert.Equal(t, 2, len(res))
	assert.Contains(t, res, "bar")
	assert.Contains(t, res, "baz")
}

func TestPassesItemAfterExecutionToOutChannel(t *testing.T) {
	f := func(item *struct{ text string }) {
		item.text += ".test"
	}

	in := make(chan *struct{ text string })
	out := make(chan *struct{ text string }, 2)
	go createActionPipe(in, out, 1, f)

	var i1 struct{ text string }
	i1.text = "bar"

	var i2 struct{ text string }
	i2.text = "baz"

	in <- &i1
	in <- &i2

	res := make([]*struct{ text string }, 0)
	res = append(res, <-out)
	res = append(res, <-out)

	assert.Equal(t, 2, len(res))
	assert.Equal(t, "bar.test", res[0].text)
	assert.Equal(t, "baz.test", res[1].text)
}

func TestDegreeOfParallelism(t *testing.T) {
	var waiter sync.WaitGroup
	delay := 1 * time.Second

	f := func(item string) {
		time.Sleep(delay)
		waiter.Done()
	}

	in := make(chan string)
	out := make(chan string)
	go createActionPipe(in, out, 2, f)

	now := time.Now()

	waiter.Add(2)
	in <- "bar"
	in <- "baz"

	waiter.Wait()
	elapsed := time.Now().Sub(now)

	assert.InDelta(t, delay.Seconds(), elapsed.Seconds(), delay.Seconds()/2)
}

type ActionPipe[T any] struct {
	Input       <-chan T
	Output      chan<- T
	Parallelism int
	F           func(item T)
}

func (pipe *ActionPipe[T]) Run() {
	for i := 0; i < pipe.Parallelism; i++ {
		go pipe.process()
	}
}

func (pipe *ActionPipe[T]) process() {
	for item := range pipe.Input {
		pipe.F(item)
		pipe.Output <- item
	}
}

func createActionPipe[T any](in chan T, out chan T, parallelism int, f func(item T)) {
	pipe := ActionPipe[T]{
		Input:       in,
		Output:      out,
		Parallelism: parallelism,
		F:           f,
	}
	pipe.Run()
}
