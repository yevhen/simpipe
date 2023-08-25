package simpipe

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

type PipeSender[T any] interface {
	Send(item T)
}

type Pipe[T any] struct {
	Receiver func(item T)
	Filter   func(item T) bool
	Next     func(item T) PipeSender[T]
}

func (pipe *Pipe[T]) Send(item T) {
	if pipe.Filter(item) {
		pipe.Receiver(item)
		return
	}

	pipe.sendNext(item)
}

func (pipe *Pipe[T]) sendNext(item T) {
	if pipe.Next == nil {
		return
	}

	next := pipe.Next(item)
	if next != nil {
		next.Send(item)
	}
}

func TestSendsItem(t *testing.T) {
	var sent string
	action := func(item string) {
		sent = item
	}

	const s = "foo"

	pipe := createPipe(action)
	pipe.Send(s)

	assert.Equal(t, sent, s)
}

func TestDoesNotPassFilteredItemToReceiver(t *testing.T) {
	var sent string
	action := func(item string) {
		sent = item
	}

	const s = "foo"
	filter := func(item string) bool {
		return item != "foo"
	}

	pipe := createFilteredPipe(action, filter)
	pipe.Send(s)

	assert.Empty(t, sent)
}

type PipeSenderMock[T any] struct {
	ReceivedItem T
}

func (p *PipeSenderMock[T]) Send(item T) {
	p.ReceivedItem = item
}

func TestPassesFilteredItemToNextPipe(t *testing.T) {
	nextPipe := new(PipeSenderMock[string])

	next := func(item string) PipeSender[string] {
		var p PipeSender[string] = nextPipe
		return p
	}

	filter := func(item string) bool {
		return false
	}

	pipe := createFilteredPipeWithNext(filter, next)
	pipe.Send("foo")

	assert.Equal(t, "foo", nextPipe.ReceivedItem)
}

func TestPassesFilteredItemToNextAndNextReturnsNil(t *testing.T) {
	var received string
	next := func(item string) PipeSender[string] {
		received = item
		return nil
	}

	filter := func(item string) bool {
		return false
	}
	pipe := createFilteredPipeWithNext(filter, next)
	pipe.Send("foo")

	assert.Equal(t, "foo", received)
}

func TestIntegration(t *testing.T) {
	input := make(chan string)

	pipe := Pipe[string]{
		Receiver: func(item string) {
			input <- item
		},
		Filter: func(item string) bool {
			return true
		},
		Next: nil,
	}

	var sent string
	action := func(item string) {
		sent = item
	}

	block := ActionBlock[string]{
		Input:       input,
		Parallelism: 1,
		Action:      action,
		Done:        pipe.sendNext,
	}
	block.Run()

	pipe.Send("foo")
	time.Sleep(100 * time.Millisecond)

	assert.Equal(t, "foo", sent)
}

func createPipe[T any](action func(item T)) *Pipe[T] {
	filter := func(item T) bool {
		return true
	}
	next := func(item T) PipeSender[T] {
		return nil
	}
	return createCompletePipe(action, filter, next)
}

func createFilteredPipe[T any](receiver func(item T), filter func(item T) bool) *Pipe[T] {
	next := func(item T) PipeSender[T] {
		return nil
	}
	return createCompletePipe(receiver, filter, next)
}

func createFilteredPipeWithNext[T any](filter func(item T) bool, next func(item T) PipeSender[T]) *Pipe[T] {
	action := func(item T) {
	}
	p := createPipe(action)
	p.Filter = filter
	p.Next = next
	return p
}

func createCompletePipe[T any](receiver func(item T), filter func(item T) bool, next func(item T) PipeSender[T]) *Pipe[T] {
	return &Pipe[T]{
		Filter:   filter,
		Receiver: receiver,
		Next:     next,
	}
}
