package pipes

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

type PipeMock[T any] struct {
	ReceivedItem T
}

func (p *PipeMock[T]) Send(item T) {
	p.ReceivedItem = item
}

func TestSendsItem(t *testing.T) {
	var sent string
	action := func(item string) {
		sent = item
	}

	const s = "foo"

	p := createPipeLinkWithReceiver(action)
	p.Send(s)

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

	p := createPipeLinkWithFilter(action, filter)
	p.Send(s)

	assert.Empty(t, sent)
}

func TestPassesFilteredItemToNextPipe(t *testing.T) {
	nextPipe := new(PipeMock[string])

	next := func(item string) Pipe[string] {
		return nextPipe
	}

	filter := func(item string) bool {
		return false
	}

	pipe := createPipeLinkWithNext(filter, next)
	pipe.Send("foo")

	assert.Equal(t, "foo", nextPipe.ReceivedItem)
}

func TestPassesFilteredItemToNextAndNextReturnsNil(t *testing.T) {
	var received string
	next := func(item string) Pipe[string] {
		received = item
		return nil
	}

	filter := func(item string) bool {
		return false
	}

	pipe := createPipeLinkWithNext(filter, next)
	pipe.Send("foo")

	assert.Equal(t, "foo", received)
}

func TestLinkingPipes(t *testing.T) {
	processed := make(chan string, 2)
	action := func(item string) {
		processed <- item
	}

	first := NewActionPipe(0, 1, action)
	second := NewActionPipe(0, 1, action)
	third := NewActionPipe(0, 1, action)

	first.LinkNext(second)
	second.LinkNext(third)

	first.Link(third, func(item string) bool {
		return item == "bar"
	})

	first.Run()
	second.Run()
	third.Run()

	first.Send("foo")
	first.Send("bar")
	first.Close()

	assert.Equal(t, "foo", <-processed)
	assert.Equal(t, "foo", <-processed)
	assert.Equal(t, "foo", <-processed)
	assert.Equal(t, "bar", <-processed)
	assert.Equal(t, "bar", <-processed)
	assert.Equal(t, 0, len(processed))
}

func createPipeLinkWithReceiver[T any](receiver func(item T)) *PipeLink[T] {
	filter := func(item T) bool {
		return true
	}
	next := func(item T) Pipe[T] {
		return nil
	}
	return createPipeLink(receiver, filter, next)
}

func createPipeLinkWithFilter[T any](receiver func(item T), filter func(item T) bool) *PipeLink[T] {
	next := func(item T) Pipe[T] {
		return nil
	}
	return createPipeLink(receiver, filter, next)
}

func createPipeLinkWithNext[T any](filter func(item T) bool, next func(item T) Pipe[T]) *PipeLink[T] {
	action := func(item T) {
	}
	p := createPipeLinkWithReceiver(action)
	p.filter = filter
	p.next = next
	return p
}

func createPipeLink[T any](receiver func(item T), filter func(item T) bool, next func(item T) Pipe[T]) *PipeLink[T] {
	return &PipeLink[T]{
		filter:   filter,
		receiver: receiver,
		next:     next,
	}
}
