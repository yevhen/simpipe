package pipes

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

type PipeMock[T any] struct {
	ReceivedItems []T
}

func (p *PipeMock[T]) Send(item T) {
	p.ReceivedItems = append(p.ReceivedItems, item)
}

func TestSendsItemThatPassesFilterToReceiver(t *testing.T) {
	var received string
	receiver := func(item string) {
		received = item
	}

	filter := func(item string) bool {
		return true
	}

	const s = "foo"

	p := createPipeLinkWithFilter(receiver, filter)
	p.Send(s)

	assert.Equal(t, s, received)
}

func TestDoesNotSendFilteredItemToReceiver(t *testing.T) {
	var received string
	receiver := func(item string) {
		received = item
	}

	filter := func(item string) bool {
		return false
	}

	const s = "foo"

	p := createPipeLinkWithFilter(receiver, filter)
	p.Send(s)

	assert.Empty(t, received)
}

func TestDoesNotAutomaticallySendItemThatPassesFilterToNextPipe(t *testing.T) {
	filter := func(item string) bool {
		return true
	}

	nextPipe := new(PipeMock[string])

	next := func(item string) Pipe[string] {
		return nextPipe
	}

	const s = "foo"

	p := createPipeLinkWithNext(filter, next)
	p.Send(s)

	assert.Empty(t, nextPipe.ReceivedItems)
}

func TestAutomaticallySendsFilteredItemToNextPipe(t *testing.T) {
	filter := func(item string) bool {
		return false
	}

	nextPipe := new(PipeMock[string])

	next := func(item string) Pipe[string] {
		return nextPipe
	}

	const s = "foo"

	p := createPipeLinkWithNext(filter, next)
	p.Send(s)

	assert.Equal(t, []string{s}, nextPipe.ReceivedItems)
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
	processed := make(chan string)

	first := NewActionPipe(0, 1, func(item string) { processed <- item + "1" })
	second := NewActionPipe(0, 1, func(item string) { processed <- item + "2" })
	third := NewActionPipe(0, 1, func(item string) { processed <- item + "3" })

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

	assert.Equal(t, "foo1", <-processed)
	assert.Equal(t, "foo2", <-processed)
	assert.Equal(t, "foo3", <-processed)
	assert.Equal(t, "bar1", <-processed)
	assert.Equal(t, "bar3", <-processed)
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
