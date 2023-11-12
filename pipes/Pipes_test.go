package pipes

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

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

type PipeMock[T any] struct {
	ReceivedItem T
}

func (p *PipeMock[T]) Send(item T) {
	p.ReceivedItem = item
}

func TestPassesFilteredItemToNextPipe(t *testing.T) {
	nextPipe := new(PipeMock[string])

	next := func(item string) Pipe[string] {
		var p Pipe[string] = nextPipe
		return p
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

func TestActionPipeBlockIntegration(t *testing.T) {
	sent := make(chan string)
	action := func(item string) {
		sent <- item
	}

	filter := func(item string) bool {
		return item == "foo"
	}

	var nextReceived = make([]string, 0)
	next := func(item string) Pipe[string] {
		nextReceived = append(nextReceived, item)
		return nil
	}

	pipe := CreateActionPipe(2, 2, action, filter, next)
	pipe.Run()

	pipe.Send("foo")
	pipe.Send("bar")
	pipe.Close()

	assert.Equal(t, "foo", <-sent)
	assert.ElementsMatch(t, []string{"foo", "bar"}, nextReceived)
}

func TestBatchPipeBlockIntegration(t *testing.T) {
	sent := make(chan []string)
	action := func(items []string) {
		sent <- items
	}

	filter := func(item string) bool {
		return item == "foo"
	}

	var nextReceived = make([]string, 0)
	next := func(item string) Pipe[string] {
		nextReceived = append(nextReceived, item)
		return nil
	}

	pipe := CreateBatchActionPipe(4, 2, 2, action, filter, next)
	pipe.Run()

	pipe.Send("foo")
	pipe.Send("bar")
	pipe.Send("foo")
	pipe.Close()

	assert.ElementsMatch(t, []string{"foo", "foo"}, <-sent)
	assert.ElementsMatch(t, []string{"foo", "foo", "bar"}, nextReceived)
}

func TestLinkingPipes(t *testing.T) {
	sent := make(chan string)
	action := func(item string) {
		sent <- item
	}

	var first *ActionPipe[string]
	var second *ActionPipe[string]

	filter := func(item string) bool {
		return true
	}

	next := func(item string) Pipe[string] {
		return second
	}

	end := func(item string) Pipe[string] {
		return nil
	}

	first = CreateActionPipe(0, 1, action, filter, next)
	first.Run()

	second = CreateActionPipe(0, 1, action, filter, end)
	second.Run()

	first.Send("foo")
	first.Close()

	assert.Equal(t, "foo", <-sent)
	assert.Equal(t, "foo", <-sent)
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
