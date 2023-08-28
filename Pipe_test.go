package simpipe

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

type Pipe[T any] interface {
	Send(item T)
}

type PipeLink[T any] struct {
	receiver func(item T)
	filter   func(item T) bool
	next     func(item T) Pipe[T]
}

func (p *PipeLink[T]) Send(item T) {
	if p.filter(item) {
		p.receiver(item)
		return
	}

	p.sendNext(item)
}

func (p *PipeLink[T]) sendNext(item T) {
	if p.next == nil {
		return
	}

	next := p.next(item)
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

	p := createPipe(action)
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

	p := createFilteredPipe(action, filter)
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

	pipe := createFilteredPipeWithNext(filter, next)
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

	pipe := createFilteredPipeWithNext(filter, next)
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

type ActionPipe[T any] struct {
	in    chan T
	link  *PipeLink[T]
	block *ActionBlock[T]
}

func (p *ActionPipe[T]) Run() {
	p.block.Run()
}

func (p *ActionPipe[T]) Send(item T) {
	p.link.Send(item)
}

func (p *ActionPipe[T]) Close() {
	close(p.in)
}

func CreateActionPipe[T any](
	capacity int,
	parallelism int,
	action func(item T),
	filter func(item T) bool,
	next func(item T) Pipe[T],
) *ActionPipe[T] {
	input := make(chan T, capacity)

	pipe := &PipeLink[T]{
		receiver: func(item T) {
			input <- item
		},
		filter: filter,
		next:   next,
	}

	block := CreateActionBlock(input, pipe.sendNext, parallelism, action)

	return &ActionPipe[T]{
		in:    input,
		link:  pipe,
		block: block,
	}
}

type BatchActionPipe[T any] struct {
	in    chan T
	link  *PipeLink[T]
	block *BatchActionBlock[T]
}

func (p *BatchActionPipe[T]) Run() {
	p.block.Run()
}

func (p *BatchActionPipe[T]) Send(item T) {
	p.link.Send(item)
}

func (p *BatchActionPipe[T]) Close() {
	close(p.in)
}

func CreateBatchActionPipe[T any](
	capacity int,
	parallelism int,
	batchSize int,
	action func(items []T),
	filter func(item T) bool,
	next func(item T) Pipe[T],
) *BatchActionPipe[T] {
	input := make(chan T, capacity)

	pipe := &PipeLink[T]{
		receiver: func(item T) {
			input <- item
		},
		filter: filter,
		next:   next,
	}

	block := CreateBatchActionBlock(input, pipe.sendNext, batchSize, time.Hour, parallelism, action)

	return &BatchActionPipe[T]{
		in:    input,
		link:  pipe,
		block: block,
	}
}

func createPipe[T any](action func(item T)) *PipeLink[T] {
	filter := func(item T) bool {
		return true
	}
	next := func(item T) Pipe[T] {
		return nil
	}
	return createCompletePipe(action, filter, next)
}

func createFilteredPipe[T any](receiver func(item T), filter func(item T) bool) *PipeLink[T] {
	next := func(item T) Pipe[T] {
		return nil
	}
	return createCompletePipe(receiver, filter, next)
}

func createFilteredPipeWithNext[T any](filter func(item T) bool, next func(item T) Pipe[T]) *PipeLink[T] {
	action := func(item T) {
	}
	p := createPipe(action)
	p.filter = filter
	p.next = next
	return p
}

func createCompletePipe[T any](receiver func(item T), filter func(item T) bool, next func(item T) Pipe[T]) *PipeLink[T] {
	return &PipeLink[T]{
		filter:   filter,
		receiver: receiver,
		next:     next,
	}
}
