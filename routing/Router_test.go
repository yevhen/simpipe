package routing

import (
	"github.com/stretchr/testify/assert"
	"simpipe/blocks"
	"sync"
	"testing"
)

type Message[T any] struct {
	Payload *T
	Ack     func(processor *Processor[T], payload *T)
}

type Item struct {
	Text string
}

type Processor[T any] struct {
	in    chan Message[T]
	block *blocks.ActionBlock[Message[T]]
}

func (n *Processor[T]) Run() {
	n.block.Run()
}

func (n *Processor[T]) Send(message Message[T]) {
	n.in <- message
}

func (n *Processor[T]) Close() {
	close(n.in)
}

type StepCompletion[T any] struct {
	message   *T
	Processor *Processor[T]
}

type Pipeline[T any] struct {
	processors  []*Processor[T]
	done        func(message *T)
	state       map[*T]*PipelineState[T]
	completions chan StepCompletion[T]
	head        *Step[T]
}

func NewPipeline[T any](done func(message *T)) *Pipeline[T] {
	pipeline := &Pipeline[T]{
		done:        done,
		state:       make(map[*T]*PipelineState[T]),
		completions: make(chan StepCompletion[T]),
	}

	go pipeline.processCompletions()

	return pipeline
}

func NewActionProcessor[T any](parallelism int, action func(message *T)) *Processor[T] {
	processor := &Processor[T]{}

	processor.in = make(chan Message[T])
	processor.block = &blocks.ActionBlock[Message[T]]{
		Input: processor.in,
		Done: func(message Message[T]) {
			message.Ack(processor, message.Payload)
		},
		Parallelism: parallelism,
		Action:      func(message Message[T]) { action(message.Payload) },
	}

	processor.block.Run()

	return processor
}

func (p *Pipeline[T]) trackDone(processor *Processor[T], message *T) {
	state := p.state[message]
	state.done(processor)

	p.advanceNext(state, message)
}

func (p *Pipeline[T]) processCompletions() {
	for {
		ack := <-p.completions
		p.trackDone(ack.Processor, ack.message)
	}
}

func (p *Pipeline[T]) Add(processor *Processor[T]) *Pipeline[T] {
	step := &Step[T]{
		processor: processor,
	}

	if p.head != nil {
		p.head.next = step
	}

	if p.head == nil {
		p.head = step
	}

	return p
}

func (p *Pipeline[T]) Send(message *T) {
	state := p.start()

	p.advanceNext(state, message)
}

func (p *Pipeline[T]) start() *PipelineState[T] {
	return &PipelineState[T]{
		step: &Step[T]{
			processor: nil,
			next:      p.head,
		},
	}
}

func (p *Pipeline[T]) advanceNext(state *PipelineState[T], message *T) {
	next := state.advance()
	p.state[message] = next

	if next == nil {
		p.done(message)
		return
	}

	next.send(Message[T]{
		Payload: message,
		Ack: func(processor *Processor[T], payload *T) {
			p.completions <- StepCompletion[T]{
				message:   payload,
				Processor: processor,
			}
		},
	})
}

type Step[T any] struct {
	processor *Processor[T]
	next      *Step[T]
}

func (s *Step[T]) Send(message Message[T]) {
	s.processor.Send(message)
}

type PipelineState[T any] struct {
	step *Step[T]
}

func (t *PipelineState[T]) done(processor *Processor[T]) {
	// do nothing for now
}

func (t *PipelineState[T]) advance() *PipelineState[T] {
	if t.step.next == nil {
		return nil
	}

	return &PipelineState[T]{
		step: t.step.next,
	}
}

func (t *PipelineState[T]) send(message Message[T]) {
	t.step.Send(message)
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

	pipeline.Add(processor)

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

	pipeline.Add(processorA)
	pipeline.Add(processorB)

	waiter.Add(1)
	pipeline.Send(message)
	waiter.Wait()

	assert.Equal(t, "foo.A.B", message.Text)
	assert.Equal(t, "foo.A.B", completedText, "Should complete only at the final step")
}
