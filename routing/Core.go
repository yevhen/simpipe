package routing

import "sync"

type Message[T any] interface {
	Payload() *T
	Apply(func(T) func(*T))
	Done()
}

type Processor[T any] interface {
	Send(message Message[T])
}

type Step[T any] interface {
	Send(message Message[T])
	Link(next Step[T])
	Next() Step[T]
	State() PipelineState[T]
	Apply(payload *T, action func(T) func(*T)) func(*T)
}

type PipelineState[T any] interface {
	Step() Step[T]
	Apply(message *PipelineMessage[T], action func(T) func(*T))
	ProcessCompletion(message *PipelineMessage[T])
	Completed() bool
}

func advance[T any](state PipelineState[T]) PipelineState[T] {
	next := state.Step().Next()
	if next == nil {
		return nil
	}
	return next.State()
}

type PipelineMessage[T any] struct {
	state   PipelineState[T]
	payload *T
	mu      sync.Mutex
	ack     func(message *PipelineMessage[T])
}

func (pm *PipelineMessage[T]) Payload() *T {
	return pm.payload
}

func (pm *PipelineMessage[T]) Apply(action func(T) func(*T)) {
	pm.state.Apply(pm, action)
}

func (pm *PipelineMessage[T]) Done() {
	pm.ack(pm)
}

type Pipeline[T any] struct {
	done        func(message *T)
	completions chan *PipelineMessage[T]
	first       Step[T]
	last        Step[T]
}

func NewPipeline[T any](done func(message *T)) *Pipeline[T] {
	pipeline := &Pipeline[T]{
		done:        done,
		completions: make(chan *PipelineMessage[T]),
	}

	go pipeline.processCompletions()

	return pipeline
}

func (p *Pipeline[T]) processCompletion(message *PipelineMessage[T]) {
	message.state.ProcessCompletion(message)

	if message.state.Completed() {
		p.advanceNext(message)
	}
}

func (p *Pipeline[T]) processCompletions() {
	for {
		ack := <-p.completions
		p.processCompletion(ack)
	}
}

func (p *Pipeline[T]) AddProcessor(processor Processor[T]) *Pipeline[T] {
	step := &ProcessorStep[T]{
		processor: processor,
	}

	return p.Add(step)
}

func (p *Pipeline[T]) AddFork(processors ...Processor[T]) *Pipeline[T] {
	fork := &ForkStep[T]{
		processors: processors,
	}

	return p.Add(fork)
}

func (p *Pipeline[T]) Add(step Step[T]) *Pipeline[T] {
	if p.first == nil {
		p.first = step
	}

	if p.last != nil {
		p.last.Link(step)
	}

	p.last = step

	return p
}

func (p *Pipeline[T]) Send(payload *T) {
	message := &PipelineMessage[T]{
		mu:      sync.Mutex{},
		payload: payload,
		ack: func(m *PipelineMessage[T]) {
			p.completions <- m
		},
	}

	message.state = p.start()
	p.advanceNext(message)
}

func (p *Pipeline[T]) start() PipelineState[T] {
	start := &ProcessorStep[T]{
		processor: nil,
		next:      p.first,
	}
	state := start.State()
	return state
}

func (p *Pipeline[T]) advanceNext(message *PipelineMessage[T]) {
	next := advance(message.state)
	if next == nil {
		p.done(message.Payload())
		return
	}

	message.state = next
	next.Step().Send(message)
}
