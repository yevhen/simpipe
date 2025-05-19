package routing

import "sync"

type PipelineMessage[T any] struct {
	state   IPipelineState[T]
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

type IPipelineState[T any] interface {
	Step() Step[T]
	Send(message Message[T])
	Apply(message *PipelineMessage[T], action func(T) func(*T))
	Done(message *PipelineMessage[T]) bool
}

type PipelineState[T any] struct {
	step      Step[T]
	remaining int
	pending   []func(*T)
}

func (state *PipelineState[T]) Step() Step[T] {
	return state.step
}

func (state *PipelineState[T]) Apply(message *PipelineMessage[T], action func(T) func(*T)) {
	message.mu.Lock()
	defer message.mu.Unlock()

	pendingPatch := state.step.Apply(message.payload, action)

	if pendingPatch != nil {
		state.pending = append(state.pending, pendingPatch)
	}
}

func (state *PipelineState[T]) Done(message *PipelineMessage[T]) bool {
	message.mu.Lock()
	defer message.mu.Unlock()

	state.remaining--
	if state.remaining > 0 {
		return false
	}

	state.applyPendingPatches(message.payload)
	return true
}

func (state *PipelineState[T]) applyPendingPatches(payload *T) {
	for _, patch := range state.pending {
		patch(payload)
	}
}

func advance[T any](state IPipelineState[T]) IPipelineState[T] {
	next := state.Step().Next()
	if next == nil {
		return nil
	}
	return next.State()
}

func (state *PipelineState[T]) Send(message Message[T]) {
	state.step.Send(message)
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

func (p *Pipeline[T]) trackDone(message *PipelineMessage[T]) {
	done := message.state.Done(message)
	if done {
		p.advanceNext(message)
	}
}

func (p *Pipeline[T]) processCompletions() {
	for {
		ack := <-p.completions
		p.trackDone(ack)
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
	if p.last != nil {
		p.last.Link(step)
	}

	p.last = step

	if p.first == nil {
		p.first = step
	}

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

func (p *Pipeline[T]) start() *PipelineState[T] {
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
	next.Send(message)
}
