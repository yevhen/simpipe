package routing

import "sync"

type PipelineMessage[T any] struct {
	payload *T
	mutex   *sync.Mutex
	ack     func(message *PipelineMessage[T])
}

func (p *PipelineMessage[T]) Payload() *T {
	return p.payload
}

func (p *PipelineMessage[T]) Mutex() *sync.Mutex {
	return p.mutex
}

func (p *PipelineMessage[T]) Done() {
	p.ack(p)
}

type PipelineState[T any] struct {
	step      Step[T]
	remaining *int
}

func (state *PipelineState[T]) done() {
	*state.remaining--
}

func (state *PipelineState[T]) advance() *PipelineState[T] {
	if state.step.Next() == nil {
		return nil
	}

	return state.step.Next().State()
}

func (state *PipelineState[T]) send(message Message[T]) {
	state.step.Send(message)
}

type Pipeline[T any] struct {
	done        func(message *T)
	state       map[*T]*PipelineState[T]
	completions chan *PipelineMessage[T]
	first       Step[T]
	last        Step[T]
}

func NewPipeline[T any](done func(message *T)) *Pipeline[T] {
	pipeline := &Pipeline[T]{
		done:        done,
		state:       make(map[*T]*PipelineState[T]),
		completions: make(chan *PipelineMessage[T]),
	}

	go pipeline.processCompletions()

	return pipeline
}

func (p *Pipeline[T]) trackDone(message *PipelineMessage[T]) {
	state := p.state[message.Payload()]
	state.done()

	if *state.remaining <= 0 {
		p.advanceNext(state, message)
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

func (p *Pipeline[T]) Send(message *T) {
	mutex := &sync.Mutex{}
	pm := &PipelineMessage[T]{
		payload: message,
		mutex:   mutex,
		ack: func(m *PipelineMessage[T]) {
			p.completions <- m
		},
	}

	state := p.start()
	p.advanceNext(state, pm)
}

func (p *Pipeline[T]) start() *PipelineState[T] {
	start := &ProcessorStep[T]{
		processor: nil,
		next:      p.first,
	}
	state := start.State()
	return state
}

func (p *Pipeline[T]) advanceNext(state *PipelineState[T], message *PipelineMessage[T]) {
	next := state.advance()
	p.state[message.Payload()] = next

	if next == nil {
		p.done(message.Payload())
		return
	}

	next.send(message)
}
