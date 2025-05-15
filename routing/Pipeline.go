package routing

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
	processors  []*Processor[T]
	done        func(message *T)
	state       map[*T]*PipelineState[T]
	completions chan ProcessorCompletion[T]
	first       Step[T]
	last        Step[T]
}

func NewPipeline[T any](done func(message *T)) *Pipeline[T] {
	pipeline := &Pipeline[T]{
		done:        done,
		state:       make(map[*T]*PipelineState[T]),
		completions: make(chan ProcessorCompletion[T]),
	}

	go pipeline.processCompletions()

	return pipeline
}

func (p *Pipeline[T]) trackDone(message *T) {
	state := p.state[message]
	state.done()

	if *state.remaining <= 0 {
		p.advanceNext(state, message)
	}
}

func (p *Pipeline[T]) processCompletions() {
	for {
		ack := <-p.completions
		p.trackDone(ack.message)
	}
}

func (p *Pipeline[T]) AddProcessor(processor *Processor[T]) *Pipeline[T] {
	step := &ProcessorStep[T]{
		processor: processor,
	}

	return p.Add(step)
}

func (p *Pipeline[T]) AddFork(processors ...*Processor[T]) *Pipeline[T] {
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
	state := p.start()

	p.advanceNext(state, message)
}

func (p *Pipeline[T]) start() *PipelineState[T] {
	start := &ProcessorStep[T]{
		processor: nil,
		next:      p.first,
	}
	state := start.State()
	return state
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
			p.completions <- ProcessorCompletion[T]{
				message:   payload,
				Processor: processor,
			}
		},
	})
}
