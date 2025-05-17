package routing

type PipelineMessage[T any] struct {
	state   *PipelineState[T]
	payload *T
	pending []func(*T)
	ack     func(message *PipelineMessage[T])
}

func (pm *PipelineMessage[T]) Payload() *T {
	return pm.payload
}

func (pm *PipelineMessage[T]) Apply(action func(T) func(*T)) {
	patch := action(*pm.payload)

	if *pm.state.remaining == 0 {
		patch(pm.payload)
		return
	}

	pm.pending = append(pm.pending, patch)
}

func (pm *PipelineMessage[T]) Done() {
	pm.ack(pm)
}

type PipelineState[T any] struct {
	step      Step[T]
	remaining *int
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
	*message.state.remaining--

	if *message.state.remaining > 0 {
		return
	}

	message.applyPendingPatches()

	p.advanceNext(message.state, message)
}

func (pm *PipelineMessage[T]) applyPendingPatches() {
	for _, patch := range pm.pending {
		patch(pm.Payload())
	}

	pm.pending = pm.pending[:0]
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
	pm := &PipelineMessage[T]{
		payload: message,
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
	message.state = next

	if next == nil {
		p.done(message.Payload())
		return
	}

	next.send(message)
}
