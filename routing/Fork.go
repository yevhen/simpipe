package routing

type ForkStep[T any] struct {
	processors []Processor[T]
	next       Step[T]
}

func (step *ForkStep[T]) Send(message Message[T]) {
	for _, processor := range step.processors {
		processor.Send(message)
	}
}

func (step *ForkStep[T]) Link(next Step[T]) {
	step.next = next
}

func (step *ForkStep[T]) Next() Step[T] {
	return step.next
}

func (step *ForkStep[T]) State() PipelineState[T] {
	return &ForkState[T]{
		step:      step,
		remaining: len(step.processors),
	}
}

func (step *ForkStep[T]) Apply(payload *T, action func(T) func(*T)) func(*T) {
	patch := action(*payload)
	return patch
}

type ForkState[T any] struct {
	step      Step[T]
	remaining int
	pending   []func(*T)
}

func (state *ForkState[T]) Step() Step[T] {
	return state.step
}

func (state *ForkState[T]) Apply(message *PipelineMessage[T], action func(T) func(*T)) {
	message.mu.Lock()
	defer message.mu.Unlock()

	patch := state.step.Apply(message.payload, action)
	state.pending = append(state.pending, patch)
}

func (state *ForkState[T]) ProcessCompletion(message *PipelineMessage[T]) {
	message.mu.Lock()
	defer message.mu.Unlock()

	state.remaining--

	if state.Completed() {
		state.applyPendingPatches(message)
	}
}

func (state *ForkState[T]) applyPendingPatches(message *PipelineMessage[T]) {
	for _, patch := range state.pending {
		patch(message.payload)
	}
}

func (state *ForkState[T]) Completed() bool {
	return state.remaining <= 0
}
