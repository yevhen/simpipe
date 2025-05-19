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

func (step *ForkStep[T]) State() IPipelineState[T] {
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

	pendingPatch := state.step.Apply(message.payload, action)

	if pendingPatch != nil {
		state.pending = append(state.pending, pendingPatch)
	}
}

func (state *ForkState[T]) Done(message *PipelineMessage[T]) bool {
	message.mu.Lock()
	defer message.mu.Unlock()

	state.remaining--
	if state.remaining > 0 {
		return false
	}

	state.applyPendingPatches(message.payload)
	return true
}

func (state *ForkState[T]) applyPendingPatches(payload *T) {
	for _, patch := range state.pending {
		patch(payload)
	}
}
