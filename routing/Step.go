package routing

type Step[T any] interface {
	Send(message Message[T])
	Link(next Step[T])
	Next() Step[T]
	State() IPipelineState[T]
	Apply(payload *T, action func(T) func(*T)) func(*T)
}

type ProcessorStep[T any] struct {
	processor Processor[T]
	next      Step[T]
}

func (step *ProcessorStep[T]) Send(message Message[T]) {
	step.processor.Send(message)
}

func (step *ProcessorStep[T]) Link(next Step[T]) {
	step.next = next
}

func (step *ProcessorStep[T]) Next() Step[T] {
	return step.next
}

func (step *ProcessorStep[T]) State() IPipelineState[T] {
	return &ProcessorState[T]{
		step: step,
	}
}

func (step *ProcessorStep[T]) Apply(payload *T, action func(T) func(*T)) func(*T) {
	patch := action(*payload)
	patch(payload)
	return nil
}

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
