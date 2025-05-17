package routing

type Step[T any] interface {
	Send(message Message[T])
	Link(next Step[T])
	Next() Step[T]
	State() *PipelineState[T]
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

func (step *ProcessorStep[T]) State() *PipelineState[T] {
	processorCount := 0
	return &PipelineState[T]{
		step:      step,
		remaining: &processorCount,
	}
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

func (step *ForkStep[T]) State() *PipelineState[T] {
	processorCount := len(step.processors)
	return &PipelineState[T]{
		step:      step,
		remaining: &processorCount,
	}
}
