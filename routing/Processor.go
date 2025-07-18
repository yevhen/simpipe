package routing

import (
	"simpipe/blocks"
)

type ActionProcessor[T any] struct {
	in     chan Message[T]
	block  *blocks.ActionBlock[Message[T]]
	filter func(message *T) bool
}

func (p *ActionProcessor[T]) Send(message Message[T]) {
	if !p.filter(message.Payload()) {
		message.Done()
		return
	}
	p.in <- message
}

func Action[T any](parallelism int, action func(message *T)) *ActionProcessor[T] {
	filter := func(message *T) bool {
		return true
	}
	return ActionWithFilter(parallelism, filter, action)
}

func ActionWithFilter[T any](parallelism int, filter func(message *T) bool, action func(message *T)) *ActionProcessor[T] {
	return PatchWithFilter[T](parallelism, filter, func(message T) func(message *T) {
		return func(message *T) {
			action(message)
		}
	})
}

func Patch[T any](parallelism int, action func(message T) func(*T)) *ActionProcessor[T] {
	filter := func(message *T) bool {
		return true
	}
	return PatchWithFilter(parallelism, filter, action)
}

func PatchWithFilter[T any](parallelism int, filter func(message *T) bool, action func(message T) func(*T)) *ActionProcessor[T] {
	processor := &ActionProcessor[T]{
		filter: filter,
	}

	processor.in = make(chan Message[T])
	processor.block = &blocks.ActionBlock[Message[T]]{
		Input: processor.in,
		Done: func(message Message[T]) {
			message.Done()
		},
		Parallelism: parallelism,
		Action: func(message Message[T]) {
			message.Apply(action)
		},
	}

	processor.block.Run()

	return processor
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

func (step *ProcessorStep[T]) State() PipelineState[T] {
	return &ProcessorState[T]{
		step: step,
	}
}

func (step *ProcessorStep[T]) Apply(payload *T, action func(T) func(*T)) func(*T) {
	patch := action(*payload)
	patch(payload)
	return nil
}

type ProcessorState[T any] struct {
	step Step[T]
}

func (state *ProcessorState[T]) Step() Step[T] {
	return state.step
}

func (state *ProcessorState[T]) Apply(message *PipelineMessage[T], action func(T) func(*T)) {
	state.step.Apply(message.payload, action)
}

func (state *ProcessorState[T]) ProcessCompletion(_ *PipelineMessage[T]) {}

func (state *ProcessorState[T]) Completed() bool {
	return true
}
