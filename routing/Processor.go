package routing

import "simpipe/blocks"

type Message[T any] struct {
	Payload *T
	Ack     func(processor Processor[T], payload *T)
}

type Processor[T any] interface {
	Send(message Message[T])
}

type ProcessorCompletion[T any] struct {
	message   *T
	Processor Processor[T]
}

type ActionProcessor[T any] struct {
	in    chan Message[T]
	block *blocks.ActionBlock[Message[T]]
}

func (p *ActionProcessor[T]) Send(message Message[T]) {
	p.in <- message
}

func NewActionProcessor[T any](parallelism int, action func(message *T)) *ActionProcessor[T] {
	processor := &ActionProcessor[T]{}

	processor.in = make(chan Message[T])
	processor.block = &blocks.ActionBlock[Message[T]]{
		Input: processor.in,
		Done: func(message Message[T]) {
			message.Ack(processor, message.Payload)
		},
		Parallelism: parallelism,
		Action:      func(message Message[T]) { action(message.Payload) },
	}

	processor.block.Run()

	return processor
}
